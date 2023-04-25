import typing
from dataclasses import dataclass
from solders.pubkey import Pubkey
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Commitment
import borsh_construct as borsh
from anchorpy.coder.accounts import ACCOUNT_DISCRIMINATOR_SIZE
from anchorpy.error import AccountInvalidDiscriminator
from anchorpy.utils.rpc import get_multiple_accounts
from ..program_id import PROGRAM_ID
from .. import types


class MessageBufferJSON(typing.TypedDict):
    header: types.buffer_header.BufferHeaderJSON
    messages: list[int]


@dataclass
class MessageBuffer:
    discriminator: typing.ClassVar = b"\x19\xf4\x03\x05\xe1\xa5\x1d\xfa"
    layout: typing.ClassVar = borsh.CStruct(
        "header" / types.buffer_header.BufferHeader.layout, "messages" / borsh.U8[9718]
    )
    header: types.buffer_header.BufferHeader
    messages: list[int]

    @classmethod
    async def fetch(
        cls,
        conn: AsyncClient,
        address: Pubkey,
        commitment: typing.Optional[Commitment] = None,
        program_id: Pubkey = PROGRAM_ID,
    ) -> typing.Optional["MessageBuffer"]:
        resp = await conn.get_account_info(address, commitment=commitment)
        info = resp.value
        if info is None:
            return None
        if info.owner != program_id:
            raise ValueError("Account does not belong to this program")
        bytes_data = info.data
        return cls.decode(bytes_data)

    @classmethod
    async def fetch_multiple(
        cls,
        conn: AsyncClient,
        addresses: list[Pubkey],
        commitment: typing.Optional[Commitment] = None,
        program_id: Pubkey = PROGRAM_ID,
    ) -> typing.List[typing.Optional["MessageBuffer"]]:
        infos = await get_multiple_accounts(conn, addresses, commitment=commitment)
        res: typing.List[typing.Optional["MessageBuffer"]] = []
        for info in infos:
            if info is None:
                res.append(None)
                continue
            if info.account.owner != program_id:
                raise ValueError("Account does not belong to this program")
            res.append(cls.decode(info.account.data))
        return res

    @classmethod
    def decode(cls, data: bytes) -> "MessageBuffer":
        if data[:ACCOUNT_DISCRIMINATOR_SIZE] != cls.discriminator:
            raise AccountInvalidDiscriminator(
                "The discriminator for this account is invalid"
            )
        dec = MessageBuffer.layout.parse(data[ACCOUNT_DISCRIMINATOR_SIZE:])
        return cls(
            header=types.buffer_header.BufferHeader.from_decoded(dec.header),
            messages=dec.messages,
        )

    def to_json(self) -> MessageBufferJSON:
        return {
            "header": self.header.to_json(),
            "messages": self.messages,
        }

    @classmethod
    def from_json(cls, obj: MessageBufferJSON) -> "MessageBuffer":
        return cls(
            header=types.buffer_header.BufferHeader.from_json(obj["header"]),
            messages=obj["messages"],
        )
