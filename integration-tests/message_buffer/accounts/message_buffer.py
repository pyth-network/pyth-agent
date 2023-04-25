import typing
from dataclasses import dataclass
from solana.publickey import PublicKey
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Commitment
import borsh_construct as borsh
from anchorpy.coder.accounts import ACCOUNT_DISCRIMINATOR_SIZE
from anchorpy.error import AccountInvalidDiscriminator
from anchorpy.utils.rpc import get_multiple_accounts
from ..program_id import PROGRAM_ID


class MessageBufferJSON(typing.TypedDict):
    bump: int
    version: int
    header_len: int
    end_offsets: list[int]


@dataclass
class MessageBuffer:
    discriminator: typing.ClassVar = b"\x19\xf4\x03\x05\xe1\xa5\x1d\xfa"
    layout: typing.ClassVar = borsh.CStruct(
        "bump" / borsh.U8,
        "version" / borsh.U8,
        "header_len" / borsh.U16,
        "end_offsets" / borsh.U16[255],
    )
    bump: int
    version: int
    header_len: int
    end_offsets: list[int]

    @classmethod
    async def fetch(
        cls,
        conn: AsyncClient,
        address: PublicKey,
        commitment: typing.Optional[Commitment] = None,
        program_id: PublicKey = PROGRAM_ID,
    ) -> typing.Optional["MessageBuffer"]:
        resp = await conn.get_account_info(address, commitment=commitment)
        info = resp.value
        if info is None:
            return None
        if info.owner != program_id.to_solders():
            raise ValueError("Account does not belong to this program")
        bytes_data = info.data
        return cls.decode(bytes_data)

    @classmethod
    async def fetch_multiple(
        cls,
        conn: AsyncClient,
        addresses: list[PublicKey],
        commitment: typing.Optional[Commitment] = None,
        program_id: PublicKey = PROGRAM_ID,
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
            bump=dec.bump,
            version=dec.version,
            header_len=dec.header_len,
            end_offsets=dec.end_offsets,
        )

    def to_json(self) -> MessageBufferJSON:
        return {
            "bump": self.bump,
            "version": self.version,
            "header_len": self.header_len,
            "end_offsets": self.end_offsets,
        }

    @classmethod
    def from_json(cls, obj: MessageBufferJSON) -> "MessageBuffer":
        return cls(
            bump=obj["bump"],
            version=obj["version"],
            header_len=obj["header_len"],
            end_offsets=obj["end_offsets"],
        )
