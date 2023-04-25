import typing
from dataclasses import dataclass
from construct import Construct
from solders.pubkey import Pubkey
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Commitment
import borsh_construct as borsh
from anchorpy.coder.accounts import ACCOUNT_DISCRIMINATOR_SIZE
from anchorpy.error import AccountInvalidDiscriminator
from anchorpy.utils.rpc import get_multiple_accounts
from anchorpy.borsh_extension import BorshPubkey
from ..program_id import PROGRAM_ID


class WhitelistJSON(typing.TypedDict):
    bump: int
    authority: str
    allowed_programs: list[str]


@dataclass
class Whitelist:
    discriminator: typing.ClassVar = b"\xcc\xb04O\x92y6\xf7"
    layout: typing.ClassVar = borsh.CStruct(
        "bump" / borsh.U8,
        "authority" / BorshPubkey,
        "allowed_programs" / borsh.Vec(typing.cast(Construct, BorshPubkey)),
    )
    bump: int
    authority: Pubkey
    allowed_programs: list[Pubkey]

    @classmethod
    async def fetch(
        cls,
        conn: AsyncClient,
        address: Pubkey,
        commitment: typing.Optional[Commitment] = None,
        program_id: Pubkey = PROGRAM_ID,
    ) -> typing.Optional["Whitelist"]:
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
    ) -> typing.List[typing.Optional["Whitelist"]]:
        infos = await get_multiple_accounts(conn, addresses, commitment=commitment)
        res: typing.List[typing.Optional["Whitelist"]] = []
        for info in infos:
            if info is None:
                res.append(None)
                continue
            if info.account.owner != program_id:
                raise ValueError("Account does not belong to this program")
            res.append(cls.decode(info.account.data))
        return res

    @classmethod
    def decode(cls, data: bytes) -> "Whitelist":
        if data[:ACCOUNT_DISCRIMINATOR_SIZE] != cls.discriminator:
            raise AccountInvalidDiscriminator(
                "The discriminator for this account is invalid"
            )
        dec = Whitelist.layout.parse(data[ACCOUNT_DISCRIMINATOR_SIZE:])
        return cls(
            bump=dec.bump,
            authority=dec.authority,
            allowed_programs=dec.allowed_programs,
        )

    def to_json(self) -> WhitelistJSON:
        return {
            "bump": self.bump,
            "authority": str(self.authority),
            "allowed_programs": list(
                map(lambda item: str(item), self.allowed_programs)
            ),
        }

    @classmethod
    def from_json(cls, obj: WhitelistJSON) -> "Whitelist":
        return cls(
            bump=obj["bump"],
            authority=Pubkey.from_string(obj["authority"]),
            allowed_programs=list(
                map(lambda item: Pubkey.from_string(item), obj["allowed_programs"])
            ),
        )
