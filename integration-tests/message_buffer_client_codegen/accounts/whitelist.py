import typing
from dataclasses import dataclass
from construct import Construct
from solana.publickey import PublicKey
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
    admin: str
    allowed_programs: list[str]


@dataclass
class Whitelist:
    discriminator: typing.ClassVar = b"\xcc\xb04O\x92y6\xf7"
    layout: typing.ClassVar = borsh.CStruct(
        "bump" / borsh.U8,
        "admin" / BorshPubkey,
        "allowed_programs" / borsh.Vec(typing.cast(Construct, BorshPubkey)),
    )
    bump: int
    admin: PublicKey
    allowed_programs: list[PublicKey]

    @classmethod
    async def fetch(
        cls,
        conn: AsyncClient,
        address: PublicKey,
        commitment: typing.Optional[Commitment] = None,
        program_id: PublicKey = PROGRAM_ID,
    ) -> typing.Optional["Whitelist"]:
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
            admin=dec.admin,
            allowed_programs=dec.allowed_programs,
        )

    def to_json(self) -> WhitelistJSON:
        return {
            "bump": self.bump,
            "admin": str(self.admin),
            "allowed_programs": list(
                map(lambda item: str(item), self.allowed_programs)
            ),
        }

    @classmethod
    def from_json(cls, obj: WhitelistJSON) -> "Whitelist":
        return cls(
            bump=obj["bump"],
            admin=PublicKey(obj["admin"]),
            allowed_programs=list(
                map(lambda item: PublicKey(item), obj["allowed_programs"])
            ),
        )
