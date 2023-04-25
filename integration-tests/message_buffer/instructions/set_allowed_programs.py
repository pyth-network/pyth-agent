from __future__ import annotations
import typing
from solana.publickey import PublicKey
from solana.transaction import TransactionInstruction, AccountMeta
from anchorpy.borsh_extension import BorshPubkey
from construct import Construct
import borsh_construct as borsh
from ..program_id import PROGRAM_ID


class SetAllowedProgramsArgs(typing.TypedDict):
    allowed_programs: list[PublicKey]


layout = borsh.CStruct(
    "allowed_programs" / borsh.Vec(typing.cast(Construct, BorshPubkey))
)
SET_ALLOWED_PROGRAMS_ACCOUNTS_WHITELIST = PublicKey.find_program_address(
    seeds=[b"message", b"whitelist"],
    program_id=PROGRAM_ID,
)[0]


class SetAllowedProgramsAccounts(typing.TypedDict):
    payer: PublicKey
    authority: PublicKey


def set_allowed_programs(
    args: SetAllowedProgramsArgs,
    accounts: SetAllowedProgramsAccounts,
    program_id: PublicKey = PROGRAM_ID,
    remaining_accounts: typing.Optional[typing.List[AccountMeta]] = None,
) -> TransactionInstruction:
    keys: list[AccountMeta] = [
        AccountMeta(pubkey=accounts["payer"], is_signer=True, is_writable=True),
        AccountMeta(pubkey=accounts["authority"], is_signer=True, is_writable=False),
        AccountMeta(
            pubkey=SET_ALLOWED_PROGRAMS_ACCOUNTS_WHITELIST,
            is_signer=False,
            is_writable=True,
        ),
    ]
    if remaining_accounts is not None:
        keys += remaining_accounts
    identifier = b"\xf2\xf0\xb2\x03\xbc!\xa1\xb6"
    encoded_args = layout.build(
        {
            "allowed_programs": args["allowed_programs"],
        }
    )
    data = identifier + encoded_args
    return TransactionInstruction(keys, program_id, data)
