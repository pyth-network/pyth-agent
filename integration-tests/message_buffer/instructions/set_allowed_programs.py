from __future__ import annotations
import typing
from solders.pubkey import Pubkey
from solders.instruction import Instruction, AccountMeta
from anchorpy.borsh_extension import BorshPubkey
from construct import Construct
import borsh_construct as borsh
from ..program_id import PROGRAM_ID


class SetAllowedProgramsArgs(typing.TypedDict):
    allowed_programs: list[Pubkey]


layout = borsh.CStruct(
    "allowed_programs" / borsh.Vec(typing.cast(Construct, BorshPubkey))
)


class SetAllowedProgramsAccounts(typing.TypedDict):
    payer: Pubkey
    authority: Pubkey
    whitelist: Pubkey


def set_allowed_programs(
    args: SetAllowedProgramsArgs,
    accounts: SetAllowedProgramsAccounts,
    program_id: Pubkey = PROGRAM_ID,
    remaining_accounts: typing.Optional[typing.List[AccountMeta]] = None,
) -> Instruction:
    keys: list[AccountMeta] = [
        AccountMeta(pubkey=accounts["payer"], is_signer=True, is_writable=True),
        AccountMeta(pubkey=accounts["authority"], is_signer=True, is_writable=False),
        AccountMeta(pubkey=accounts["whitelist"], is_signer=False, is_writable=True),
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
    return Instruction(program_id, data, keys)
