from __future__ import annotations
import typing
from solders.pubkey import Pubkey
from solders.instruction import Instruction, AccountMeta
from anchorpy.borsh_extension import BorshPubkey
import borsh_construct as borsh
from ..program_id import PROGRAM_ID


class UpdateWhitelistAuthorityArgs(typing.TypedDict):
    new_authority: Pubkey


layout = borsh.CStruct("new_authority" / BorshPubkey)


class UpdateWhitelistAuthorityAccounts(typing.TypedDict):
    payer: Pubkey
    authority: Pubkey
    whitelist: Pubkey


def update_whitelist_authority(
    args: UpdateWhitelistAuthorityArgs,
    accounts: UpdateWhitelistAuthorityAccounts,
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
    identifier = b'\xdc\x82J"\xa9\x8d\x92\xc8'
    encoded_args = layout.build(
        {
            "new_authority": args["new_authority"],
        }
    )
    data = identifier + encoded_args
    return Instruction(program_id, data, keys)
