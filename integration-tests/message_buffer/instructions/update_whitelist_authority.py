from __future__ import annotations
import typing
from solana.publickey import PublicKey
from solana.transaction import TransactionInstruction, AccountMeta
from anchorpy.borsh_extension import BorshPubkey
import borsh_construct as borsh
from ..program_id import PROGRAM_ID


class UpdateWhitelistAuthorityArgs(typing.TypedDict):
    new_authority: PublicKey


layout = borsh.CStruct("new_authority" / BorshPubkey)
UPDATE_WHITELIST_AUTHORITY_ACCOUNTS_WHITELIST = PublicKey.find_program_address(
    seeds=[b"message", b"whitelist"],
    program_id=PROGRAM_ID,
)[0]


class UpdateWhitelistAuthorityAccounts(typing.TypedDict):
    payer: PublicKey
    authority: PublicKey


def update_whitelist_authority(
    args: UpdateWhitelistAuthorityArgs,
    accounts: UpdateWhitelistAuthorityAccounts,
    program_id: PublicKey = PROGRAM_ID,
    remaining_accounts: typing.Optional[typing.List[AccountMeta]] = None,
) -> TransactionInstruction:
    keys: list[AccountMeta] = [
        AccountMeta(pubkey=accounts["payer"], is_signer=True, is_writable=True),
        AccountMeta(pubkey=accounts["authority"], is_signer=True, is_writable=False),
        AccountMeta(
            pubkey=UPDATE_WHITELIST_AUTHORITY_ACCOUNTS_WHITELIST,
            is_signer=False,
            is_writable=True,
        ),
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
    return TransactionInstruction(keys, program_id, data)
