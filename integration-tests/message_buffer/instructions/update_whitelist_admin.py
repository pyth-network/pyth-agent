from __future__ import annotations
import typing
from solana.publickey import PublicKey
from solana.transaction import TransactionInstruction, AccountMeta
from anchorpy.borsh_extension import BorshPubkey
import borsh_construct as borsh
from ..program_id import PROGRAM_ID


class UpdateWhitelistAdminArgs(typing.TypedDict):
    new_admin: PublicKey


layout = borsh.CStruct("new_admin" / BorshPubkey)
UPDATE_WHITELIST_ADMIN_ACCOUNTS_WHITELIST = PublicKey.find_program_address(
    seeds=[b"message", b"whitelist"],
    program_id=PROGRAM_ID,
)[0]


class UpdateWhitelistAdminAccounts(typing.TypedDict):
    payer: PublicKey
    admin: PublicKey


def update_whitelist_admin(
    args: UpdateWhitelistAdminArgs,
    accounts: UpdateWhitelistAdminAccounts,
    program_id: PublicKey = PROGRAM_ID,
    remaining_accounts: typing.Optional[typing.List[AccountMeta]] = None,
) -> TransactionInstruction:
    keys: list[AccountMeta] = [
        AccountMeta(pubkey=accounts["payer"], is_signer=True, is_writable=True),
        AccountMeta(pubkey=accounts["admin"], is_signer=True, is_writable=False),
        AccountMeta(
            pubkey=UPDATE_WHITELIST_ADMIN_ACCOUNTS_WHITELIST,
            is_signer=False,
            is_writable=True,
        ),
    ]
    if remaining_accounts is not None:
        keys += remaining_accounts
    identifier = b"r\x1e\xf9\x9a\xb6\x17\x94b"
    encoded_args = layout.build(
        {
            "new_admin": args["new_admin"],
        }
    )
    data = identifier + encoded_args
    return TransactionInstruction(keys, program_id, data)
