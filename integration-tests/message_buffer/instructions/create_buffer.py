from __future__ import annotations
import typing
from solana.publickey import PublicKey
from solana.system_program import SYS_PROGRAM_ID
from solana.transaction import TransactionInstruction, AccountMeta
from anchorpy.borsh_extension import BorshPubkey
import borsh_construct as borsh
from ..program_id import PROGRAM_ID


class CreateBufferArgs(typing.TypedDict):
    allowed_program_auth: PublicKey
    base_account_key: PublicKey
    target_size: int


layout = borsh.CStruct(
    "allowed_program_auth" / BorshPubkey,
    "base_account_key" / BorshPubkey,
    "target_size" / borsh.U32,
)
CREATE_BUFFER_ACCOUNTS_WHITELIST = PublicKey.find_program_address(
    seeds=[b"message", b"whitelist"],
    program_id=PROGRAM_ID,
)[0]


class CreateBufferAccounts(typing.TypedDict):
    admin: PublicKey


def create_buffer(
    args: CreateBufferArgs,
    accounts: CreateBufferAccounts,
    program_id: PublicKey = PROGRAM_ID,
    remaining_accounts: typing.Optional[typing.List[AccountMeta]] = None,
) -> TransactionInstruction:
    keys: list[AccountMeta] = [
        AccountMeta(
            pubkey=CREATE_BUFFER_ACCOUNTS_WHITELIST, is_signer=False, is_writable=False
        ),
        AccountMeta(pubkey=accounts["admin"], is_signer=True, is_writable=True),
        AccountMeta(pubkey=SYS_PROGRAM_ID, is_signer=False, is_writable=False),
    ]
    if remaining_accounts is not None:
        keys += remaining_accounts
    identifier = b"\xafLeJ\xe0\xf9h\xaa"
    encoded_args = layout.build(
        {
            "allowed_program_auth": args["allowed_program_auth"],
            "base_account_key": args["base_account_key"],
            "target_size": args["target_size"],
        }
    )
    data = identifier + encoded_args
    return TransactionInstruction(keys, program_id, data)
