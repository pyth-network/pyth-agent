from __future__ import annotations
import typing
from solana.publickey import PublicKey
from solana.system_program import SYS_PROGRAM_ID
from solana.transaction import TransactionInstruction, AccountMeta
from anchorpy.borsh_extension import BorshPubkey
import borsh_construct as borsh
from ..program_id import PROGRAM_ID


class ResizeBufferArgs(typing.TypedDict):
    allowed_program_auth: PublicKey
    base_account_key: PublicKey
    buffer_bump: int
    target_size: int


layout = borsh.CStruct(
    "allowed_program_auth" / BorshPubkey,
    "base_account_key" / BorshPubkey,
    "buffer_bump" / borsh.U8,
    "target_size" / borsh.U32,
)
RESIZE_BUFFER_ACCOUNTS_WHITELIST = PublicKey.find_program_address(
    seeds=[b"message", b"whitelist"],
    program_id=PROGRAM_ID,
)[0]


class ResizeBufferAccounts(typing.TypedDict):
    admin: PublicKey


def resize_buffer(
    args: ResizeBufferArgs,
    accounts: ResizeBufferAccounts,
    program_id: PublicKey = PROGRAM_ID,
    remaining_accounts: typing.Optional[typing.List[AccountMeta]] = None,
) -> TransactionInstruction:
    keys: list[AccountMeta] = [
        AccountMeta(
            pubkey=RESIZE_BUFFER_ACCOUNTS_WHITELIST, is_signer=False, is_writable=False
        ),
        AccountMeta(pubkey=accounts["admin"], is_signer=True, is_writable=True),
        AccountMeta(pubkey=SYS_PROGRAM_ID, is_signer=False, is_writable=False),
    ]
    if remaining_accounts is not None:
        keys += remaining_accounts
    identifier = b"\x05\xb5\x15\xbf\xd8N\xf5/"
    encoded_args = layout.build(
        {
            "allowed_program_auth": args["allowed_program_auth"],
            "base_account_key": args["base_account_key"],
            "buffer_bump": args["buffer_bump"],
            "target_size": args["target_size"],
        }
    )
    data = identifier + encoded_args
    return TransactionInstruction(keys, program_id, data)
