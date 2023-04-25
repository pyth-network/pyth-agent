from __future__ import annotations
import typing
from solana.publickey import PublicKey
from solana.system_program import SYS_PROGRAM_ID
from solana.transaction import TransactionInstruction, AccountMeta
from anchorpy.borsh_extension import BorshPubkey
import borsh_construct as borsh
from ..program_id import PROGRAM_ID


class InitializeArgs(typing.TypedDict):
    authority: PublicKey


layout = borsh.CStruct("authority" / BorshPubkey)
INITIALIZE_ACCOUNTS_WHITELIST = PublicKey.find_program_address(
    seeds=[b"message", b"whitelist"],
    program_id=PROGRAM_ID,
)[0]


class InitializeAccounts(typing.TypedDict):
    payer: PublicKey


def initialize(
    args: InitializeArgs,
    accounts: InitializeAccounts,
    program_id: PublicKey = PROGRAM_ID,
    remaining_accounts: typing.Optional[typing.List[AccountMeta]] = None,
) -> TransactionInstruction:
    keys: list[AccountMeta] = [
        AccountMeta(pubkey=accounts["payer"], is_signer=True, is_writable=True),
        AccountMeta(
            pubkey=INITIALIZE_ACCOUNTS_WHITELIST, is_signer=False, is_writable=True
        ),
        AccountMeta(pubkey=SYS_PROGRAM_ID, is_signer=False, is_writable=False),
    ]
    if remaining_accounts is not None:
        keys += remaining_accounts
    identifier = b"\xaf\xafm\x1f\r\x98\x9b\xed"
    encoded_args = layout.build(
        {
            "authority": args["authority"],
        }
    )
    data = identifier + encoded_args
    return TransactionInstruction(keys, program_id, data)
