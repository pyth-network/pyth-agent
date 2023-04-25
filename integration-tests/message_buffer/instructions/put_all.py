from __future__ import annotations
import typing
from solana.publickey import PublicKey
from solana.system_program import SYS_PROGRAM_ID
from solana.transaction import TransactionInstruction, AccountMeta
from anchorpy.borsh_extension import BorshPubkey
from construct import Construct
import borsh_construct as borsh
from ..program_id import PROGRAM_ID


class PutAllArgs(typing.TypedDict):
    base_account_key: PublicKey
    messages: list[bytes]


layout = borsh.CStruct(
    "base_account_key" / BorshPubkey,
    "messages" / borsh.Vec(typing.cast(Construct, borsh.Bytes)),
)
WHITELIST_VERIFIER_NESTED_WHITELIST = PublicKey.find_program_address(
    seeds=[b"message", b"whitelist"],
    program_id=PROGRAM_ID,
)[0]
PUT_ALL_ACCOUNTS_FUND = PublicKey.find_program_address(
    seeds=[b"fund"],
    program_id=PROGRAM_ID,
)[0]


class PutAllAccounts(typing.TypedDict):
    whitelist_verifier: WhitelistVerifierNested


class WhitelistVerifierNested(typing.TypedDict):
    cpi_caller_auth: PublicKey


def put_all(
    args: PutAllArgs,
    accounts: PutAllAccounts,
    program_id: PublicKey = PROGRAM_ID,
    remaining_accounts: typing.Optional[typing.List[AccountMeta]] = None,
) -> TransactionInstruction:
    keys: list[AccountMeta] = [
        AccountMeta(pubkey=PUT_ALL_ACCOUNTS_FUND, is_signer=False, is_writable=True),
        AccountMeta(
            pubkey=WHITELIST_VERIFIER_NESTED_WHITELIST,
            is_signer=False,
            is_writable=False,
        ),
        AccountMeta(
            pubkey=accounts["whitelist_verifier"]["cpi_caller_auth"],
            is_signer=True,
            is_writable=False,
        ),
        AccountMeta(pubkey=SYS_PROGRAM_ID, is_signer=False, is_writable=False),
    ]
    if remaining_accounts is not None:
        keys += remaining_accounts
    identifier = b"\xd4\xe1\xc1[\x97\xee\x14]"
    encoded_args = layout.build(
        {
            "base_account_key": args["base_account_key"],
            "messages": args["messages"],
        }
    )
    data = identifier + encoded_args
    return TransactionInstruction(keys, program_id, data)
