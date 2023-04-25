from __future__ import annotations
import typing
from solders.pubkey import Pubkey
from solders.system_program import ID as SYS_PROGRAM_ID
from solders.instruction import Instruction, AccountMeta
from anchorpy.borsh_extension import BorshPubkey
from construct import Construct
import borsh_construct as borsh
from ..program_id import PROGRAM_ID


class PutAllArgs(typing.TypedDict):
    base_account_key: Pubkey
    messages: list[bytes]


layout = borsh.CStruct(
    "base_account_key" / BorshPubkey,
    "messages" / borsh.Vec(typing.cast(Construct, borsh.Bytes)),
)


class PutAllAccounts(typing.TypedDict):
    fund: Pubkey
    whitelist_verifier: WhitelistVerifierNested


class WhitelistVerifierNested(typing.TypedDict):
    whitelist: Pubkey
    cpi_caller_auth: Pubkey


def put_all(
    args: PutAllArgs,
    accounts: PutAllAccounts,
    program_id: Pubkey = PROGRAM_ID,
    remaining_accounts: typing.Optional[typing.List[AccountMeta]] = None,
) -> Instruction:
    keys: list[AccountMeta] = [
        AccountMeta(pubkey=accounts["fund"], is_signer=False, is_writable=True),
        AccountMeta(
            pubkey=accounts["whitelist_verifier"]["whitelist"],
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
    return Instruction(program_id, data, keys)
