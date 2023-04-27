import asyncio
import json
import os
import requests
import time
from typing import Any, List
import pytest
import subprocess
import logging
from program_admin import ProgramAdmin
from program_admin.parsing import (
    parse_permissions_json,
    parse_products_json,
    parse_publishers_json,
)
import pytest_asyncio
from pathlib import Path
from contextlib import contextmanager
import shutil
from solana.keypair import Keypair
from solders.system_program import ID as SYSTEM_PROGRAM_ID
from solana.rpc.async_api import AsyncClient
from solana.transaction import AccountMeta, Transaction, TransactionInstruction
from anchorpy import Provider, Wallet
from construct import Bytes, Int32sl, Int32ul, Struct
from solana.publickey import PublicKey
from message_buffer.instructions import initialize, set_allowed_programs, create_buffer
from jsonrpc_websocket import Server

LOGGER = logging.getLogger(__name__)

# The core Pyth oracle address
ORACLE_PROGRAM = "BujGr9ChcuaCJhxeFEvGvaCFTxSV1CUCSVHL1SVFpU4i"
# The pythnet-specific accumulator message buffer address
MESSAGE_BUFFER_PROGRAM = "Vbmv1jt4vyuqBZcpYPpnVhrqVe5e6ZPb6JxDcffRHUM"

# BTJKZngp3vzeJiRmmT9PitQH4H29dhQZ1GNhxFfDi4kw
MAPPING_KEYPAIR = [62, 251, 237, 123, 32, 23, 77, 112, 75, 109, 141, 142, 101, 235, 231, 46, 82, 224, 124, 182, 136, 15, 157, 13, 130, 60, 8, 251, 212, 255,
                   116, 8, 155, 81, 141, 223, 90, 30, 205, 238, 119, 249, 130, 159, 191, 87, 136, 130, 225, 86, 103, 26, 255, 105, 59, 48, 101, 66, 157, 174, 106, 186, 51, 72]
# 5F1MvPpfXytDPJb7beKiFEzdMacbzc5DmKrHzvzEorH8
PUBLISHER_KEYPAIR = [28, 188, 185, 140, 54, 34, 203, 52, 83, 136, 217, 69, 104, 188, 165, 215, 42, 23, 73, 14, 87, 84, 155, 47, 91, 166, 208, 129, 10,
                     67, 4, 72, 63, 5, 73, 112, 194, 37, 117, 20, 46, 66, 102, 78, 196, 75, 127, 90, 40, 85, 69, 209, 12, 237, 118, 39, 218, 157, 86, 251, 112, 61, 104, 235]

# NOTE: Set to a value to run agent with accumulator support. Should be used with a pythnet validator, set below.
USE_ACCUMULATOR = os.environ.get("USE_ACCUMULATOR") is not None

# NOTE: Set to a local pythnet solana-test-validator binary for accumulator integration testing. Does not imply USE_ACCUMULATOR
SOLANA_TEST_VALIDATOR = os.environ.get("SOLANA_TEST_VALIDATOR", "solana-test-validator")

# Product account metadata
BTC_USD = {
    "account": "",
    "attr_dict": {
        "symbol": "Crypto.BTC/USD",
        "asset_type": "Crypto",
        "base": "BTC",
        "quote_currency": "USD",
        "generic_symbol": "BTCUSD",
        "description": "BTC/USD",
    },
    "metadata": {"jump_id": "78876709", "jump_symbol": "BTCUSD", "price_exp": -8, "min_publishers": 1},
}
AAPL_USD = {
    "account": "",
    "attr_dict": {
        "asset_type": "Equity",
        "country": "US",
        "description": "APPLE INC",
        "quote_currency": "USD",
        "cms_symbol": "AAPL",
        "cqs_symbol": "AAPL",
        "nasdaq_symbol": "AAPL",
        "symbol": "Equity.US.AAPL/USD",
        "base": "AAPL",
    },
    "metadata": {"jump_id": "186", "jump_symbol": "AAPL", "price_exp": -5, "min_publishers": 1},
}
ALL_PRODUCTS=[BTC_USD, AAPL_USD]

asyncio.set_event_loop(asyncio.new_event_loop())


class PythAgentClient:

    def __init__(self, address: str) -> None:
        self.address: str = address
        self.server: Server = None

    async def connect(self) -> Server:
        self.server = Server(self.address)
        task = await self.server.ws_connect()
        task.add_done_callback(self._on_connection_done)
        LOGGER.debug(
            "connected to pyth agent websocket server at %s", self.address)

    async def close(self) -> None:
        await self.server.close()
        LOGGER.debug("closed pyth agent websocket connection")

    @staticmethod
    def _on_connection_done(task):
        LOGGER.debug("pyth agent connection closed")
        if not task.cancelled() and task.exception() is not None:
            e = task.exception()
            LOGGER.error(e, exc_info=1)

    async def update_price(self, account: str, price: int, conf: int, status: str) -> None:
        LOGGER.info("sending update_price(account=%s, price=%s, conf=%s, status=%s)",
                    account, price, conf, status)
        await self.server.update_price(account=account, price=price, conf=conf, status=status)

    async def get_all_products(self) -> List:
        LOGGER.info("sending get_all_products()")
        products = await self.server.get_all_products()
        LOGGER.info("get_all_products result: %s", products)
        return products

    async def get_product(self, account) -> Any:
        LOGGER.info(f"sending get_product({account})")
        product = await self.server.get_product(account=account)
        LOGGER.info("get_product(%s) result: %s", account, product)
        return product


class PythTest:

    def run(self, cmd):
        LOGGER.debug("Running command %s", cmd)
        try:
            return subprocess.run(cmd.split(), capture_output=True, text=True, check=True)
        except subprocess.CalledProcessError as e:
            LOGGER.error("Error running command %s", cmd)
            LOGGER.error(e, exc_info=1)
            LOGGER.error(e.output)
            LOGGER.error(e.stderr)
            raise

    @contextmanager
    def spawn(self, cmd, log_dir=None):
        os.makedirs(log_dir)
        stdout_path = os.path.join(log_dir, "stdout")
        stderr_path = os.path.join(log_dir, "stderr")

        env = {}
        env.update(os.environ)

        with open(stdout_path, 'w') as stdout:
            with open(stderr_path, 'w') as stderr:
                process = subprocess.Popen(cmd.split(), stdout=stdout, stderr=stderr, env=env)
                LOGGER.debug(
                    "Spawned subprocess with command %s logging to %s", cmd, log_dir)
                yield

                process.poll() # fills return code if available

                if process.returncode is not None and process.returncode != 0:
                    LOGGER.error("Spawned process \"%s\" finished with error code %d before teardown. See logs in %s", cmd, process.returncode, log_dir)

                process.terminate()
                process.wait()

                stderr.flush()
            stdout.flush()

        LOGGER.debug("Terminated subprocess running command %s", cmd)

    @pytest.fixture
    def ledger_path(self, tmp_path):
        path = os.path.join(tmp_path, "ledger")
        os.makedirs(path)
        LOGGER.info("Ledger path: %s", path)
        yield path

    @pytest.fixture
    def validator(self, ledger_path):
        log_dir = os.path.join(ledger_path, "validator_log")

        maybe_message_buffer = f"--bpf-program {MESSAGE_BUFFER_PROGRAM} message_buffer.so" if USE_ACCUMULATOR else ""

        command = f"{SOLANA_TEST_VALIDATOR} --ledger {ledger_path} --bpf-program {ORACLE_PROGRAM} oracle.so {maybe_message_buffer}"
        with self.spawn(command, log_dir=log_dir):
            time.sleep(15) # Debug-built binaries need a little more time
            yield

    '''
    @pytest.fixture
    def validator_logs(self, ledger_path, validator):
        log_dir = os.path.join(ledger_path, "solana_logs")
        with self.spawn("solana logs --url localhost", log_dir=log_dir):
            LOGGER.debug("Capturing solana logs at %s", log_dir)
            yield
    '''

    @pytest.fixture
    def sync_key_path(self, tmp_path):
        path = os.path.join(tmp_path, "sync_keystore")
        os.makedirs(path)
        LOGGER.info("Account sync keystore path: %s", path)
        yield path

    @pytest.fixture
    def sync_mapping_keypair(self, sync_key_path):
        with open(f"{sync_key_path}/mapping_0.json", 'w') as f:
            f.write(json.dumps(MAPPING_KEYPAIR))
            f.flush()

        pubkey = str(Keypair.from_secret_key(MAPPING_KEYPAIR).public_key)

        shutil.copyfile(f"{sync_key_path}/mapping_0.json",
                        f"{sync_key_path}/account_{pubkey}.json")


    @pytest.fixture
    def refdata_path(self, tmp_path):
        path = os.path.join(tmp_path, "sync_refdata")
        os.makedirs(path)
        LOGGER.info("Account sync refdata path: %s", path)
        yield path

    @pytest.fixture
    def refdata_products(self, refdata_path):
        path = os.path.join(refdata_path, 'products.json')
        with open(path, 'w') as f:
            f.write(json.dumps([BTC_USD, AAPL_USD]))
            f.flush()
            yield f.name

    @pytest.fixture
    def refdata_publishers(self, refdata_path):
        path = os.path.join(refdata_path, 'publishers.json')
        with open(path, 'w') as f:
            f.write(json.dumps({"some_publisher": str(
                Keypair.from_secret_key(PUBLISHER_KEYPAIR).public_key)}))
            f.flush()
            yield f.name

    @pytest.fixture
    def refdata_permissions(self, refdata_path):
        path = os.path.join(refdata_path, 'permissions.json')
        with open(path, 'w') as f:
            f.write(json.dumps({
                    "AAPL": {"price": ["some_publisher"]},
                    "BTCUSD": {"price": ["some_publisher"]},
                    }))
            f.flush()
            yield f.name

    @pytest.fixture
    def funding_keypair(self, sync_key_path):
        path = os.path.join(sync_key_path, "funding.json")
        self.run(
            f"solana-keygen new --no-bip39-passphrase --outfile {path}")
        self.run(
            f"solana airdrop 1000 -k {path} -u localhost")

        yield path


    @pytest_asyncio.fixture
    async def sync_accounts(self, validator, funding_keypair, sync_key_path, sync_mapping_keypair, refdata_products, refdata_publishers, refdata_permissions):
        LOGGER.debug("Syncing Oracle program accounts")
        os.environ["TEST_MODE"] = "1"
        await ProgramAdmin(
            network="localhost",
            key_dir=sync_key_path,
            program_key=ORACLE_PROGRAM,
            commitment="confirmed",
        ).sync(
            parse_products_json(Path(refdata_products)),
            parse_publishers_json(Path(refdata_publishers)),
            parse_permissions_json(Path(refdata_permissions)),
            generate_keys=True,
        )

    @pytest.fixture
    def agent_keystore_path(self, tmp_path):
        path = os.path.join(tmp_path, "agent_keystore")
        os.makedirs(path)
        LOGGER.debug("Agent keystore path: %s", path)
        yield path

    @pytest.fixture
    def agent_publish_keypair(self, agent_keystore_path, sync_accounts):
        path = os.path.join(agent_keystore_path, "publish_key_pair.json")
        with open(path, 'w') as f:
            f.write(json.dumps(PUBLISHER_KEYPAIR))
            f.flush()

        LOGGER.debug("Airdropping SOL to publish keypair at %s", path)
        self.run(f"solana airdrop 1000 -k {path} -u localhost")
        address = self.run(f"solana address -k {path} -u localhost")
        balance = self.run(f"solana balance -k {path} -u localhost")
        LOGGER.debug(f"Publisher {address.stdout.strip()} balance: {balance.stdout.strip()}")
        time.sleep(8)

    @pytest.fixture
    def agent_keystore(self, agent_keystore_path, agent_publish_keypair):
        self.run(
            f"../scripts/init_key_store.sh localnet {agent_keystore_path}")

        if USE_ACCUMULATOR:
            path = os.path.join(agent_keystore_path, "accumulator_program_key.json")

            with open(path, 'w') as f:
                f.write(MESSAGE_BUFFER_PROGRAM)

        if os.path.exists("keystore"):
            os.remove("keystore")
        os.symlink(agent_keystore_path, "keystore")

    @pytest_asyncio.fixture
    async def initialize_message_buffer_program(self, funding_keypair, sync_key_path, sync_accounts):

        if not USE_ACCUMULATOR:
            return

        keypair_file = open(funding_keypair)
        parsed_funding_keypair = Keypair.from_secret_key(json.load(keypair_file))

        client = AsyncClient("http://localhost:8899/")
        provider = Provider(client, Wallet(parsed_funding_keypair))

        init_ix = initialize({
                "admin": parsed_funding_keypair.public_key,
             }, {
                "payer": parsed_funding_keypair.public_key,
             })

        tx = Transaction().add(init_ix)

        oracle_pubkey = PublicKey(ORACLE_PROGRAM)
        msg_buf_pubkey = PublicKey(MESSAGE_BUFFER_PROGRAM)
        oracle_auth_pda, _ = PublicKey.find_program_address(
            [b"upd_price_write", bytes(msg_buf_pubkey)],
            oracle_pubkey
        )

        LOGGER.info(f"Oracle Auth PDA: {oracle_auth_pda}")

        set_allowed_ix = set_allowed_programs({
            "allowed_programs": [oracle_auth_pda],
        }, {
            "payer": parsed_funding_keypair.public_key,
            "admin": parsed_funding_keypair.public_key,
        })

        tx.add(set_allowed_ix)

        for product in ALL_PRODUCTS:
            jump_symbol = product["metadata"]["jump_symbol"]
            address_string = self.run(f"solana address -k {sync_key_path}/price_{jump_symbol}.json -u localhost").stdout.strip()
            LOGGER.info(f"{jump_symbol} price account: {address_string}")
            address = PublicKey(address_string)

            message_buffer_pda, _ = PublicKey.find_program_address(
                [bytes(oracle_auth_pda), b"message", bytes(address)],
                msg_buf_pubkey
            )

            ix = create_buffer({
                "allowed_program_auth": oracle_auth_pda,
                "base_account_key": address,
                "target_size": 1024
            }, {
                "admin": parsed_funding_keypair.public_key,
            },
                remaining_accounts = [
                    AccountMeta(pubkey=message_buffer_pda, is_signer=False, is_writable=True)
                ]
            )
            tx.add(ix)

        await provider.send(tx, [parsed_funding_keypair])

    @pytest.fixture
    def agent_config(self, agent_keystore, tmp_path):
        with open("agent_conf.toml") as config_file:
            agent_config = config_file.read()

            # Add accumulator setting if option is enabled
            if USE_ACCUMULATOR:
                agent_config += '\nkey_store.accumulator_key_path = "accumulator_program_key.json"'

            path = os.path.join(tmp_path, "agent_conf.toml")

            with open(path, 'w') as f:
                f.write(agent_config)

            return path



    @pytest.fixture
    def agent(self, sync_accounts, agent_keystore, tmp_path, initialize_message_buffer_program, agent_config):
        LOGGER.debug("Building agent binary")
        self.run("cargo build --release")

        log_dir = os.path.join(tmp_path, "agent_logs")
        LOGGER.debug("Launching agent logging to %s", log_dir)

        os.environ["RUST_BACKTRACE"] = "full"
        os.environ["RUST_LOG"] = "debug"
        with self.spawn(f"../target/release/agent --config {agent_config}", log_dir=log_dir):
            time.sleep(3)
            yield

    @pytest.fixture
    def agent_hotload(self, sync_accounts, agent_keystore, agent_keystore_path, tmp_path, initialize_message_buffer_program, agent_config):
        """
        Spawns an agent without a publish keypair, used for keypair hotloading testing
        """
        os.remove(os.path.join(agent_keystore_path, "publish_key_pair.json"))

        LOGGER.debug("Building hotload agent binary")
        self.run("cargo build --release")

        log_dir = os.path.join(tmp_path, "agent_logs")
        LOGGER.debug("Launching hotload agent logging to %s", log_dir)

        os.environ["RUST_BACKTRACE"] = "full"
        os.environ["RUST_LOG"] = "debug"
        with self.spawn(f"../target/release/agent --config {agent_config}", log_dir=log_dir):
            time.sleep(3)
            yield

    @pytest_asyncio.fixture
    async def client(self, agent):
        client = PythAgentClient(address="ws://localhost:8910")
        await client.connect()
        yield client
        await client.close()

    @pytest_asyncio.fixture
    async def client_hotload(self, agent_hotload):
        client = PythAgentClient(address="ws://localhost:8910")
        await client.connect()
        yield client
        await client.close()


class TestUpdatePrice(PythTest):

    @pytest.mark.asyncio
    async def test_update_price_simple(self, client: PythAgentClient):
        # Fetch all products
        products = {product["attr_dict"]["symbol"]: product for product in await client.get_all_products()}

        # Find the product account ID corresponding to the BTC/USD symbol
        product = products[BTC_USD["attr_dict"]["symbol"]]
        product_account = product["account"]

        # Get the price account with which to send updates
        price_account = product["price_accounts"][0]["account"]

        # Send an "update_price" request
        await client.update_price(price_account, 42, 2, "trading")
        time.sleep(1)

        # Send another "update_price" request to trigger aggregation
        await client.update_price(price_account, 81, 1, "trading")
        time.sleep(2)

        # Confirm that the price account has been updated with the values from the first "update_price" request
        final_product_state = await client.get_product(product_account)

        final_price_account = final_product_state["price_accounts"][0]
        assert final_price_account["price"] == 42
        assert final_price_account["conf"] == 2
        assert final_price_account["status"] == "trading"

    @pytest.mark.asyncio
    async def test_update_price_simple_with_keypair_hotload(self, client_hotload: PythAgentClient):
        # Hotload the keypair into running agent
        hl_request = requests.post("http://localhost:9001/primary/load_keypair", json=PUBLISHER_KEYPAIR)

        # Verify succesful hotload
        assert hl_request.status_code == 200

        LOGGER.info("Publisher keypair hotload OK")

        # Continue normally with the existing simple scenario
        await self.test_update_price_simple(client_hotload)

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Test not meant for automatic CI")
    async def test_publish_forever(self, client: PythAgentClient):
        '''
        Convenience test routine for manual experiments on a running test setup.
        '''
        # Fetch all products
        products = {product["attr_dict"]["symbol"]: product for product in await client.get_all_products()}

        # Find the product account ID corresponding to the BTC/USD symbol
        product = products[BTC_USD["attr_dict"]["symbol"]]
        product_account = product["account"]

        # Get the price account with which to send updates
        price_account = product["price_accounts"][0]["account"]

        while True:
            # Send an "update_price" request
            await client.update_price(price_account, 47, 2, "trading")
            time.sleep(1)

            # Send another "update_price" request to trigger aggregation
            await client.update_price(price_account, 81, 1, "trading")
            time.sleep(2)
