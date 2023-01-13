import asyncio
import json
import os
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
from jsonrpc_websocket import Server

LOGGER = logging.getLogger(__name__)

# Keypairs
# BujGr9ChcuaCJhxeFEvGvaCFTxSV1CUCSVHL1SVFpU4i
PROGRAM_KEYPAIR = [28, 50, 87, 251, 247, 251, 45, 91, 139, 128, 72, 197, 172, 61, 15, 60, 76, 81, 6, 13, 94, 109, 212, 28, 40, 110, 61, 207, 40, 34, 37,
                   216, 162, 22, 222, 173, 4, 56, 58, 79, 253, 77, 93, 134, 47, 144, 105, 188, 77, 237, 92, 194, 133, 54, 94, 129, 10, 19, 192, 115, 215, 209, 28, 155]
# BTJKZngp3vzeJiRmmT9PitQH4H29dhQZ1GNhxFfDi4kw
MAPPING_KEYPAIR = [62, 251, 237, 123, 32, 23, 77, 112, 75, 109, 141, 142, 101, 235, 231, 46, 82, 224, 124, 182, 136, 15, 157, 13, 130, 60, 8, 251, 212, 255,
                   116, 8, 155, 81, 141, 223, 90, 30, 205, 238, 119, 249, 130, 159, 191, 87, 136, 130, 225, 86, 103, 26, 255, 105, 59, 48, 101, 66, 157, 174, 106, 186, 51, 72]
# 5F1MvPpfXytDPJb7beKiFEzdMacbzc5DmKrHzvzEorH8
PUBLISHER_KEYPAIR = [28, 188, 185, 140, 54, 34, 203, 52, 83, 136, 217, 69, 104, 188, 165, 215, 42, 23, 73, 14, 87, 84, 155, 47, 91, 166, 208, 129, 10,
                     67, 4, 72, 63, 5, 73, 112, 194, 37, 117, 20, 46, 66, 102, 78, 196, 75, 127, 90, 40, 85, 69, 209, 12, 237, 118, 39, 218, 157, 86, 251, 112, 61, 104, 235]

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
    "metadata": {"jump_id": "78876709", "jump_symbol": "BTCUSD", "price_exp": -8},
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
    "metadata": {"jump_id": "186", "jump_symbol": "AAPL", "price_exp": -5},
}
ETH_USD = {
    "account": "",
    "attr_dict": {
        "symbol": "Crypto.ETH/USD",
        "asset_type": "Crypto",
        "base": "ETH",
        "quote_currency": "USD",
        "generic_symbol": "ETHUSD",
        "description": "ETH/USD",
    },
    "metadata": {"jump_id": "12345", "jump_symbol": "ETHUSD", "price_exp": -8},
}

asyncio.set_event_loop(asyncio.new_event_loop())


class PythdClient:

    def __init__(self, address: str) -> None:
        self.address: str = address
        self.server: Server = None

    async def connect(self) -> Server:
        self.server = Server(self.address)
        task = await self.server.ws_connect()
        task.add_done_callback(self._on_connection_done)
        LOGGER.debug("connected to pythd websocket server at %s", self.address)

    async def close(self) -> None:
        await self.server.close()
        LOGGER.debug("closed pythd connection")

    @staticmethod
    def _on_connection_done(task):
        LOGGER.debug("pythd connection closed")
        if not task.cancelled() and task.exception() is not None:
            e = task.exception()
            LOGGER.error(e, exc_info=1)

    async def update_price(self, account: str, price: int, conf: int, status: str) -> None:
        LOGGER.info("sending update_price(account=%s, price=%s, conf=%s, status=%s)",
                    account, price, conf, status)
        await self.server.update_price(account=account, price=price, conf=conf, status=status)

    async def get_all_products(self) -> List:
        products = await self.server.get_all_products()
        LOGGER.info("get_all_products result: %s", products)
        return products

    async def get_product(self, account) -> Any:
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
                with subprocess.Popen(cmd.split(), stdout=stdout, stderr=stderr, env=env) as process:
                    LOGGER.debug(
                        "Spawned subprocess with command %s logging to %s", cmd, log_dir)
                    yield
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
        with self.spawn(f"solana-test-validator --ledger {ledger_path}", log_dir=log_dir):
            time.sleep(3)
            yield

    @pytest.fixture
    def validator_logs(self, ledger_path, validator):
        log_dir = os.path.join(ledger_path, "solana_logs")
        with self.spawn("solana logs --url localhost", log_dir=log_dir):
            LOGGER.debug("Capturing solana logs at %s", log_dir)
            yield

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
    def deploy_program_keypair(self, tmp_path):
        path = os.path.join(tmp_path, "program_keypair.json")
        with open(path, 'w') as f:
            f.write(json.dumps(PROGRAM_KEYPAIR))
            f.flush()
        yield path

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
        yield path

    @pytest.fixture
    def oracle_program(self, funding_keypair, deploy_program_keypair, validator, validator_logs):
        LOGGER.debug("Airdropping SOL to funding keypair at %s",
                     funding_keypair)
        self.run(f"solana airdrop 100 -k {funding_keypair} -u localhost")

        LOGGER.debug("Deploying Oracle program")
        _, _, program_account = self.run(
            f"solana program deploy -k {funding_keypair} -u localhost --program-id {deploy_program_keypair} oracle.so").stdout.split()

        LOGGER.info("Oracle program account: %s", program_account)

        os.environ["PROGRAM_KEY"] = program_account

        yield program_account

    @pytest_asyncio.fixture
    async def sync_accounts(self, oracle_program, sync_key_path, sync_mapping_keypair, refdata_products, refdata_publishers, refdata_permissions):
        LOGGER.debug("Syncing Oracle program accounts")
        os.environ["TEST_MODE"] = "1"
        await ProgramAdmin(
            network="localhost",
            key_dir=sync_key_path,
            program_key=oracle_program,
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
        self.run(f"solana airdrop 100 -k {path} -u localhost")
        time.sleep(8)

    @pytest.fixture
    def agent_keystore(self, agent_keystore_path, agent_publish_keypair):
        self.run(
            f"../scripts/init_key_store.sh localnet {agent_keystore_path}")
        if os.path.exists("keystore"):
            os.remove("keystore")
        os.symlink(agent_keystore_path, "keystore")

    @pytest.fixture
    def agent(self, sync_accounts, agent_keystore, tmp_path):
        LOGGER.debug("Building agent binary")
        self.run("cargo build --release")

        log_dir = os.path.join(tmp_path, "agent_logs")
        LOGGER.debug("Launching agent logging to %s", log_dir)

        os.environ["RUST_BACKTRACE"] = "full"
        with self.spawn("../target/release/agent --config agent_conf.toml", log_dir=log_dir):
            time.sleep(3)
            yield

    @pytest_asyncio.fixture
    async def pythd(self, agent):
        pythd = PythdClient(address="ws://localhost:8910")
        await pythd.connect()
        yield pythd
        await pythd.close()


class TestUpdatePrice(PythTest):

    @pytest.mark.asyncio
    async def test_update_price_simple(self, pythd: PythdClient):
        # Fetch all products
        products = {product["attr_dict"]["symbol"]: product for product in await pythd.get_all_products()}

        # Find the product account ID corresponding to the BTC/USD symbol
        product = products[BTC_USD["attr_dict"]["symbol"]]
        product_account = product["account"]

        # Get the price account with which to send updates
        price_account = product["price_accounts"][0]["account"]

        # Send an "update_price" request
        await pythd.update_price(price_account, 42, 2, "trading")
        time.sleep(2)

        # Send another "update_price" request to trigger aggregation
        await pythd.update_price(price_account, 81, 1, "trading")
        time.sleep(2)

        # Confirm that the price account has been updated with the values from the first "update_price" request
        product = await pythd.get_product(product_account)
        price_account = product["price_accounts"][0]
        assert price_account["price"] == 42
        assert price_account["conf"] == 2
        assert price_account["status"] == "trading"
