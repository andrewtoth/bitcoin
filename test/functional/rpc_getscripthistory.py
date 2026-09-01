#!/usr/bin/env python3
# Copyright (c) The Bitcoin Core developers
# Distributed under the MIT software license, see the accompanying
# file COPYING or http://www.opensource.org/licenses/mit-license.php.
"""Test getscripthistory RPC and scriptpubkeyindex."""

import hashlib

from test_framework.test_framework import BitcoinTestFramework
from test_framework.util import (
    assert_equal,
    assert_raises_rpc_error,
)
from test_framework.wallet import MiniWallet


def scripthash(script_hex):
    """SHA256(scriptPubKey) in Bitcoin / Electrum display byte order."""
    return hashlib.sha256(bytes.fromhex(script_hex)).digest()[::-1].hex()


def strip_spentness(tx):
    """Copy a verbose tx object without spent/spending annotations or height."""
    out = dict(tx)
    out.pop("height", None)
    if "vout" in out:
        new_vout = []
        for vout in out["vout"]:
            vout = dict(vout)
            vout.pop("spent", None)
            vout.pop("spending", None)
            new_vout.append(vout)
        out["vout"] = new_vout
    return out


class GetScriptHistoryTest(BitcoinTestFramework):
    def set_test_params(self):
        self.num_nodes = 1
        self.extra_args = [["-scriptpubkeyindex", "-txospenderindex", "-txindex"]]

    def wait_for_indexes(self, node):
        self.wait_until(lambda: all(i["synced"] for i in node.getindexinfo().values()))

    def run_test(self):
        node0 = self.nodes[0]
        self.wallet = MiniWallet(node0)

        self.log.info("Index is required")
        self.restart_node(0, extra_args=[])
        assert_raises_rpc_error(-1, "scriptpubkeyindex is not enabled", node0.getscripthistory, "00" * 32)
        self.restart_node(0, extra_args=self.extra_args[0])
        self.wallet.rescan_utxos()
        self.wait_for_indexes(node0)

        self.log.info("Height range validation")
        dummy = "00" * 32
        assert_raises_rpc_error(-8, "start_height cannot be negative", node0.getscripthistory, dummy, 0, {"start_height": -1})
        assert_raises_rpc_error(-8, "end_height must be greater than or equal to start_height", node0.getscripthistory, dummy, 0, {"start_height": 5, "end_height": 4})
        assert_raises_rpc_error(-5, "Invalid address, descriptor, or scripthash", node0.getscripthistory, "not-a-script")

        self.log.info("Index coinbase and later payments to MiniWallet script")
        utxo = self.wallet.get_utxo()
        spk = self.wallet.get_output_script().hex()
        sh = scripthash(spk)

        self.wait_for_indexes(node0)
        history = node0.getscripthistory(sh)
        assert any(tx["txid"] == utxo["txid"] for tx in history)
        funding_entry = next(tx for tx in history if tx["txid"] == utxo["txid"])
        assert_equal(funding_entry["hex"], node0.getrawtransaction(utxo["txid"]))
        assert_equal(funding_entry["vout"][0]["spent"], False)

        self.log.info("Address and descriptor queries match scripthash")
        addr = self.wallet.get_address()
        assert_equal(node0.getscripthistory(addr), history)
        assert_equal(node0.getscripthistory(self.wallet.get_descriptor()), history)
        assert_equal(node0.getscripthistory(f"addr({addr})"), history)
        assert_raises_rpc_error(-8, "range is only valid for ranged descriptors", node0.getscripthistory, addr, 0, {"range": 10})
        assert_raises_rpc_error(-8, "range is only valid for ranged descriptors", node0.getscripthistory, sh, 0, {"range": 10})

        self.log.info("Payment with two outputs to the same script is one history entry")
        pay = self.wallet.send_self_transfer_multi(from_node=node0, utxos_to_spend=[utxo], num_outputs=2)
        blockhash = self.generate(self.wallet, 1)[0]
        self.wait_for_indexes(node0)

        history = node0.getscripthistory(sh)
        pay_entries = [tx for tx in history if tx["txid"] == pay["txid"]]
        assert_equal(len(pay_entries), 1)
        assert_equal(pay_entries[0]["blockhash"], blockhash)
        assert_equal(pay_entries[0]["hex"], node0.getrawtransaction(pay["txid"]))
        assert_equal(sorted(out["n"] for out in pay_entries[0]["vout"]), [0, 1])
        assert all(not out["spent"] for out in pay_entries[0]["vout"])

        self.log.info("Verbosity 1 matches getrawtransaction")
        history_v1 = node0.getscripthistory(sh, 1)
        pay_v1 = next(tx for tx in history_v1 if tx["txid"] == pay["txid"])
        assert_equal(strip_spentness(pay_v1), node0.getrawtransaction(pay["txid"], 1))
        assert_equal(sorted(out["n"] for out in pay_v1["vout"] if "spent" in out), [0, 1])
        assert all(out["spent"] is False for out in pay_v1["vout"] if "spent" in out)

        self.log.info("Height range skips earlier transactions")
        pay_height = pay_entries[0]["height"]
        ranged = node0.getscripthistory(sh, 0, {"start_height": pay_height, "end_height": pay_height})
        ranged_txids = [tx["txid"] for tx in ranged]
        assert pay["txid"] in ranged_txids
        assert utxo["txid"] not in ranged_txids
        before = node0.getscripthistory(sh, 0, {"start_height": 0, "end_height": pay_height - 1})
        assert pay["txid"] not in [tx["txid"] for tx in before]

        self.log.info("Spending marks the output spent and includes the spender")
        spend_utxo = pay["new_utxos"][0]
        spend = self.wallet.send_self_transfer(from_node=node0, utxo_to_spend=spend_utxo)

        self.log.info("Unconfirmed matching transactions are included from the mempool")
        history = node0.getscripthistory(sh)
        pay_entries = next(tx for tx in history if tx["txid"] == pay["txid"])
        spent_out = next(out for out in pay_entries["vout"] if out["n"] == 0)
        assert_equal(spent_out["spent"], True)
        assert_equal(spent_out["spending"], node0.getrawtransaction(spend["txid"]))
        mempool_entry = next(tx for tx in history if tx["txid"] == spend["txid"])
        assert_equal(mempool_entry["height"], 0)
        assert "blockhash" not in mempool_entry
        assert_equal(mempool_entry["hex"], node0.getrawtransaction(spend["txid"]))

        history_v1 = node0.getscripthistory(sh, 1)
        mempool_v1 = next(tx for tx in history_v1 if tx["txid"] == spend["txid"])
        raw_v1 = node0.getrawtransaction(spend["txid"], 1)
        assert_equal(mempool_v1["txid"], raw_v1["txid"])
        assert_equal(mempool_v1["hex"], raw_v1["hex"])
        assert_equal(mempool_v1["vin"], raw_v1["vin"])
        assert "blockhash" not in mempool_v1

        no_mempool = node0.getscripthistory(sh, 0, {"include_mempool": False})
        assert spend["txid"] not in [tx["txid"] for tx in no_mempool]
        pay_nomem = next(tx for tx in no_mempool if tx["txid"] == pay["txid"])
        spent_nomem = next(out for out in pay_nomem["vout"] if out["n"] == 0)
        assert_equal(spent_nomem["spent"], False)
        assert "spending" not in spent_nomem

        self.log.info("Chained mempool transaction uses height -1")
        child = self.wallet.send_self_transfer(from_node=node0, utxo_to_spend=spend["new_utxo"])
        history = node0.getscripthistory(sh)
        child_entry = next(tx for tx in history if tx["txid"] == child["txid"])
        assert_equal(child_entry["height"], -1)
        assert "blockhash" not in child_entry
        spend_entry = next(tx for tx in history if tx["txid"] == spend["txid"])
        spend_vout = next(out for out in spend_entry["vout"] if "spent" in out)
        assert_equal(spend_vout["spent"], True)
        assert_equal(spend_vout["spending"], node0.getrawtransaction(child["txid"]))

        spend_block = self.generate(self.wallet, 1)[0]
        self.wait_for_indexes(node0)

        history = node0.getscripthistory(sh)
        pay_entries = [tx for tx in history if tx["txid"] == pay["txid"]][0]
        spent_out = next(out for out in pay_entries["vout"] if out["n"] == 0)
        unspent_out = next(out for out in pay_entries["vout"] if out["n"] == 1)
        assert_equal(spent_out["spent"], True)
        assert_equal(spent_out["spending"], node0.getrawtransaction(spend["txid"]))
        assert_equal(unspent_out["spent"], False)
        assert "spending" not in unspent_out

        self.log.info("Verbosity 1 nested spender matches getrawtransaction")
        history_v1 = node0.getscripthistory(sh, True)
        pay_v1 = next(tx for tx in history_v1 if tx["txid"] == pay["txid"])
        spent_v1 = next(out for out in pay_v1["vout"] if out["n"] == 0)
        unspent_v1 = next(out for out in pay_v1["vout"] if out["n"] == 1)
        assert_equal(spent_v1["spent"], True)
        spending_v1 = dict(spent_v1["spending"])
        spending_v1.pop("height")
        assert_equal(spending_v1, node0.getrawtransaction(spend["txid"], 1))
        assert_equal(unspent_v1["spent"], False)
        assert "spending" not in unspent_v1

        self.log.info("Verbosity 2 includes fee/prevout and nested spender at the same verbosity")
        history_v2 = node0.getscripthistory(sh, 2)
        pay_v2 = next(tx for tx in history_v2 if tx["txid"] == pay["txid"])
        assert_equal(strip_spentness(pay_v2), node0.getrawtransaction(pay["txid"], 2))
        spent_v2 = next(out for out in pay_v2["vout"] if out["n"] == 0)
        spending_v2 = dict(spent_v2["spending"])
        spending_v2.pop("height")
        assert_equal(spending_v2, node0.getrawtransaction(spend["txid"], 2))
        assert "prevout" in pay_v2["vin"][0]
        assert "fee" in pay_v2
        assert "prevout" in spent_v2["spending"]["vin"][0]
        assert "fee" in spent_v2["spending"]

        self.log.info("Reorg removes disconnected transactions from confirmed history")
        self.nodes[0].invalidateblock(spend_block)
        self.wait_for_indexes(node0)
        history = node0.getscripthistory(sh)
        pay_entries = [tx for tx in history if tx["txid"] == pay["txid"]][0]
        spent_out = next(out for out in pay_entries["vout"] if out["n"] == 0)
        # After invalidate, the spend is back in the mempool, so the confirmed
        # output is marked spent and the spender is a mempool history entry.
        assert_equal(spent_out["spent"], True)
        assert_equal(spent_out["spending"], node0.getrawtransaction(spend["txid"]))
        spend_entry = next(tx for tx in history if tx["txid"] == spend["txid"])
        assert_equal(spend_entry["height"], 0)
        child_entry = next(tx for tx in history if tx["txid"] == child["txid"])
        assert_equal(child_entry["height"], -1)

        confirmed_only = node0.getscripthistory(sh, 0, {"include_mempool": False})
        pay_confirmed = next(tx for tx in confirmed_only if tx["txid"] == pay["txid"])
        spent_confirmed = next(out for out in pay_confirmed["vout"] if out["n"] == 0)
        assert_equal(spent_confirmed["spent"], False)
        assert spend["txid"] not in [tx["txid"] for tx in confirmed_only]
        assert child["txid"] not in [tx["txid"] for tx in confirmed_only]

        self.nodes[0].reconsiderblock(spend_block)
        self.wait_for_indexes(node0)
        history = node0.getscripthistory(sh)
        pay_entries = [tx for tx in history if tx["txid"] == pay["txid"]][0]
        spent_out = next(out for out in pay_entries["vout"] if out["n"] == 0)
        assert_equal(spent_out["spent"], True)
        assert_equal(spent_out["spending"], node0.getrawtransaction(spend["txid"]))

        self.log.info("Spentness is reported without txospenderindex; spending is omitted")
        self.restart_node(0, extra_args=["-scriptpubkeyindex", "-txindex"])
        self.wait_for_indexes(node0)
        history = node0.getscripthistory(sh, 1)
        pay_entries = next(tx for tx in history if tx["txid"] == pay["txid"])
        spent_out = next(out for out in pay_entries["vout"] if out["n"] == 0)
        assert_equal(spent_out["spent"], True)
        assert "spending" not in spent_out


if __name__ == "__main__":
    GetScriptHistoryTest(__file__).main()
