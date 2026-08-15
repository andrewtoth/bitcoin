// Copyright (c) The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <index/txospenderindex.h>

#include <chain.h>
#include <common/args.h>
#include <consensus/validation.h>
#include <crypto/siphash.h>
#include <dbwrapper.h>
#include <index/disktxpos.h>
#include <index/txindex_key.h>
#include <interfaces/chain.h>
#include <node/blockstorage.h>
#include <primitives/block.h>
#include <script/script.h>
#include <serialize.h>
#include <sync.h>
#include <test/util/common.h>
#include <test/util/setup_common.h>
#include <uint256.h>
#include <util/byte_units.h>
#include <util/check.h>
#include <validation.h>

#include <ios>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_SUITE(txospenderindex_tests)

class TxoSpenderIndexTest
{
public:
    static CDBWrapper& GetDB(const TxoSpenderIndex& index) { return index.GetDB(); }
    static CBlockLocator ReadBestBlock(const TxoSpenderIndex& index) { return index.GetDB().ReadBestBlock(); }
    static void WriteBestBlock(const TxoSpenderIndex& index, const CBlockLocator& locator)
    {
        auto& db{index.GetDB()};
        CDBBatch batch{db};
        db.WriteBestBlock(batch, locator);
        db.WriteBatch(batch);
    }
};

namespace {

constexpr uint8_t DB_TXOSPENDERINDEX{'s'};

struct LegacyDBKey {
    uint64_t hash{0};
    CDiskTxPos pos;

    SERIALIZE_METHODS(LegacyDBKey, obj)
    {
        uint8_t prefix{DB_TXOSPENDERINDEX};
        READWRITE(prefix);
        if (ser_action.ForRead() && prefix != DB_TXOSPENDERINDEX) {
            throw std::ios_base::failure("Invalid format for spender index DB key");
        }
        READWRITE(obj.hash);
        READWRITE(obj.pos);
    }
};

void InvalidateBlock(ChainstateManager& chainman, const uint256& block_hash)
{
    CBlockIndex* block_index{WITH_LOCK(cs_main, return chainman.m_blockman.LookupBlockIndex(block_hash))};
    BOOST_REQUIRE(block_index);
    BlockValidationState state;
    BOOST_REQUIRE(chainman.ActiveChainstate().InvalidateBlock(state, block_index));
}

std::vector<txindex::BlockTxPosition> BucketPositions(CDBWrapper& db, txindex::TxHashKeyPrefix prefix)
{
    std::vector<txindex::BlockTxPosition> positions;
    std::unique_ptr<CDBIterator> it{db.NewIterator()};
    txindex::DBKey key{prefix, {}};
    for (it->Seek(key); it->Valid() && it->GetKey(key) && key.hash_prefix == prefix; it->Next()) {
        positions.push_back(key.pos);
    }
    return positions;
}

SipHasher13UJ ReadHasher(const CDBWrapper& db)
{
    std::pair<uint64_t, uint64_t> salt;
    BOOST_REQUIRE(db.Read(txindex::DB_TXID_HASH_SALT, salt));
    return SipHasher13UJ{salt.first, salt.second};
}

} // namespace

BOOST_FIXTURE_TEST_CASE(txospenderindex_initial_sync, TestChain100Setup)
{
    // Setup phase:
    // Mine blocks for coinbase maturity, so we can spend some coinbase outputs in the test.
    const CScript& coinbase_script = m_coinbase_txns[0]->vout[0].scriptPubKey;
    for (int i = 0; i < 10; i++) CreateAndProcessBlock({}, coinbase_script);

    // Spend 10 outputs
    std::vector<COutPoint> spent(10);
    std::vector<CMutableTransaction> spender(spent.size());
    for (size_t i = 0; i < spent.size(); i++) {
        // Outpoint
        auto coinbase_tx = m_coinbase_txns[i];
        spent[i] = COutPoint(coinbase_tx->GetHash(), 0);

        // Spending tx
        spender[i].version = 1;
        spender[i].vin.resize(1);
        spender[i].vin[0].prevout.hash = spent[i].hash;
        spender[i].vin[0].prevout.n = spent[i].n;
        spender[i].vout.resize(1);
        spender[i].vout[0].nValue = coinbase_tx->GetValueOut();
        spender[i].vout[0].scriptPubKey = coinbase_script;

        // Sign
        std::vector<unsigned char> vchSig;
        const uint256 hash = SignatureHash(coinbase_script, spender[i], 0, SIGHASH_ALL, 0, SigVersion::BASE);
        BOOST_REQUIRE(coinbaseKey.Sign(hash, vchSig));
        vchSig.push_back((unsigned char)SIGHASH_ALL);
        spender[i].vin[0].scriptSig << vchSig;
    }

    // Generate and ensure block has been fully processed
    const uint256 tip_hash = CreateAndProcessBlock(spender, coinbase_script).GetHash();
    m_node.validation_signals->SyncWithValidationInterfaceQueue();
    BOOST_CHECK_EQUAL(WITH_LOCK(::cs_main, return m_node.chainman->ActiveTip()->GetBlockHash()), tip_hash);

    // Now we concluded the setup phase, run index
    TxoSpenderIndex txospenderindex(interfaces::MakeChain(m_node), 1 << 20, true);
    BOOST_REQUIRE(txospenderindex.Init());
    BOOST_CHECK(!txospenderindex.BlockUntilSyncedToCurrentChain()); // false when not synced
    BOOST_CHECK_NE(txospenderindex.GetSummary().best_block_hash, tip_hash);

    // Transaction should not be found in the index before it is synced.
    for (const auto& outpoint : spent) {
        BOOST_CHECK(!txospenderindex.FindSpender(outpoint).value());
    }

    txospenderindex.Sync();
    BOOST_CHECK_EQUAL(txospenderindex.GetSummary().best_block_hash, tip_hash);

    for (size_t i = 0; i < spent.size(); i++) {
        const auto tx_spender{txospenderindex.FindSpender(spent[i])};
        BOOST_REQUIRE(tx_spender.has_value());
        BOOST_REQUIRE(tx_spender->has_value());
        BOOST_CHECK_EQUAL((*tx_spender)->tx->GetHash(), spender[i].GetHash());
        BOOST_CHECK_EQUAL((*tx_spender)->block_hash, tip_hash);
    }

    // Shutdown sequence (c.f. Shutdown() in init.cpp)
    txospenderindex.Stop();
}

BOOST_FIXTURE_TEST_CASE(txospenderindex_locator_upgrade, TestChain100Setup)
{
    uint256 legacy_hash, new_hash;
    {
        LOCK(cs_main);
        legacy_hash = Assert(m_node.chainman->ActiveChain()[1])->GetBlockHash();
        new_hash = Assert(m_node.chainman->ActiveChain().Tip())->GetBlockHash();
    }
    CBlockLocator legacy_locator{{legacy_hash}}, new_locator{{new_hash}};
    { CDBWrapper{DBParams{.path = gArgs.GetDataDirNet() / "indexes" / "txospenderindex" / "db", .cache_bytes = 1_MiB}}.Write(uint8_t{'B'}, legacy_locator); }

    TxoSpenderIndex index(interfaces::MakeChain(m_node), /*n_cache_size=*/1_MiB, /*f_memory=*/false);
    BOOST_CHECK(TxoSpenderIndexTest::ReadBestBlock(index).vHave == legacy_locator.vHave);

    TxoSpenderIndexTest::WriteBestBlock(index, new_locator);
    BOOST_CHECK(TxoSpenderIndexTest::ReadBestBlock(index).vHave == new_locator.vHave);

    CBlockLocator stored_legacy_locator;
    BOOST_REQUIRE(TxoSpenderIndexTest::GetDB(index).Read(uint8_t{'B'}, stored_legacy_locator));
    BOOST_CHECK(stored_legacy_locator.vHave == legacy_locator.vHave);
}

BOOST_FIXTURE_TEST_CASE(txospenderindex_legacy_fallback, TestChain100Setup)
{
    const CScript& coinbase_script = m_coinbase_txns[0]->vout[0].scriptPubKey;
    for (int i = 0; i < 10; i++) CreateAndProcessBlock({}, coinbase_script);

    CMutableTransaction spender;
    spender.version = 1;
    spender.vin.resize(1);
    spender.vin[0].prevout = COutPoint(m_coinbase_txns[0]->GetHash(), 0);
    spender.vout.resize(1);
    spender.vout[0].nValue = m_coinbase_txns[0]->GetValueOut();
    spender.vout[0].scriptPubKey = coinbase_script;
    std::vector<unsigned char> vchSig;
    const uint256 sighash = SignatureHash(coinbase_script, spender, 0, SIGHASH_ALL, 0, SigVersion::BASE);
    BOOST_REQUIRE(coinbaseKey.Sign(sighash, vchSig));
    vchSig.push_back((unsigned char)SIGHASH_ALL);
    spender.vin[0].scriptSig << vchSig;

    const CBlock spend_block{CreateAndProcessBlock({spender}, coinbase_script)};
    m_node.validation_signals->SyncWithValidationInterfaceQueue();

    CDiskTxPos legacy_pos;
    {
        LOCK(cs_main);
        const CBlockIndex* tip{Assert(m_node.chainman->ActiveTip())};
        legacy_pos = CDiskTxPos({tip->nFile, tip->nDataPos}, GetSizeOfCompactSize(spend_block.vtx.size()));
        legacy_pos.nTxOffset += spend_block.vtx[0]->ComputeTotalSize();
    }

    const COutPoint spent{spender.vin[0].prevout};
    const std::pair<uint64_t, uint64_t> legacy_salt{0x0102030405060708ULL, 0x1112131415161718ULL};
    const LegacyDBKey legacy_key{PresaltedSipHasher(legacy_salt.first, legacy_salt.second)(spent.hash.ToUint256(), spent.n), legacy_pos};
    {
        CDBWrapper db{DBParams{.path = gArgs.GetDataDirNet() / "indexes" / "txospenderindex" / "db", .cache_bytes = 1_MiB}};
        db.Write("siphash_key", legacy_salt);
        db.Write(legacy_key, txindex::EMPTY_VALUE);
    }

    TxoSpenderIndex index(interfaces::MakeChain(m_node), /*n_cache_size=*/1_MiB, /*f_memory=*/false);
    BOOST_REQUIRE(TxoSpenderIndexTest::GetDB(index).Exists(legacy_key));
    BOOST_REQUIRE(index.Init());
    index.Sync();
    BOOST_REQUIRE(TxoSpenderIndexTest::GetDB(index).Exists(legacy_key));

    CDBWrapper& db{TxoSpenderIndexTest::GetDB(index)};
    const auto prefix{txindex::CreateKeyPrefix(ReadHasher(db), spent.hash.ToUint256(), spent.n)};
    for (const auto& pos : BucketPositions(db, prefix)) {
        db.Erase(txindex::DBKey{prefix, pos});
    }

    const auto tx_spender{index.FindSpender(spent)};
    BOOST_REQUIRE(tx_spender.has_value());
    BOOST_REQUIRE(tx_spender->has_value());
    BOOST_CHECK_EQUAL((*tx_spender)->tx->GetHash(), spender.GetHash());

    index.Stop();
}

BOOST_FIXTURE_TEST_CASE(txospenderindex_reorg_erases_entries, TestChain100Setup)
{
    TxoSpenderIndex index(interfaces::MakeChain(m_node), /*n_cache_size=*/1_MiB, /*f_memory=*/true);
    BOOST_REQUIRE(index.Init());
    index.Sync();

    const CScript coinbase_script{CScript() << ToByteVector(coinbaseKey.GetPubKey()) << OP_CHECKSIG};
    CMutableTransaction unique_mtx{CreateValidMempoolTransaction(
        /*input_transaction=*/m_coinbase_txns[0],
        /*input_vout=*/0,
        /*input_height=*/1,
        /*input_signing_key=*/coinbaseKey,
        /*output_destination=*/CScript() << OP_TRUE,
        /*output_amount=*/CAmount{1 * COIN},
        /*submit=*/false)};
    const COutPoint spent{unique_mtx.vin[0].prevout};
    const uint256 stale_block_hash{CreateAndProcessBlock({unique_mtx}, coinbase_script).GetHash()};
    BOOST_REQUIRE(index.BlockUntilSyncedToCurrentChain());

    {
        const auto tx_spender{index.FindSpender(spent)};
        BOOST_REQUIRE(tx_spender.has_value());
        BOOST_REQUIRE(tx_spender->has_value());
        BOOST_CHECK_EQUAL((*tx_spender)->block_hash, stale_block_hash);
    }

    InvalidateBlock(*m_node.chainman, stale_block_hash);
    BOOST_REQUIRE(index.BlockUntilSyncedToCurrentChain());

    const auto tx_spender{index.FindSpender(spent)};
    BOOST_REQUIRE(tx_spender.has_value());
    BOOST_CHECK(!tx_spender->has_value());

    index.Stop();
}

BOOST_AUTO_TEST_SUITE_END()
