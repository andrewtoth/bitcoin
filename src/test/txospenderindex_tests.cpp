// Copyright (c) The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#include <chain.h>
#include <common/args.h>
#include <consensus/validation.h>
#include <crypto/siphash.h>
#include <dbwrapper.h>
#include <index/disktxpos.h>
#include <index/txindex_key.h>
#include <index/txospenderindex.h>
#include <index/txospenderindex_key.h>
#include <interfaces/chain.h>
#include <key.h>
#include <node/blockstorage.h>
#include <primitives/block.h>
#include <primitives/transaction.h>
#include <script/interpreter.h>
#include <script/script.h>
#include <streams.h>
#include <sync.h>
#include <test/util/common.h>
#include <test/util/setup_common.h>
#include <uint256.h>
#include <util/byte_units.h>
#include <util/check.h>
#include <util/strencodings.h>
#include <validation.h>

#include <boost/test/unit_test.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

BOOST_AUTO_TEST_SUITE(txospenderindex_tests)

// Grants tests access to the otherwise non-public spender-index database handle.
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

SipHasher13UJ ReadHasher(const CDBWrapper& db)
{
    std::pair<uint64_t, uint64_t> salt;
    BOOST_REQUIRE(db.Read(txospenderindex::DB_OUTPOINT_HASH_SALT, salt));
    return SipHasher13UJ{salt.first, salt.second};
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

CDiskTxPos LegacyPosForTx(ChainstateManager& chainman, const uint256& block_hash, const Txid& txid)
{
    LOCK(cs_main);
    const CBlockIndex* block_index{chainman.m_blockman.LookupBlockIndex(block_hash)};
    BOOST_REQUIRE(block_index);
    CBlock block;
    BOOST_REQUIRE(chainman.m_blockman.ReadBlock(block, *block_index));
    uint32_t n_tx_offset{GetSizeOfCompactSize(block.vtx.size())};
    for (const auto& tx : block.vtx) {
        if (tx->GetHash() == txid) {
            return {{block_index->nFile, block_index->nDataPos}, n_tx_offset};
        }
        n_tx_offset += tx->ComputeTotalSize();
    }
    BOOST_FAIL("transaction not found in block");
    return {};
}

struct SpentOutput {
    COutPoint outpoint;
    Txid spender_txid;
    uint256 block_hash;
};

SpentOutput MineSpend(TestChain100Setup& setup)
{
    const CScript& coinbase_script = setup.m_coinbase_txns[0]->vout[0].scriptPubKey;
    for (int i = 0; i < 10; i++)
        setup.CreateAndProcessBlock({}, coinbase_script);

    auto coinbase_tx = setup.m_coinbase_txns[0];
    COutPoint spent{coinbase_tx->GetHash(), 0};

    CMutableTransaction spender;
    spender.version = 1;
    spender.vin.resize(1);
    spender.vin[0].prevout = spent;
    spender.vout.resize(1);
    spender.vout[0].nValue = coinbase_tx->GetValueOut();
    spender.vout[0].scriptPubKey = coinbase_script;

    std::vector<unsigned char> vchSig;
    const uint256 hash = SignatureHash(coinbase_script, spender, 0, SIGHASH_ALL, 0, SigVersion::BASE);
    BOOST_REQUIRE(setup.coinbaseKey.Sign(hash, vchSig));
    vchSig.push_back(static_cast<unsigned char>(SIGHASH_ALL));
    spender.vin[0].scriptSig << vchSig;

    const uint256 block_hash{setup.CreateAndProcessBlock({spender}, coinbase_script).GetHash()};
    setup.m_node.validation_signals->SyncWithValidationInterfaceQueue();
    return {spent, spender.GetHash(), block_hash};
}

} // namespace

BOOST_AUTO_TEST_CASE(txospenderindex_hash_prefix)
{
    BOOST_CHECK_EQUAL(
        txospenderindex::CreateKeyPrefix(
            SipHasher13UJ{0x0706050403020100ULL, 0x0F0E0D0C0B0A0908ULL},
            COutPoint{Txid{"1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100"}, 0}),
        0xe465cdf37dULL);

    // Hashed rows reuse txindex::DBKey: type prefix, 5-byte siphash, VARINT height, 3-byte offset.
    BOOST_CHECK_EQUAL(HexStr(DataStream{} << txindex::DBKey{0x0102030405, {1, 2}}),
                      "78010203040501000002");
}

BOOST_FIXTURE_TEST_CASE(txospenderindex_initial_sync, TestChain100Setup)
{
    // Setup phase:
    // Mine blocks for coinbase maturity, so we can spend some coinbase outputs in the test.
    const CScript& coinbase_script = m_coinbase_txns[0]->vout[0].scriptPubKey;
    for (int i = 0; i < 10; i++)
        CreateAndProcessBlock({}, coinbase_script);

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
        BOOST_REQUIRE((*tx_spender)->tx);
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
    {
        CDBWrapper{DBParams{.path = gArgs.GetDataDirNet() / "indexes" / "txospenderindex" / "db", .cache_bytes = 1_MiB}}.Write(uint8_t{'B'}, legacy_locator);
    }

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
    const auto spent{MineSpend(*this)};
    const CDiskTxPos legacy_pos{LegacyPosForTx(*m_node.chainman, spent.block_hash, spent.spender_txid)};
    constexpr std::pair<uint64_t, uint64_t> siphash_key{1, 2};
    const uint64_t prefix{PresaltedSipHasher(siphash_key.first, siphash_key.second)(spent.outpoint.hash.ToUint256(), spent.outpoint.n)};
    {
        CDBWrapper db{DBParams{.path = gArgs.GetDataDirNet() / "indexes" / "txospenderindex" / "db", .cache_bytes = 1_MiB}};
        db.Write(txospenderindex::DB_LEGACY_SIPHASH_KEY, siphash_key);
        db.Write(txospenderindex::LegacyDBKey{prefix, legacy_pos}, txindex::EMPTY_VALUE);
    }

    TxoSpenderIndex index(interfaces::MakeChain(m_node), /*n_cache_size=*/1_MiB, /*f_memory=*/false);
    BOOST_REQUIRE(index.HasLegacyEntries());
    BOOST_REQUIRE(index.Init());
    index.Sync();

    // Drop the hashed entries so only the legacy row remains, then confirm the
    // lookup succeeds through the fallback.
    CDBWrapper& db{TxoSpenderIndexTest::GetDB(index)};
    const auto hashed_prefix{txospenderindex::CreateKeyPrefix(ReadHasher(db), spent.outpoint)};
    const auto bucket{BucketPositions(db, hashed_prefix)};
    BOOST_REQUIRE(!bucket.empty());
    for (const auto& pos : bucket)
        db.Erase(txindex::DBKey{hashed_prefix, pos});

    const auto result{index.FindSpender(spent.outpoint)};
    BOOST_REQUIRE(result.has_value());
    BOOST_REQUIRE(result->has_value());
    BOOST_REQUIRE((*result)->tx);
    BOOST_CHECK_EQUAL((*result)->tx->GetHash(), spent.spender_txid);
    BOOST_CHECK_EQUAL((*result)->block_hash, spent.block_hash);

    index.Stop();
}

BOOST_FIXTURE_TEST_CASE(txospenderindex_fresh_skips_legacy, TestChain100Setup)
{
    const auto spent{MineSpend(*this)};

    TxoSpenderIndex index(interfaces::MakeChain(m_node), /*n_cache_size=*/1_MiB, /*f_memory=*/false);
    BOOST_CHECK(!index.HasLegacyEntries());
    BOOST_REQUIRE(index.Init());
    index.Sync();

    // A database created fresh by this version cannot contain legacy entries, so
    // lookups skip the legacy fallback: drop the hashed entry and re-add it under
    // the old 's' schema, then confirm the lookup misses.
    CDBWrapper& db{TxoSpenderIndexTest::GetDB(index)};
    const auto hashed_prefix{txospenderindex::CreateKeyPrefix(ReadHasher(db), spent.outpoint)};
    const auto bucket{BucketPositions(db, hashed_prefix)};
    BOOST_REQUIRE_EQUAL(bucket.size(), 1U);
    db.Erase(txindex::DBKey{hashed_prefix, bucket.front()});

    constexpr std::pair<uint64_t, uint64_t> siphash_key{1, 2};
    const uint64_t prefix{PresaltedSipHasher(siphash_key.first, siphash_key.second)(spent.outpoint.hash.ToUint256(), spent.outpoint.n)};
    db.Write(txospenderindex::DB_LEGACY_SIPHASH_KEY, siphash_key);
    db.Write(txospenderindex::LegacyDBKey{prefix, LegacyPosForTx(*m_node.chainman, spent.block_hash, spent.spender_txid)},
             txindex::EMPTY_VALUE);

    const auto result{index.FindSpender(spent.outpoint)};
    BOOST_REQUIRE(result.has_value());
    BOOST_CHECK(!result->has_value());

    index.Stop();
}

BOOST_FIXTURE_TEST_CASE(txospenderindex_pruned_returns_blockhash, TestChain100Setup)
{
    const auto spent{MineSpend(*this)};

    TxoSpenderIndex index(interfaces::MakeChain(m_node), /*n_cache_size=*/1_MiB, /*f_memory=*/true);
    BOOST_REQUIRE(index.Init());
    index.Sync();

    {
        const auto result{index.FindSpender(spent.outpoint)};
        BOOST_REQUIRE(result.has_value());
        BOOST_REQUIRE(result->has_value());
        BOOST_REQUIRE((*result)->tx);
        BOOST_CHECK_EQUAL((*result)->block_hash, spent.block_hash);
    }

    {
        LOCK(cs_main);
        CBlockIndex* block_index{m_node.chainman->m_blockman.LookupBlockIndex(spent.block_hash)};
        BOOST_REQUIRE(block_index);
        block_index->nStatus &= ~BLOCK_HAVE_DATA;
    }

    const auto result{index.FindSpender(spent.outpoint)};
    BOOST_REQUIRE(result.has_value());
    BOOST_REQUIRE(result->has_value());
    BOOST_CHECK(!(*result)->tx);
    BOOST_CHECK_EQUAL((*result)->block_hash, spent.block_hash);

    index.Stop();
}

BOOST_FIXTURE_TEST_CASE(txospenderindex_reorg_removes_stale, TestChain100Setup)
{
    const auto spent{MineSpend(*this)};

    TxoSpenderIndex index(interfaces::MakeChain(m_node), /*n_cache_size=*/1_MiB, /*f_memory=*/true);
    BOOST_REQUIRE(index.Init());
    index.Sync();

    {
        const auto result{index.FindSpender(spent.outpoint)};
        BOOST_REQUIRE(result.has_value());
        BOOST_REQUIRE(result->has_value());
        BOOST_CHECK_EQUAL((*result)->block_hash, spent.block_hash);
    }

    CBlockIndex* block_index{WITH_LOCK(cs_main, return m_node.chainman->m_blockman.LookupBlockIndex(spent.block_hash))};
    BOOST_REQUIRE(block_index);
    BlockValidationState state;
    BOOST_REQUIRE(m_node.chainman->ActiveChainstate().InvalidateBlock(state, block_index));
    BOOST_REQUIRE(index.BlockUntilSyncedToCurrentChain());

    const auto result{index.FindSpender(spent.outpoint)};
    BOOST_REQUIRE(result.has_value());
    BOOST_CHECK(!result->has_value());

    index.Stop();
}

BOOST_AUTO_TEST_SUITE_END()
