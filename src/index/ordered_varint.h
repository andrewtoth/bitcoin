// Copyright (c) The Bitcoin Core developers
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

#ifndef BITCOIN_INDEX_ORDERED_VARINT_H
#define BITCOIN_INDEX_ORDERED_VARINT_H

#include <serialize.h>

#include <cstdint>
#include <ios>
#include <limits>
#include <type_traits>

/**
 * Unsigned integer encoding whose lexicographic byte order matches numeric order.
 *
 * Length is stored in the leading bits of the first byte (UTF-8 style), so
 * every n-byte encoding sorts before every (n+1)-byte encoding. Payload bits
 * are big-endian. Size cutovers match Bitcoin Core VARINT:
 *
 *   1 byte: 0 .. 127
 *   2 byte: 128 .. 16,511
 *   3 byte: 16,512 .. 2,113,663
 *   4 byte: 2,113,664 .. 270,549,119
 *   5 byte: 270,549,120 .. 34,630,287,487
 *
 * Unlike VARINT, this can be used as a LevelDB key field for range seeks.
 */
struct OrderedVarIntFormatter {
    static constexpr uint64_t MAX_1{0x7F};
    static constexpr uint64_t MAX_2{0x407F};
    static constexpr uint64_t MAX_3{0x20407F};
    static constexpr uint64_t MAX_4{0x1020407F};
    static constexpr uint64_t MAX_5{MAX_4 + (uint64_t{1} << 35)};

    template <typename Stream>
    static void Write(Stream& s, uint64_t n)
    {
        if (n <= MAX_1) {
            ser_writedata8(s, n);
            return;
        }
        if (n <= MAX_2) {
            const uint64_t v{n - (MAX_1 + 1)};
            ser_writedata8(s, 0x80 | (v >> 8));
            ser_writedata8(s, v);
            return;
        }
        if (n <= MAX_3) {
            const uint64_t v{n - (MAX_2 + 1)};
            ser_writedata8(s, 0xC0 | (v >> 16));
            ser_writedata8(s, v >> 8);
            ser_writedata8(s, v);
            return;
        }
        if (n <= MAX_4) {
            const uint64_t v{n - (MAX_3 + 1)};
            ser_writedata8(s, 0xE0 | (v >> 24));
            ser_writedata8(s, v >> 16);
            ser_writedata8(s, v >> 8);
            ser_writedata8(s, v);
            return;
        }
        if (n > MAX_5) {
            throw std::ios_base::failure("OrderedVarInt value too large");
        }
        const uint64_t v{n - (MAX_4 + 1)};
        ser_writedata8(s, 0xF0 | (v >> 32));
        ser_writedata8(s, v >> 24);
        ser_writedata8(s, v >> 16);
        ser_writedata8(s, v >> 8);
        ser_writedata8(s, v);
    }

    template <typename Stream>
    static uint64_t Read(Stream& s)
    {
        const uint8_t b0{ser_readdata8(s)};
        if (b0 <= 0x7F) return b0;
        if ((b0 & 0xC0) == 0x80) {
            const uint8_t b1{ser_readdata8(s)};
            return (MAX_1 + 1) + ((uint64_t{b0} & 0x3F) << 8 | b1);
        }
        if ((b0 & 0xE0) == 0xC0) {
            const uint8_t b1{ser_readdata8(s)};
            const uint8_t b2{ser_readdata8(s)};
            return (MAX_2 + 1) + ((uint64_t{b0} & 0x1F) << 16 | (uint64_t{b1} << 8) | b2);
        }
        if ((b0 & 0xF0) == 0xE0) {
            const uint8_t b1{ser_readdata8(s)};
            const uint8_t b2{ser_readdata8(s)};
            const uint8_t b3{ser_readdata8(s)};
            return (MAX_3 + 1) + ((uint64_t{b0} & 0x0F) << 24 | (uint64_t{b1} << 16) | (uint64_t{b2} << 8) | b3);
        }
        if ((b0 & 0xF8) == 0xF0) {
            const uint8_t b1{ser_readdata8(s)};
            const uint8_t b2{ser_readdata8(s)};
            const uint8_t b3{ser_readdata8(s)};
            const uint8_t b4{ser_readdata8(s)};
            return (MAX_4 + 1) + ((uint64_t{b0} & 0x07) << 32 | (uint64_t{b1} << 24) | (uint64_t{b2} << 16) | (uint64_t{b3} << 8) | b4);
        }
        throw std::ios_base::failure("Invalid OrderedVarInt encoding");
    }

    template <typename Stream, typename I>
    void Ser(Stream& s, I v)
    {
        static_assert(std::is_unsigned_v<I>, "OrderedVarInt requires an unsigned type");
        Write(s, v);
    }

    template <typename Stream, typename I>
    void Unser(Stream& s, I& v)
    {
        static_assert(std::is_unsigned_v<I>, "OrderedVarInt requires an unsigned type");
        const uint64_t n{Read(s)};
        if (n > std::numeric_limits<I>::max()) {
            throw std::ios_base::failure("OrderedVarInt exceeds limit of type");
        }
        v = static_cast<I>(n);
    }
};

#endif // BITCOIN_INDEX_ORDERED_VARINT_H
