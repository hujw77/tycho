// SPDX-License-Identifier: MIT
pragma solidity >=0.8.24;

interface IUniswapV3PoolSnapshotLite {
    function tickSpacing() external view returns (int24);

    function tickBitmap(int16 wordPos) external view returns (uint256);

    function ticks(int24 tick)
        external
        view
        returns (
            uint128 liquidityGross,
            int128 liquidityNet,
            uint256 feeGrowthOutside0X128,
            uint256 feeGrowthOutside1X128,
            int56 tickCumulativeOutside,
            uint160 secondsPerLiquidityOutsideX128,
            uint32 secondsOutside,
            bool initialized
        );
}

library BitMathSnapshot {
    function leastSignificantBit(uint256 x) internal pure returns (uint8 r) {
        require(x > 0, "x must be non-zero");
        unchecked {
            uint256 isolated = x & (~x + 1);

            if (isolated >= 2 ** 128) {
                isolated >>= 128;
                r += 128;
            }
            if (isolated >= 2 ** 64) {
                isolated >>= 64;
                r += 64;
            }
            if (isolated >= 2 ** 32) {
                isolated >>= 32;
                r += 32;
            }
            if (isolated >= 2 ** 16) {
                isolated >>= 16;
                r += 16;
            }
            if (isolated >= 2 ** 8) {
                isolated >>= 8;
                r += 8;
            }
            if (isolated >= 2 ** 4) {
                isolated >>= 4;
                r += 4;
            }
            if (isolated >= 2 ** 2) {
                isolated >>= 2;
                r += 2;
            }
            if (isolated >= 2 ** 1) {
                r += 1;
            }
        }
    }
}

contract UniswapV3TickSnapshotLens {
    int24 internal constant MIN_TICK = -887272;
    int24 internal constant MAX_TICK = 887272;
    uint256 internal constant PACKED_TICK_SIZE = 19;

    /// @notice Returns the legal bitmap word range for the pool.
    function wordBounds(
        address pool
    ) external view returns (int24 tickSpacing, int16 minWord, int16 maxWord) {
        tickSpacing = IUniswapV3PoolSnapshotLite(pool).tickSpacing();
        require(tickSpacing > 0, "invalid tick spacing");
        (minWord, maxWord) = _wordBounds(tickSpacing);
    }

    /// @notice Scans a page of bitmap words and returns only the non-empty ones.
    function scanWordsPage(
        address pool,
        int16 startWord,
        uint16 maxWordsToScan,
        uint16 maxNonEmptyWords
    )
        external
        view
        returns (
            int24 tickSpacing,
            int16[] memory nonEmptyWords,
            int16 nextWord,
            bool done
        )
    {
        require(maxWordsToScan > 0, "maxWordsToScan=0");
        require(maxNonEmptyWords > 0, "maxNonEmptyWords=0");

        tickSpacing = IUniswapV3PoolSnapshotLite(pool).tickSpacing();
        require(tickSpacing > 0, "invalid tick spacing");

        (int16 minWord, int16 maxWord) = _wordBounds(tickSpacing);
        if (startWord > maxWord) {
            return (tickSpacing, new int16[](0), maxWord, true);
        }

        int16 cursor = startWord < minWord ? minWord : startWord;
        int16[] memory tmp = new int16[](maxNonEmptyWords);
        uint256 found;
        uint256 scanned;

        while (
            cursor <= maxWord &&
            scanned < maxWordsToScan &&
            found < maxNonEmptyWords
        ) {
            uint256 bitmap = IUniswapV3PoolSnapshotLite(pool).tickBitmap(cursor);
            if (bitmap != 0) {
                tmp[found] = cursor;
                found++;
            }

            unchecked {
                cursor++;
                scanned++;
            }
        }

        nonEmptyWords = new int16[](found);
        for (uint256 i; i < found; i++) {
            nonEmptyWords[i] = tmp[i];
        }

        done = cursor > maxWord;
        nextWord = done ? maxWord : cursor;
    }

    /// @notice Returns packed tick records for the provided bitmap words.
    /// Each record is encoded as abi.encodePacked(int24 tick, int128 liquidityNet).
    /// `counts[i]` tells the caller how many records belong to `words[i]`.
    function getTicksForWords(
        address pool,
        int16[] calldata words
    ) external view returns (bytes memory packedTicks, uint32[] memory counts) {
        int24 tickSpacing = IUniswapV3PoolSnapshotLite(pool).tickSpacing();
        require(tickSpacing > 0, "invalid tick spacing");

        counts = new uint32[](words.length);
        uint256 totalTicks;

        for (uint256 i; i < words.length; i++) {
            uint32 count = uint32(_countSetBits(IUniswapV3PoolSnapshotLite(pool).tickBitmap(words[i])));
            counts[i] = count;
            totalTicks += count;
        }

        packedTicks = new bytes(totalTicks * PACKED_TICK_SIZE);
        uint256 offset;

        for (uint256 i; i < words.length; i++) {
            uint256 bitmap = IUniswapV3PoolSnapshotLite(pool).tickBitmap(words[i]);

            while (bitmap != 0) {
                uint8 bitPos = BitMathSnapshot.leastSignificantBit(bitmap);
                int24 tick = ((int24(words[i]) << 8) + int24(uint24(bitPos))) * tickSpacing;
                (, int128 liquidityNet, , , , , , ) = IUniswapV3PoolSnapshotLite(pool).ticks(tick);

                bytes memory encodedTick = abi.encodePacked(tick, liquidityNet);
                for (uint256 j; j < PACKED_TICK_SIZE; j++) {
                    packedTicks[offset + j] = encodedTick[j];
                }
                offset += PACKED_TICK_SIZE;

                unchecked {
                    bitmap &= (bitmap - 1);
                }
            }
        }
    }

    function _wordBounds(
        int24 tickSpacing
    ) internal pure returns (int16 minWord, int16 maxWord) {
        int24 compressedMin = MIN_TICK / tickSpacing;
        if (MIN_TICK < 0 && MIN_TICK % tickSpacing != 0) {
            compressedMin--;
        }

        int24 compressedMax = MAX_TICK / tickSpacing;
        if (MAX_TICK < 0 && MAX_TICK % tickSpacing != 0) {
            compressedMax--;
        }

        minWord = int16(compressedMin >> 8);
        maxWord = int16(compressedMax >> 8);
    }

    function _countSetBits(uint256 bitmap) internal pure returns (uint256 count) {
        while (bitmap != 0) {
            unchecked {
                bitmap &= (bitmap - 1);
                count++;
            }
        }
    }
}
