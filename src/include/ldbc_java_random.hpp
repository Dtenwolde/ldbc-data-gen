#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class LdbcJavaRandom {
public:
	static constexpr uint64_t MULTIPLIER = 0x5DEECE66DULL;
	static constexpr uint64_t ADDEND = 0xBULL;
	static constexpr uint64_t MASK = (1ULL << 48U) - 1U;

	explicit LdbcJavaRandom(int64_t seed);

	void SetSeed(int64_t seed);
	uint64_t GetRawSeed() const;
	void SetRawSeed(uint64_t raw_seed);
	int32_t Next(int bits);
	int32_t NextInt();
	int32_t NextInt(int32_t bound);
	int64_t NextLong();
	float NextFloat();
	double NextDouble();

private:
	uint64_t seed;
};

} // namespace duckdb
