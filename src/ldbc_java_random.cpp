#include "ldbc_java_random.hpp"

#include "duckdb/common/exception.hpp"

#include <cstring>
#include <limits>

namespace duckdb {

LdbcJavaRandom::LdbcJavaRandom(int64_t seed) {
	SetSeed(seed);
}

void LdbcJavaRandom::SetSeed(int64_t seed_p) {
	seed = (static_cast<uint64_t>(seed_p) ^ MULTIPLIER) & MASK;
}

uint64_t LdbcJavaRandom::GetRawSeed() const {
	return seed;
}

void LdbcJavaRandom::SetRawSeed(uint64_t raw_seed) {
	seed = raw_seed & MASK;
}

int32_t LdbcJavaRandom::Next(int bits) {
	if (bits <= 0 || bits > 32) {
		throw InternalException("LdbcJavaRandom::Next bits must be in [1, 32]");
	}
	seed = (seed * MULTIPLIER + ADDEND) & MASK;
	auto value = static_cast<uint32_t>(seed >> (48U - static_cast<uint32_t>(bits)));
	int32_t result;
	std::memcpy(&result, &value, sizeof(result));
	return result;
}

int32_t LdbcJavaRandom::NextInt() {
	return Next(32);
}

int32_t LdbcJavaRandom::NextInt(int32_t bound) {
	if (bound <= 0) {
		throw InvalidInputException("Java Random nextInt bound must be positive");
	}
	if ((bound & -bound) == bound) {
		return static_cast<int32_t>((static_cast<int64_t>(bound) * static_cast<int64_t>(Next(31))) >> 31U);
	}

	while (true) {
		auto bits = Next(31);
		auto value = bits % bound;
		auto java_sum = static_cast<int64_t>(bits) - value + (bound - 1);
		if (java_sum <= std::numeric_limits<int32_t>::max()) {
			return value;
		}
	}
}

int64_t LdbcJavaRandom::NextLong() {
	auto high = static_cast<uint64_t>(static_cast<uint32_t>(Next(32)));
	auto low = static_cast<int64_t>(Next(32));
	auto bits = (high << 32U) + static_cast<uint64_t>(low);
	int64_t result;
	std::memcpy(&result, &bits, sizeof(result));
	return result;
}

float LdbcJavaRandom::NextFloat() {
	return static_cast<float>(Next(24)) / static_cast<float>(1U << 24U);
}

double LdbcJavaRandom::NextDouble() {
	auto high = static_cast<int64_t>(Next(26));
	auto low = static_cast<int64_t>(Next(27));
	return static_cast<double>((high << 27U) + low) / static_cast<double>(1ULL << 53U);
}

} // namespace duckdb
