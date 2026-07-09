#pragma once

#include "ldbc_datagen_config.hpp"
#include "ldbc_java_random.hpp"
#include "ldbc_person_dictionaries.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <array>

namespace duckdb {

enum class LdbcRandomAspect : uint8_t {
	DATE = 0,
	BIRTH_DAY = 1,
	GENDER = 18,
	RANDOM = 19,
	NUM_TAG = 6,
	EXTRA_INFO = 25,
	IP = 33,
	BROWSER = 36,
	CITY = 38,
	COUNTRY = 39,
	TAG = 40,
	UNIVERSITY = 41,
	EMAIL = 46,
	TOP_EMAIL = 47,
	COMPANY = 48,
	LANGUAGE = 52,
	NAME = 55,
	SURNAME = 56,
	TAG_OTHER_COUNTRY = 57,
	TOPIC = 64,
	DELETION_PERSON = 65,
	NUM_ASPECT = 72
};

class LdbcRandomGeneratorFarm {
public:
	LdbcRandomGeneratorFarm();

	void Reset(int64_t seed);
	LdbcJavaRandom &Get(LdbcRandomAspect aspect);

private:
	static constexpr idx_t ASPECT_COUNT = static_cast<idx_t>(LdbcRandomAspect::NUM_ASPECT);
	std::array<LdbcJavaRandom, ASPECT_COUNT> generators;
};

class LdbcDateGenerator {
public:
	static constexpr int64_t ONE_DAY_MS = 24LL * 60LL * 60LL * 1000LL;
	static constexpr int64_t ONE_YEAR_MS = 365LL * ONE_DAY_MS;
	static constexpr int64_t TEN_YEARS_MS = 10LL * ONE_YEAR_MS;

	explicit LdbcDateGenerator(const LdbcDatagenConfig &config);

	int64_t RandomPersonCreationDate(LdbcJavaRandom &random) const;
	int64_t RandomBirthday(LdbcJavaRandom &random) const;
	int64_t SimulationStart() const;
	int64_t SimulationEnd() const;
	int64_t NetworkCollapse() const;

private:
	int64_t simulation_start;
	int64_t simulation_end;
	int64_t from_birthday;
	int64_t to_birthday;
};

struct LdbcPersonCore {
	int64_t sequential_id;
	int64_t block_id;
	int64_t block_offset;
	int64_t creation_date;
	int64_t account_id;
	int64_t birthday;
	int8_t gender;
	int32_t country_id;
	int32_t city_id;
	int32_t browser_id;
	string browser_name;
	string ip_address;
	string languages;
	string emails;
	string first_name;
	string last_name;
	bool message_deleter;
	int64_t random_id;
};

class LdbcPersonGenerator {
public:
	explicit LdbcPersonGenerator(const LdbcDatagenConfig &config);

	LdbcPersonCore GenerateCore(int64_t sequential_id);
	int64_t ComposePersonId(int64_t sequential_id, int64_t creation_date) const;

private:
	void ResetBlock(int64_t block_id);

	const LdbcDatagenConfig &config;
	LdbcDateGenerator dates;
	LdbcPersonDictionaries dictionaries;
	LdbcRandomGeneratorFarm random_farm;
	int64_t current_block = -1;
};

timestamp_t LdbcTimestampMs(int64_t epoch_ms);
date_t LdbcDateFromEpochMs(int64_t epoch_ms);
int64_t LdbcEpochMsFromDate(int32_t year, int32_t month, int32_t day);

} // namespace duckdb
