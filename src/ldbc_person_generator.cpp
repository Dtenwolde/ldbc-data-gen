#include "ldbc_person_generator.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"

namespace duckdb {

namespace {

static idx_t AspectIndex(LdbcRandomAspect aspect) {
	return static_cast<idx_t>(aspect);
}

} // namespace

LdbcRandomGeneratorFarm::LdbcRandomGeneratorFarm()
    : generators({LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),
                  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),
                  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),
                  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),
                  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),
                  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),
                  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),
                  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),
                  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),
                  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),
                  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),
                  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),
                  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),
                  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),  LdbcJavaRandom(0),
                  LdbcJavaRandom(0),  LdbcJavaRandom(0)}) {
}

void LdbcRandomGeneratorFarm::Reset(int64_t seed) {
	LdbcJavaRandom seed_random(53223436LL + 1234567LL * seed);
	for (auto &generator : generators) {
		generator.SetSeed(seed_random.NextLong());
	}
}

LdbcJavaRandom &LdbcRandomGeneratorFarm::Get(LdbcRandomAspect aspect) {
	return generators[AspectIndex(aspect)];
}

LdbcDateGenerator::LdbcDateGenerator(const LdbcDatagenConfig &config)
    : simulation_start(LdbcEpochMsFromDate(config.start_year, 1, 1)),
      simulation_end(LdbcEpochMsFromDate(config.start_year + config.num_years, 1, 1)),
      from_birthday(LdbcEpochMsFromDate(1980, 1, 1)), to_birthday(LdbcEpochMsFromDate(1990, 1, 1)) {
}

int64_t LdbcDateGenerator::RandomPersonCreationDate(LdbcJavaRandom &random) const {
	return static_cast<int64_t>(static_cast<double>(simulation_start) +
	                            random.NextDouble() * static_cast<double>(simulation_end - simulation_start));
}

int64_t LdbcDateGenerator::RandomBirthday(LdbcJavaRandom &random) const {
	auto epoch_ms = static_cast<int64_t>(random.NextDouble() * static_cast<double>(to_birthday - from_birthday)) +
	                from_birthday;
	return Date::EpochMilliseconds(LdbcDateFromEpochMs(epoch_ms));
}

int64_t LdbcDateGenerator::SimulationStart() const {
	return simulation_start;
}

int64_t LdbcDateGenerator::SimulationEnd() const {
	return simulation_end;
}

int64_t LdbcDateGenerator::NetworkCollapse() const {
	return simulation_start + TEN_YEARS_MS;
}

LdbcPersonGenerator::LdbcPersonGenerator(const LdbcDatagenConfig &config)
    : config(config), dates(config), dictionaries(config.resource_dir, config.prob_english, config.prob_second_lang) {
}

void LdbcPersonGenerator::ResetBlock(int64_t block_id) {
	if (block_id > NumericLimits<int32_t>::Maximum()) {
		throw InvalidInputException("LDBC person block id exceeds Java int range");
	}
	random_farm.Reset(block_id);
	current_block = block_id;
}

int64_t LdbcPersonGenerator::ComposePersonId(int64_t sequential_id, int64_t creation_date) const {
	const uint64_t id_mask = ~(0xFFFFFFFFFFFFFFFFULL << 41U);
	auto bucket = static_cast<int64_t>(256.0 * static_cast<double>(creation_date - dates.SimulationStart()) /
	                                   static_cast<double>(dates.SimulationEnd()));
	return (bucket << 41U) | static_cast<int64_t>(static_cast<uint64_t>(sequential_id) & id_mask);
}

LdbcPersonCore LdbcPersonGenerator::GenerateCore(int64_t sequential_id) {
	if (sequential_id < 0 || sequential_id >= config.num_persons) {
		throw InvalidInputException("LDBC person sequential id is outside configured population");
	}
	auto block_id = sequential_id / config.block_size;
	if (block_id != current_block) {
		ResetBlock(block_id);
	}

	auto creation_date = dates.RandomPersonCreationDate(random_farm.Get(LdbcRandomAspect::DATE));
	auto country_id = dictionaries.places.GetCountryForPerson(random_farm.Get(LdbcRandomAspect::COUNTRY));
	auto gender = random_farm.Get(LdbcRandomAspect::GENDER).NextDouble() > 0.5 ? int8_t(1) : int8_t(0);
	auto birthday = dates.RandomBirthday(random_farm.Get(LdbcRandomAspect::BIRTH_DAY));
	auto browser_id = dictionaries.browsers.GetRandomBrowserId(random_farm.Get(LdbcRandomAspect::BROWSER));
	auto city_id = dictionaries.places.GetRandomCity(random_farm.Get(LdbcRandomAspect::CITY), country_id);
	auto ip_address = dictionaries.ips.GetIP(random_farm.Get(LdbcRandomAspect::IP), country_id);

	// The full Person generator consumes additional dictionary-backed RNG streams here. Until those dictionaries are
	// ported, this core generator only exposes fields that are already backed by local dictionary implementations.
	auto message_deleter = random_farm.Get(LdbcRandomAspect::RANDOM).NextDouble() > 0.5;
	auto account_id = ComposePersonId(sequential_id, creation_date);
	auto random_id = random_farm.Get(LdbcRandomAspect::RANDOM).NextInt(NumericLimits<int32_t>::Maximum()) % 100;
	auto languages = dictionaries.languages.GetLanguageList(random_farm.Get(LdbcRandomAspect::LANGUAGE), country_id);
	auto birth_year = Date::ExtractYear(LdbcDateFromEpochMs(birthday));
	auto first_name =
	    dictionaries.names.GetRandomGivenName(random_farm.Get(LdbcRandomAspect::NAME), country_id, gender == 1, birth_year);
	auto last_name = dictionaries.names.GetRandomSurname(random_farm.Get(LdbcRandomAspect::SURNAME), country_id);

	LdbcPersonCore result;
	result.sequential_id = sequential_id;
	result.block_id = block_id;
	result.block_offset = sequential_id - block_id * config.block_size;
	result.creation_date = creation_date;
	result.account_id = account_id;
	result.birthday = birthday;
	result.gender = gender;
	result.country_id = country_id;
	result.city_id = city_id;
	result.browser_id = browser_id;
	result.browser_name = dictionaries.browsers.GetName(browser_id);
	result.ip_address = ip_address;
	result.languages = languages;
	result.first_name = first_name;
	result.last_name = last_name;
	result.message_deleter = message_deleter;
	result.random_id = random_id;
	return result;
}

timestamp_t LdbcTimestampMs(int64_t epoch_ms) {
	return Timestamp::FromEpochMs(epoch_ms);
}

date_t LdbcDateFromEpochMs(int64_t epoch_ms) {
	return Timestamp::GetDate(Timestamp::FromEpochMs(epoch_ms));
}

int64_t LdbcEpochMsFromDate(int32_t year, int32_t month, int32_t day) {
	return Date::EpochMilliseconds(Date::FromDate(year, month, day));
}

} // namespace duckdb
