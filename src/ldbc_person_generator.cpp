#include "ldbc_person_generator.hpp"

#include "ldbc_unicode.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <map>
#include <set>
#include <sstream>

namespace duckdb {

namespace {

static idx_t AspectIndex(LdbcRandomAspect aspect) {
	return static_cast<idx_t>(aspect);
}

static string ClampStringLocal(const string &value, idx_t max_length) {
	if (value.size() <= max_length) {
		return value;
	}
	return value.substr(0, max_length);
}

static int64_t RandomDateUnchecked(LdbcJavaRandom &random, int64_t min_date, int64_t max_date) {
	return static_cast<int64_t>(random.NextDouble() * static_cast<double>(max_date - min_date) + min_date);
}

} // namespace

LdbcRandomGeneratorFarm::LdbcRandomGeneratorFarm()
    : generators({LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0),
                  LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0),
                  LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0),
                  LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0),
                  LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0),
                  LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0),
                  LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0),
                  LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0),
                  LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0),
                  LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0),
                  LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0),
                  LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0),
                  LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0),
                  LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0), LdbcJavaRandom(0),
                  LdbcJavaRandom(0), LdbcJavaRandom(0)}) {
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
	auto epoch_ms =
	    static_cast<int64_t>(random.NextDouble() * static_cast<double>(to_birthday - from_birthday)) + from_birthday;
	return Date::EpochMilliseconds(LdbcDateFromEpochMs(epoch_ms));
}

int64_t LdbcDateGenerator::RandomDate(LdbcJavaRandom &random, int64_t min_date, int64_t max_date) const {
	if (min_date >= max_date) {
		throw InternalException("LDBC random date bounds are invalid");
	}
	return static_cast<int64_t>(random.NextDouble() * static_cast<double>(max_date - min_date) + min_date);
}

int64_t LdbcDateGenerator::RandomPersonDeletionDate(LdbcJavaRandom &random, int64_t creation_date,
                                                    int64_t max_deletion_date) const {
	return RandomDate(random, creation_date + 10000, max_deletion_date);
}

int64_t LdbcDateGenerator::RandomKnowsCreationDate(LdbcJavaRandom &random, int64_t person_a_creation,
                                                   int64_t person_a_deletion, int64_t person_b_creation,
                                                   int64_t person_b_deletion) const {
	auto from_date = std::max(person_a_creation, person_b_creation) + 10000;
	auto to_date = std::min(std::min(person_a_deletion, person_b_deletion), simulation_end);
	return RandomDate(random, from_date, to_date);
}

int64_t LdbcDateGenerator::RandomKnowsDeletionDate(LdbcJavaRandom &random, int64_t person_a_deletion,
                                                   int64_t person_b_deletion, int64_t knows_creation_date) const {
	auto from_date = knows_creation_date + 10000;
	auto to_date = std::min(std::min(person_a_deletion, person_b_deletion), simulation_end);
	return RandomDate(random, from_date, to_date);
}

int64_t LdbcDateGenerator::RandomClassYear(LdbcJavaRandom &random, int64_t birthday) const {
	auto graduate_age = static_cast<int64_t>(random.NextInt(5) + 18) * ONE_YEAR_MS;
	auto class_year = birthday + graduate_age;
	if (class_year > simulation_end) {
		return -1;
	}
	return class_year;
}

int64_t LdbcDateGenerator::GetWorkFromYear(LdbcJavaRandom &random, int64_t class_year, int64_t birthday) const {
	if (class_year == -1) {
		auto working_age = 18LL * ONE_YEAR_MS;
		auto from = birthday + working_age;
		return std::min(static_cast<int64_t>(random.NextDouble() * static_cast<double>(simulation_end - from)) + from,
		                simulation_end);
	}
	return class_year + static_cast<int64_t>(random.NextDouble() * static_cast<double>(2LL * ONE_YEAR_MS));
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

LdbcFacebookDegreeDistribution::LdbcFacebookDegreeDistribution(const LdbcDatagenConfig &config) : random_percentile(0) {
	if (config.degree_distribution != "Facebook") {
		throw InvalidInputException("Only the Facebook LDBC degree distribution is currently supported");
	}
	auto path = LdbcResourcePath(LdbcResourcePath(config.resource_dir, "dictionaries"), "facebookBucket100.dat");
	std::ifstream file(path);
	if (!file.is_open()) {
		throw IOException("Could not open LDBC degree distribution file '%s'", path);
	}

	auto mean = std::round(std::pow(static_cast<double>(config.num_persons),
	                                0.512 - 0.028 * std::log10(static_cast<double>(config.num_persons))));
	string line;
	while (std::getline(file, line)) {
		if (!line.empty() && line.back() == '\r') {
			line.pop_back();
		}
		if (line.empty()) {
			continue;
		}
		std::stringstream parser(line);
		double min_value;
		double max_value;
		parser >> min_value >> max_value;
		if (parser.fail()) {
			throw InvalidInputException("Malformed facebookBucket100.dat line: '%s'", line);
		}
		auto scaled_min = min_value * mean / 190.0;
		auto scaled_max = max_value * mean / 190.0;
		if (scaled_max < scaled_min) {
			scaled_max = scaled_min;
		}
		buckets.push_back({scaled_min, scaled_max});
		random_degree.emplace_back(0);
	}
}

void LdbcFacebookDegreeDistribution::Reset(int64_t seed) {
	LdbcJavaRandom seed_random(53223436LL + 1234567LL * seed);
	for (auto &random : random_degree) {
		random.SetSeed(seed_random.NextLong());
	}
	random_percentile.SetSeed(seed_random.NextLong());
}

int64_t LdbcFacebookDegreeDistribution::NextDegree() {
	auto bucket_idx = random_percentile.NextInt(NumericCast<int32_t>(buckets.size()));
	auto &bucket = buckets[bucket_idx];
	auto min_range = static_cast<int32_t>(bucket.min);
	auto max_range = static_cast<int32_t>(bucket.max);
	if (max_range < min_range) {
		max_range = min_range;
	}
	return random_degree[bucket_idx].NextInt(max_range - min_range + 1) + min_range;
}

LdbcPersonGenerator::LdbcPersonGenerator(const LdbcDatagenConfig &config)
    : config(config), dates(config),
      dictionaries(config.resource_dir, config.prob_english, config.prob_second_lang, config.tag_country_corr_prob,
                   config.prob_uncorrelated_company, config.prob_uncorrelated_organisation, config.prob_top_univ),
      degree_distribution(config) {
}

void LdbcPersonGenerator::ResetBlock(int64_t block_id) {
	if (block_id > NumericLimits<int32_t>::Maximum()) {
		throw InvalidInputException("LDBC person block id exceeds Java int range");
	}
	random_farm.Reset(block_id);
	degree_distribution.Reset(block_id);
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

	auto message_deleter = random_farm.Get(LdbcRandomAspect::RANDOM).NextDouble() > 0.5;
	auto max_num_knows = std::min<int64_t>(degree_distribution.NextDegree(), config.max_num_friends);
	auto explicitly_deleted =
	    dictionaries.person_deletes.IsDeleted(random_farm.Get(LdbcRandomAspect::DELETION_PERSON), max_num_knows);
	auto deletion_date = explicitly_deleted ? dates.RandomPersonDeletionDate(random_farm.Get(LdbcRandomAspect::DATE),
	                                                                         creation_date, dates.SimulationEnd())
	                                        : dates.NetworkCollapse();
	auto account_id = ComposePersonId(sequential_id, creation_date);
	auto main_interest = dictionaries.tags.GetTagByCountry(random_farm.Get(LdbcRandomAspect::TAG_OTHER_COUNTRY),
	                                                       random_farm.Get(LdbcRandomAspect::TAG), country_id);
	auto tag_count = static_cast<int32_t>(
	    static_cast<double>(config.min_num_tags_per_person) +
	    static_cast<double>(config.max_num_tags_per_person + 1 - config.min_num_tags_per_person) *
	        std::pow(random_farm.Get(LdbcRandomAspect::NUM_TAG).NextDouble(), 1.0 / LdbcDatagenConfig::ALPHA));
	auto interests = dictionaries.tag_matrix.GetSetOfTags(random_farm.Get(LdbcRandomAspect::TOPIC),
	                                                      random_farm.Get(LdbcRandomAspect::TAG_OTHER_COUNTRY),
	                                                      main_interest, tag_count);
	auto university_location_id = dictionaries.universities.GetRandomUniversityLocation(
	    random_farm.Get(LdbcRandomAspect::UNCORRELATED_UNIVERSITY),
	    random_farm.Get(LdbcRandomAspect::UNCORRELATED_UNIVERSITY_LOCATION),
	    random_farm.Get(LdbcRandomAspect::TOP_UNIVERSITY), random_farm.Get(LdbcRandomAspect::UNIVERSITY), country_id);
	auto random_id = random_farm.Get(LdbcRandomAspect::RANDOM).NextInt(NumericLimits<int32_t>::Maximum()) % 100;
	auto birth_year = Date::ExtractYear(LdbcDateFromEpochMs(birthday));
	auto first_name = dictionaries.names.GetRandomGivenName(random_farm.Get(LdbcRandomAspect::NAME), country_id,
	                                                        gender == 1, birth_year);
	auto last_name = dictionaries.names.GetRandomSurname(random_farm.Get(LdbcRandomAspect::SURNAME), country_id);
	auto email_base = LdbcEmailBaseFromFirstName(first_name);
	auto email_count = random_farm.Get(LdbcRandomAspect::EXTRA_INFO).NextInt(config.max_emails) + 1;
	vector<string> emails;
	while (NumericCast<int32_t>(emails.size()) < email_count) {
		auto email = email_base + std::to_string(account_id) + "@" +
		             dictionaries.emails.GetRandomEmail(random_farm.Get(LdbcRandomAspect::TOP_EMAIL),
		                                                random_farm.Get(LdbcRandomAspect::EMAIL));
		if (std::find(emails.begin(), emails.end(), email) == emails.end()) {
			emails.push_back(std::move(email));
		}
	}
	string email_list;
	for (idx_t email_idx = 0; email_idx < emails.size(); email_idx++) {
		if (email_idx > 0) {
			email_list += ";";
		}
		email_list += emails[email_idx];
	}
	auto class_probability = random_farm.Get(LdbcRandomAspect::EXTRA_INFO).NextDouble();
	auto class_year = class_probability < config.missing_ratio || university_location_id == -1
	                      ? int64_t(-1)
	                      : dates.RandomClassYear(random_farm.Get(LdbcRandomAspect::DATE), birthday);
	auto university_id = university_location_id == -1
	                         ? int64_t(-1)
	                         : dictionaries.universities.GetUniversityFromLocation(university_location_id);
	auto company_count = random_farm.Get(LdbcRandomAspect::EXTRA_INFO).NextInt(config.max_companies) + 1;
	auto company_probability = random_farm.Get(LdbcRandomAspect::EXTRA_INFO).NextDouble();
	unordered_map<int64_t, int64_t> companies;
	if (company_probability >= config.missing_ratio) {
		for (int32_t company_idx = 0; company_idx < company_count; company_idx++) {
			auto work_from = dates.GetWorkFromYear(random_farm.Get(LdbcRandomAspect::DATE), class_year, birthday);
			auto company = dictionaries.companies.GetRandomCompany(
			    random_farm.Get(LdbcRandomAspect::UNCORRELATED_COMPANY),
			    random_farm.Get(LdbcRandomAspect::UNCORRELATED_COMPANY_LOCATION),
			    random_farm.Get(LdbcRandomAspect::COMPANY), country_id);
			companies[company] = work_from;
		}
	}
	auto &language_random = random_farm.Get(LdbcRandomAspect::LANGUAGE);
	auto language_ids = dictionaries.languages.GetLanguages(language_random, country_id);
	auto international_language = dictionaries.languages.GetInternationalLanguage(language_random);
	if (international_language != -1 &&
	    std::find(language_ids.begin(), language_ids.end(), international_language) == language_ids.end()) {
		language_ids.push_back(international_language);
	}
	string languages;
	for (idx_t language_idx = 0; language_idx < language_ids.size(); language_idx++) {
		if (language_idx > 0) {
			languages += ";";
		}
		languages += dictionaries.languages.GetLanguageName(language_ids[language_idx]);
	}

	LdbcPersonCore result;
	result.sequential_id = sequential_id;
	result.block_id = block_id;
	result.block_offset = sequential_id - block_id * config.block_size;
	result.creation_date = creation_date;
	result.account_id = account_id;
	result.birthday = birthday;
	result.deletion_date = deletion_date;
	result.max_num_knows = max_num_knows;
	result.main_interest = main_interest;
	result.gender = gender;
	result.country_id = country_id;
	result.city_id = city_id;
	result.browser_id = browser_id;
	result.browser_name = dictionaries.browsers.GetName(browser_id);
	result.ip_address = ip_address;
	result.languages = languages;
	result.emails = email_list;
	result.first_name = first_name;
	result.last_name = last_name;
	result.message_deleter = message_deleter;
	result.explicitly_deleted = explicitly_deleted;
	result.large_poster = Date::ExtractMonth(LdbcDateFromEpochMs(birthday)) == 1;
	result.random_id = random_id;
	result.language_ids = std::move(language_ids);
	result.interests = std::move(interests);
	result.university_location_id = university_location_id;
	result.university_id = university_id;
	result.class_year = class_year;
	result.companies = std::move(companies);
	return result;
}

vector<LdbcPersonCore> LdbcGeneratePersons(const LdbcDatagenConfig &config) {
	LdbcPersonGenerator generator(config);
	vector<LdbcPersonCore> persons;
	persons.reserve(NumericCast<idx_t>(config.num_persons));
	for (int64_t sequential_id = 0; sequential_id < config.num_persons; sequential_id++) {
		persons.push_back(generator.GenerateCore(sequential_id));
	}
	return persons;
}

namespace {

static int64_t KnowsTargetEdges(const LdbcPersonCore &person, const vector<float> &percentages, idx_t step_index) {
	int64_t generated_edges = 0;
	for (idx_t idx = 0; idx < step_index; idx++) {
		generated_edges += static_cast<int64_t>(std::ceil(percentages[idx] * static_cast<float>(person.max_num_knows)));
	}
	generated_edges = std::min(generated_edges, person.max_num_knows);
	auto step_edges =
	    static_cast<int64_t>(std::ceil(percentages[step_index] * static_cast<float>(person.max_num_knows)));
	return std::min(person.max_num_knows - generated_edges, step_edges);
}

static float GeoDistanceSimilarity(const LdbcPersonCore &person_a, const LdbcPersonCore &person_b,
                                   const LdbcPersonDictionaries &dictionaries) {
	auto zorder_a = dictionaries.places.GetZOrderId(person_a.country_id);
	auto zorder_b = dictionaries.places.GetZOrderId(person_b.country_id);
	return 1.0f - (static_cast<float>(std::abs(zorder_a - zorder_b)) / 256.0f);
}

static bool IsValidKnowsWindow(const LdbcPersonCore &person_a, const LdbcPersonCore &person_b, int32_t delta) {
	return person_a.creation_date + delta <= person_b.deletion_date &&
	       person_b.creation_date + delta <= person_a.deletion_date;
}

static LdbcKnowsEdge CreateKnowsEdge(LdbcDateGenerator &dates, LdbcRandomGeneratorFarm &random_farm,
                                     const LdbcPersonCore &person_a, const LdbcPersonCore &person_b,
                                     const LdbcPersonDictionaries &dictionaries) {
	auto creation_date =
	    dates.RandomKnowsCreationDate(random_farm.Get(LdbcRandomAspect::DATE), person_a.creation_date,
	                                  person_a.deletion_date, person_b.creation_date, person_b.deletion_date);
	auto similarity = GeoDistanceSimilarity(person_a, person_b, dictionaries);
	auto delete_probability = similarity < 0.9222521f ? 0.025 : 0.075;
	auto explicitly_deleted = random_farm.Get(LdbcRandomAspect::DELETION_KNOWS).NextDouble() < delete_probability;
	auto deletion_date = explicitly_deleted ? dates.RandomKnowsDeletionDate(random_farm.Get(LdbcRandomAspect::DATE),
	                                                                        person_a.deletion_date,
	                                                                        person_b.deletion_date, creation_date)
	                                        : std::min(person_a.deletion_date, person_b.deletion_date);

	LdbcKnowsEdge edge;
	edge.creation_date = creation_date;
	edge.deletion_date = deletion_date;
	edge.explicitly_deleted = explicitly_deleted;
	edge.weight = similarity;
	edge.person1_id = person_a.account_id;
	edge.person2_id = person_b.account_id;
	return edge;
}

template <class SORT>
static void GenerateKnowsStep(const LdbcDatagenConfig &config, const vector<LdbcPersonCore> &persons,
                              const LdbcPersonDictionaries &dictionaries, LdbcDateGenerator &dates,
                              const vector<float> &percentages, idx_t step_index, SORT sort_function,
                              vector<LdbcKnowsEdge> &edges) {
	vector<idx_t> order;
	order.reserve(persons.size());
	for (idx_t idx = 0; idx < persons.size(); idx++) {
		order.push_back(idx);
	}
	std::stable_sort(order.begin(), order.end(), sort_function);

	for (idx_t block_start = 0; block_start < order.size(); block_start += NumericCast<idx_t>(config.block_size)) {
		auto block_end = std::min<idx_t>(order.size(), block_start + NumericCast<idx_t>(config.block_size));
		auto block_id = block_start / NumericCast<idx_t>(config.block_size);
		LdbcRandomGeneratorFarm random_farm;
		random_farm.Reset(NumericCast<int64_t>(block_id));
		vector<int64_t> local_degrees(block_end - block_start, 0);

		for (idx_t local_i = 0; local_i < block_end - block_start; local_i++) {
			auto &person_a = persons[order[block_start + local_i]];
			auto target_a = KnowsTargetEdges(person_a, percentages, step_index);
			for (idx_t local_j = local_i + 1; local_degrees[local_i] < target_a && local_j < block_end - block_start;
			     local_j++) {
				auto &person_b = persons[order[block_start + local_j]];
				auto target_b = KnowsTargetEdges(person_b, percentages, step_index);
				if (local_degrees[local_j] >= target_b) {
					continue;
				}
				auto random_probability = random_farm.Get(LdbcRandomAspect::UNIFORM).NextDouble();
				auto probability = std::pow(config.base_prob_correlated, static_cast<double>(local_j - local_i));
				if (random_probability >= probability && random_probability >= config.limit_prob_correlated) {
					continue;
				}
				if (!IsValidKnowsWindow(person_a, person_b, config.delta)) {
					continue;
				}
				auto edge = CreateKnowsEdge(dates, random_farm, person_a, person_b, dictionaries);
				edges.push_back(edge);
				std::swap(edge.person1_id, edge.person2_id);
				edges.push_back(edge);
				local_degrees[local_i]++;
				local_degrees[local_j]++;
			}
		}
	}
}

} // namespace

vector<LdbcKnowsEdge> LdbcGenerateKnows(const LdbcDatagenConfig &config, const vector<LdbcPersonCore> &persons) {
	if (config.knows_generator != "Distance") {
		throw InvalidInputException("Only the Distance LDBC knows generator is currently supported");
	}
	if (config.person_similarity != "GeoDistance") {
		throw InvalidInputException("Only GeoDistance LDBC person similarity is currently supported");
	}

	LdbcPersonDictionaries dictionaries(config.resource_dir, config.prob_english, config.prob_second_lang,
	                                    config.tag_country_corr_prob, config.prob_uncorrelated_company,
	                                    config.prob_uncorrelated_organisation, config.prob_top_univ);
	LdbcDateGenerator dates(config);
	vector<float> percentages {0.45f, 0.45f, 0.1f};
	vector<LdbcKnowsEdge> generated_edges;

	GenerateKnowsStep(
	    config, persons, dictionaries, dates, percentages, 0,
	    [&](idx_t left, idx_t right) {
		    auto &left_person = persons[left];
		    auto &right_person = persons[right];
		    if (left_person.university_location_id != right_person.university_location_id) {
			    return left_person.university_location_id < right_person.university_location_id;
		    }
		    return left_person.account_id < right_person.account_id;
	    },
	    generated_edges);
	GenerateKnowsStep(
	    config, persons, dictionaries, dates, percentages, 1,
	    [&](idx_t left, idx_t right) {
		    auto &left_person = persons[left];
		    auto &right_person = persons[right];
		    if (left_person.main_interest != right_person.main_interest) {
			    return left_person.main_interest < right_person.main_interest;
		    }
		    return left_person.account_id < right_person.account_id;
	    },
	    generated_edges);
	GenerateKnowsStep(
	    config, persons, dictionaries, dates, percentages, 2,
	    [&](idx_t left, idx_t right) {
		    auto &left_person = persons[left];
		    auto &right_person = persons[right];
		    if (left_person.random_id != right_person.random_id) {
			    return left_person.random_id < right_person.random_id;
		    }
		    return left_person.account_id < right_person.account_id;
	    },
	    generated_edges);

	std::sort(generated_edges.begin(), generated_edges.end(),
	          [](const LdbcKnowsEdge &left, const LdbcKnowsEdge &right) {
		          if (left.person1_id != right.person1_id) {
			          return left.person1_id < right.person1_id;
		          }
		          if (left.person2_id != right.person2_id) {
			          return left.person2_id < right.person2_id;
		          }
		          return left.creation_date < right.creation_date;
	          });

	vector<LdbcKnowsEdge> merged_edges;
	int64_t last_from = NumericLimits<int64_t>::Minimum();
	int64_t last_to = NumericLimits<int64_t>::Minimum();
	for (auto &edge : generated_edges) {
		if (edge.person1_id == last_from && edge.person2_id == last_to) {
			continue;
		}
		last_from = edge.person1_id;
		last_to = edge.person2_id;
		if (edge.person1_id < edge.person2_id) {
			merged_edges.push_back(edge);
		}
	}
	return merged_edges;
}

namespace {

struct LdbcActivityFriend {
	const LdbcPersonCore *person;
	int64_t creation_date;
	int64_t deletion_date;
};

struct LdbcActivityMembership {
	const LdbcPersonCore *person;
	int64_t creation_date;
	int64_t deletion_date;
};

struct LdbcActivityMessage {
	const LdbcPersonCore *creator;
	int64_t id;
	int64_t root_post_id;
	int64_t creation_date;
	int64_t deletion_date;
	vector<int32_t> tags;
	bool long_content;
};

struct LdbcFlashmobTag {
	int64_t date;
	int32_t level;
	int32_t tag;
	double probability = 0.0;
};

static constexpr double LDBC_POST_DELETE_MAPPING[] = {0.18, 0.152, 0.142, 0.1,  0.1,  0.1,  0.1,
                                                      0.05, 0.05,  0.05,  0.01, 0.01, 0.01, 0.01,
                                                      0.01, 0.01,  0.01,  0.01, 0.01, 0.01, 0.01};
static constexpr idx_t LDBC_SHORT_COMMENT_COUNT = 16;
static constexpr const char *LDBC_SHORT_COMMENTS[] = {"ok",  "good",   "great",   "cool",  "thx",   "fine",
                                                      "LOL", "roflol", "no way!", "I see", "right", "yes",
                                                      "no",  "duh",    "thanks",  "maybe"};

static int64_t NumberOfMonths(const LdbcDateGenerator &dates, int64_t from_date) {
	return (dates.SimulationEnd() - from_date) / (30LL * LdbcDateGenerator::ONE_DAY_MS);
}

static int64_t FormActivityId(const LdbcDatagenConfig &config, const LdbcDateGenerator &dates, int64_t local_id,
                              int64_t creation_date, int64_t block_id) {
	const uint64_t id_mask = ~(0xFFFFFFFFFFFFFFFFULL << 36U);
	auto bucket = static_cast<int64_t>(256.0 * static_cast<double>(creation_date - dates.SimulationStart()) /
	                                   static_cast<double>(dates.SimulationEnd()));
	auto composed = (bucket << 36U) | static_cast<int64_t>(static_cast<uint64_t>(local_id) & id_mask);
	auto blocks = static_cast<int64_t>(
	    std::ceil(static_cast<double>(config.num_persons) / static_cast<double>(config.block_size)));
	auto num_bits = blocks <= 1 ? int64_t(0) : static_cast<int64_t>(std::ceil(std::log2(static_cast<double>(blocks))));
	if (num_bits > 20) {
		throw InvalidInputException("LDBC activity id block bits exceed Spark-compatible range");
	}
	auto lower_part = composed & 0x0FFFFFLL;
	auto machine_part = block_id << 20U;
	auto upper_part = (composed >> 20U) << (20U + NumericCast<uint32_t>(num_bits));
	return upper_part | machine_part | lower_part;
}

static int32_t RandomPersonLanguage(LdbcRandomGeneratorFarm &random_farm, const LdbcPersonCore &person) {
	if (person.language_ids.empty()) {
		return -1;
	}
	auto language_idx =
	    random_farm.Get(LdbcRandomAspect::LANGUAGE).NextInt(NumericCast<int32_t>(person.language_ids.size()));
	return person.language_ids[language_idx];
}

static int32_t RandomPersonInterest(LdbcRandomGeneratorFarm &random_farm, const LdbcPersonCore &person) {
	if (person.interests.empty()) {
		return -1;
	}
	auto interest_idx =
	    random_farm.Get(LdbcRandomAspect::FORUM_INTEREST).NextInt(NumericCast<int32_t>(person.interests.size()));
	return person.interests[interest_idx];
}

static string EscapedTagName(const LdbcPersonDictionaries &dictionaries, int32_t tag_id) {
	auto name = dictionaries.tags.GetName(tag_id);
	string result;
	for (auto c : name) {
		if (c == '"') {
			result += "\\\"";
		} else {
			result += c;
		}
	}
	return result;
}

static bool IsTravelSeason(int64_t epoch_ms) {
	auto date = LdbcDateFromEpochMs(epoch_ms);
	auto month = Date::ExtractMonth(date);
	auto day = Date::ExtractDay(date);
	return (month > 4 && month < 7) || (month == 11 && day > 23);
}

static bool ChangeUsualCountry(const LdbcDatagenConfig &config, LdbcJavaRandom &random, int64_t creation_date) {
	auto probability = random.NextDouble();
	if (IsTravelSeason(creation_date)) {
		return probability < config.prob_diff_ip_travel_season;
	}
	return probability < config.prob_diff_ip_not_travel_season;
}

static int32_t GeneratePostBrowser(const LdbcDatagenConfig &config, const LdbcPersonDictionaries &dictionaries,
                                   LdbcRandomGeneratorFarm &random_farm, int32_t person_browser_id) {
	auto probability = random_farm.Get(LdbcRandomAspect::DIFF_BROWSER).NextDouble();
	if (probability < config.prob_another_browser) {
		return dictionaries.browsers.GetRandomBrowserId(random_farm.Get(LdbcRandomAspect::BROWSER));
	}
	return person_browser_id;
}

static string GeneratePostText(const LdbcDatagenConfig &config, const LdbcPersonDictionaries &dictionaries,
                               LdbcRandomGeneratorFarm &random_farm, const LdbcPersonCore &person,
                               const vector<int32_t> &tags) {
	auto &random = random_farm.Get(LdbcRandomAspect::LARGE_TEXT);
	int32_t text_size;
	if (person.large_poster && random.NextDouble() > (1.0 - config.ratio_large_post)) {
		text_size = dictionaries.tag_text.GetRandomLargeTextSize(random, config.min_large_post_size,
		                                                         config.max_large_post_size);
	} else {
		text_size = dictionaries.tag_text.GetRandomTextSize(random, random, config.min_text_size, config.max_text_size,
		                                                    config.ratio_reduce_text);
	}
	return dictionaries.tag_text.GenerateText(random, tags, text_size);
}

static string GenerateCommentText(const LdbcDatagenConfig &config, const LdbcPersonDictionaries &dictionaries,
                                  LdbcRandomGeneratorFarm &random_farm, const LdbcPersonCore &person,
                                  const vector<int32_t> &tags) {
	auto &random = random_farm.Get(LdbcRandomAspect::LARGE_TEXT);
	int32_t text_size;
	if (person.large_poster && random.NextDouble() > (1.0 - config.ratio_large_comment)) {
		text_size = dictionaries.tag_text.GetRandomLargeTextSize(random, config.min_large_comment_size,
		                                                         config.max_large_comment_size);
	} else {
		text_size = dictionaries.tag_text.GetRandomTextSize(random, random, config.min_comment_size,
		                                                    config.max_comment_size, config.ratio_reduce_text);
	}
	return dictionaries.tag_text.GenerateText(random, tags, text_size);
}

static int64_t NumPostsPerForum(const LdbcDatagenConfig &config, const LdbcDateGenerator &dates,
                                LdbcRandomGeneratorFarm &random_farm, const LdbcForum &forum,
                                int32_t max_posts_per_month, int32_t max_members_per_forum) {
	auto month_count = NumericCast<int32_t>(NumberOfMonths(dates, forum.creation_date));
	int32_t number_post;
	if (month_count == 0) {
		number_post = random_farm.Get(LdbcRandomAspect::NUM_POST).NextInt(max_posts_per_month + 1);
	} else {
		number_post = random_farm.Get(LdbcRandomAspect::NUM_POST).NextInt(max_posts_per_month * month_count + 1);
	}
	return (static_cast<int64_t>(number_post) * static_cast<int64_t>(forum.memberships.size())) / max_members_per_forum;
}

static int32_t PowerDistributionValue(LdbcJavaRandom &random, double min_value, double max_value, double alpha) {
	return static_cast<int32_t>(min_value + (max_value - min_value) * std::pow(random.NextDouble(), 1.0 / alpha));
}

static double PowerDistributionDouble(LdbcJavaRandom &random, double min_value, double max_value, double alpha) {
	return min_value + (max_value - min_value) * std::pow(random.NextDouble(), 1.0 / alpha);
}

class LdbcActivityDeleteDistribution {
public:
	explicit LdbcActivityDeleteDistribution(const string &resource_dir) {
		auto path = LdbcResourcePath(LdbcResourcePath(resource_dir, "dictionaries"), "powerLawActivityDeleteDate.txt");
		std::ifstream file(path);
		if (!file.is_open()) {
			throw IOException("Could not open LDBC activity delete distribution file '%s'", path);
		}
		string line;
		while (std::getline(file, line)) {
			if (!line.empty() && line.back() == '\r') {
				line.pop_back();
			}
			if (!line.empty()) {
				distribution.push_back(std::stod(line));
			}
		}
	}

	int64_t NextDeleteDate(LdbcJavaRandom &random, int64_t min_date, int64_t max_date) const {
		static constexpr double MINUTES[] = {0,   0.5, 1,    5,    10,   20,   30,   40,   60,
		                                     120, 300, 1440, 2880, 4320, 5760, 7200, 8460, 10080};
		auto probability = random.NextDouble();
		double draw = 0.0;
		for (idx_t idx = 0; idx < distribution.size(); idx++) {
			if (probability < distribution[idx]) {
				auto lower = idx == 0 ? MINUTES[0] : MINUTES[idx - 1];
				auto upper = MINUTES[idx];
				draw = lower + (upper - lower) * random.NextDouble();
				break;
			}
		}
		auto deletion_date = min_date + static_cast<int64_t>(draw);
		if (deletion_date > max_date) {
			deletion_date = min_date + (max_date - min_date) / 2;
		}
		return deletion_date;
	}

private:
	vector<double> distribution;
};

static void ConsumeLikes(const LdbcDatagenConfig &config, const LdbcDateGenerator &dates,
                         const LdbcActivityDeleteDistribution &delete_distribution,
                         LdbcRandomGeneratorFarm &random_farm, LdbcForum &forum,
                         const vector<LdbcActivityMembership> &memberships, const LdbcActivityMessage &message,
                         bool comment_like) {
	auto member_count = NumericCast<int32_t>(memberships.size());
	auto like_count = std::min<int32_t>(PowerDistributionValue(random_farm.Get(LdbcRandomAspect::NUM_LIKE), 1.0,
	                                                           static_cast<double>(config.max_num_like), 0.07),
	                                    member_count);
	auto start_index =
	    like_count < member_count ? random_farm.Get(LdbcRandomAspect::NUM_LIKE).NextInt(member_count - like_count) : 0;
	for (int32_t like_idx = 0; like_idx < like_count; like_idx++) {
		auto &membership = memberships[NumericCast<idx_t>(start_index + like_idx)];
		auto min_creation = std::max(membership.person->creation_date, message.creation_date) + config.delta;
		auto max_creation = std::min(
		    std::min(message.creation_date + 7LL * LdbcDateGenerator::ONE_DAY_MS, membership.person->deletion_date),
		    std::min(message.deletion_date, dates.SimulationEnd()));
		if (max_creation <= min_creation) {
			continue;
		}
		auto like_creation = dates.RandomDate(random_farm.Get(LdbcRandomAspect::NUM_LIKE), min_creation, max_creation);
		if (membership.person->message_deleter &&
		    random_farm.Get(LdbcRandomAspect::DELETION_LIKES).NextDouble() < config.prob_like_deleted) {
			auto min_deletion = like_creation + config.delta;
			auto max_deletion =
			    std::min(std::min(membership.person->deletion_date, message.deletion_date), dates.SimulationEnd());
			if (max_deletion <= min_deletion) {
				continue;
			}
			delete_distribution.NextDeleteDate(random_farm.Get(LdbcRandomAspect::NUM_LIKE), min_deletion, max_deletion);
		}
		auto &likes = comment_like ? forum.comment_likes : forum.post_likes;
		likes.push_back({like_creation, membership.person->account_id, message.id});
	}
}

static void ConsumeComments(const LdbcDatagenConfig &config, const LdbcDateGenerator &dates,
                            const LdbcPersonDictionaries &dictionaries,
                            const LdbcActivityDeleteDistribution &delete_distribution,
                            LdbcRandomGeneratorFarm &random_farm, LdbcForum &forum,
                            const vector<LdbcActivityMembership> &memberships, const LdbcActivityMessage &post,
                            int32_t comment_count, int64_t block_id, int64_t &local_message_id) {
	vector<LdbcActivityMessage> parent_candidates;
	parent_candidates.push_back(post);
	for (int32_t comment_idx = 0; comment_idx < comment_count; comment_idx++) {
		auto parent_idx =
		    random_farm.Get(LdbcRandomAspect::REPLY_TO).NextInt(NumericCast<int32_t>(parent_candidates.size()));
		auto parent = parent_candidates[NumericCast<idx_t>(parent_idx)];
		vector<LdbcActivityMembership> valid_memberships;
		for (auto &membership : memberships) {
			if ((membership.creation_date < parent.creation_date && membership.deletion_date > parent.creation_date) ||
			    (membership.creation_date < parent.deletion_date && membership.deletion_date > parent.deletion_date)) {
				valid_memberships.push_back(membership);
			}
		}
		if (valid_memberships.empty()) {
			break;
		}
		auto membership_idx =
		    random_farm.Get(LdbcRandomAspect::MEMBERSHIP_INDEX).NextInt(NumericCast<int32_t>(valid_memberships.size()));
		auto membership = valid_memberships[NumericCast<idx_t>(membership_idx)];

		std::set<int32_t> tags;
		bool is_short = false;
		string content;
		if (random_farm.Get(LdbcRandomAspect::REDUCED_TEXT).NextDouble() > 0.6666) {
			vector<int32_t> current_tags;
			for (auto tag : parent.tags) {
				if (random_farm.Get(LdbcRandomAspect::TAG).NextDouble() > 0.5) {
					tags.insert(tag);
				}
				current_tags.push_back(tag);
			}
			auto related_count = static_cast<int32_t>(std::ceil(static_cast<double>(parent.tags.size()) / 2.0));
			for (int32_t idx = 0; idx < related_count && !current_tags.empty(); idx++) {
				auto random_tag = current_tags[random_farm.Get(LdbcRandomAspect::TAG)
				                                   .NextInt(NumericCast<int32_t>(current_tags.size()))];
				tags.insert(
				    dictionaries.tag_matrix.GetRandomRelated(random_farm.Get(LdbcRandomAspect::TOPIC), random_tag));
			}
			if (tags.empty() && !parent.tags.empty()) {
				tags.insert(parent.tags[0]);
			}
			vector<int32_t> generated_tags(tags.begin(), tags.end());
			content = GenerateCommentText(config, dictionaries, random_farm, *membership.person, generated_tags);
		} else {
			is_short = true;
			auto short_idx =
			    random_farm.Get(LdbcRandomAspect::TEXT_SIZE).NextInt(NumericCast<int32_t>(LDBC_SHORT_COMMENT_COUNT));
			content = LDBC_SHORT_COMMENTS[NumericCast<idx_t>(short_idx)];
		}

		auto min_creation = std::max(parent.creation_date, membership.creation_date) + config.delta;
		auto max_creation = std::min(std::min(membership.deletion_date, parent.deletion_date), dates.SimulationEnd());
		if (max_creation <= min_creation) {
			continue;
		}
		auto creation_date = static_cast<int64_t>(
		    PowerDistributionDouble(random_farm.Get(LdbcRandomAspect::DATE), 0.0, 1.0, LdbcDatagenConfig::ALPHA) *
		        static_cast<double>(LdbcDateGenerator::ONE_DAY_MS) +
		    static_cast<double>(min_creation));
		if (creation_date > max_creation) {
			continue;
		}
		int64_t deletion_date;
		bool explicitly_deleted = false;
		if (membership.person->message_deleter &&
		    random_farm.Get(LdbcRandomAspect::DELETION_COMM).NextDouble() < config.prob_comment_deleted) {
			explicitly_deleted = true;
			auto min_deletion = creation_date + config.delta;
			auto max_deletion =
			    std::min(std::min(parent.deletion_date, membership.deletion_date), dates.SimulationEnd());
			if (max_deletion <= min_deletion) {
				continue;
			}
			deletion_date =
			    delete_distribution.NextDeleteDate(random_farm.Get(LdbcRandomAspect::DATE), min_deletion, max_deletion);
		} else {
			deletion_date = std::min(parent.deletion_date, membership.deletion_date);
		}

		int32_t country_id = membership.person->country_id;
		string ip_address = membership.person->ip_address;
		if (ChangeUsualCountry(config, random_farm.Get(LdbcRandomAspect::DIFF_IP_FOR_TRAVELER), creation_date)) {
			auto country_idx = random_farm.Get(LdbcRandomAspect::COUNTRY)
			                       .NextInt(NumericCast<int32_t>(dictionaries.places.GetCountries().size()));
			country_id = dictionaries.places.GetCountries()[country_idx];
			ip_address = dictionaries.ips.GetIP(random_farm.Get(LdbcRandomAspect::IP), country_id);
		}
		auto browser_id = GeneratePostBrowser(config, dictionaries, random_farm, membership.person->browser_id);

		vector<int32_t> comment_tags(tags.begin(), tags.end());
		auto content_length = LdbcJavaStringLength(content);
		auto comment_id = FormActivityId(config, dates, local_message_id++, creation_date, block_id);
		auto parent_post_id = parent.id == post.id ? parent.id : int64_t(-1);
		auto parent_comment_id = parent.id == post.id ? int64_t(-1) : parent.id;
		forum.comments.push_back({creation_date, deletion_date, explicitly_deleted, comment_id, ip_address,
		                          dictionaries.browsers.GetName(browser_id), content, content_length,
		                          membership.person->account_id, country_id, parent_post_id, parent_comment_id,
		                          comment_tags});
		LdbcActivityMessage comment {membership.person, comment_id,   post.id,  creation_date,
		                             deletion_date,     comment_tags, !is_short};
		if (!is_short) {
			parent_candidates.push_back(comment);
		}
		if (content_length > 10 && random_farm.Get(LdbcRandomAspect::NUM_LIKE).NextDouble() <= 0.1) {
			ConsumeLikes(config, dates, delete_distribution, random_farm, forum, memberships, comment, true);
		}
	}
}

class LdbcFlashmobDictionary {
public:
	LdbcFlashmobDictionary(const LdbcDatagenConfig &config, const LdbcDateGenerator &dates,
	                       const LdbcPersonDictionaries &dictionaries) {
		LdbcJavaRandom random(0);
		auto tag_count = static_cast<int32_t>(static_cast<double>(config.flashmob_tags_per_month) *
		                                      static_cast<double>(NumberOfMonths(dates, dates.SimulationStart())));
		auto tags = dictionaries.tags.GetRandomTags(random, tag_count);
		tags_by_id.reserve(NumericCast<idx_t>(tag_count));
		all_tags.reserve(NumericCast<idx_t>(tag_count));
		double sum_levels = 0.0;
		for (int32_t idx = 0; idx < tag_count; idx++) {
			auto date = dates.RandomDate(random, dates.SimulationStart(), dates.SimulationEnd());
			auto level = static_cast<int32_t>(config.flashmob_tag_min_level +
			                                  (config.flashmob_tag_max_level - config.flashmob_tag_min_level) *
			                                      std::pow(random.NextDouble(), 1.0 / config.flashmob_tag_dist_exp));
			LdbcFlashmobTag tag;
			tag.date = date;
			tag.level = level;
			tag.tag = tags[idx];
			tag.probability = 0.0;
			sum_levels += static_cast<double>(tag.level);
			tags_by_id[tag.tag].push_back(tag);
			all_tags.push_back(tag);
		}
		std::sort(all_tags.begin(), all_tags.end(),
		          [](const LdbcFlashmobTag &left, const LdbcFlashmobTag &right) { return left.date < right.date; });
		double current_probability = 0.0;
		for (auto &tag : all_tags) {
			tag.probability = current_probability;
			current_probability += static_cast<double>(tag.level) / sum_levels;
		}
	}

	vector<LdbcFlashmobTag> GenerateFlashmobTags(LdbcJavaRandom &random, const std::set<int32_t> &interests,
	                                             int64_t from_date, double prob_interest_flashmob_tag,
	                                             double prob_random_per_level) const {
		vector<LdbcFlashmobTag> result;
		for (auto interest : interests) {
			auto entry = tags_by_id.find(interest);
			if (entry == tags_by_id.end()) {
				continue;
			}
			for (auto &tag : entry->second) {
				if (tag.date >= from_date && random.NextDouble() > 1.0 - prob_interest_flashmob_tag) {
					result.push_back(tag);
				}
			}
		}
		auto earliest_idx = SearchEarliestIndex(from_date);
		for (idx_t idx = earliest_idx; idx < all_tags.size(); idx++) {
			if (random.NextDouble() > 1.0 - prob_random_per_level * static_cast<double>(all_tags[idx].level)) {
				result.push_back(all_tags[idx]);
			}
		}
		return result;
	}

private:
	idx_t SearchEarliestIndex(int64_t from_date) const {
		if (all_tags.empty()) {
			return 0;
		}
		idx_t lower_bound = 0;
		idx_t upper_bound = all_tags.size();
		idx_t mid_point = (upper_bound + lower_bound) / 2;
		while (upper_bound > lower_bound + 1) {
			if (all_tags[mid_point].date > from_date) {
				upper_bound = mid_point;
			} else {
				lower_bound = mid_point;
			}
			mid_point = (upper_bound + lower_bound) / 2;
		}
		return mid_point;
	}

	unordered_map<int32_t, vector<LdbcFlashmobTag>> tags_by_id;
	vector<LdbcFlashmobTag> all_tags;
};

class LdbcFlashmobDateDistribution {
public:
	explicit LdbcFlashmobDateDistribution(const string &resource_dir) {
		auto path = LdbcResourcePath(LdbcResourcePath(resource_dir, "dictionaries"), "flashmobDist.txt");
		std::ifstream file(path);
		if (!file.is_open()) {
			throw IOException("Could not open LDBC flashmob distribution file '%s'", path);
		}
		string line;
		while (std::getline(file, line)) {
			if (!line.empty() && line.back() == '\r') {
				line.pop_back();
			}
			if (!line.empty()) {
				distribution.push_back(std::stod(line));
			}
		}
	}

	double NextDouble(LdbcJavaRandom &random) const {
		auto probability = random.NextDouble();
		idx_t upper_bound = distribution.size() - 1;
		idx_t lower_bound = 0;
		idx_t mid_point = (upper_bound + lower_bound) / 2;
		while (upper_bound > lower_bound + 1) {
			if (distribution[mid_point] > probability) {
				upper_bound = mid_point;
			} else {
				lower_bound = mid_point;
			}
			mid_point = (upper_bound + lower_bound) / 2;
		}
		return static_cast<double>(mid_point) / static_cast<double>(distribution.size());
	}

private:
	vector<double> distribution;
};

class LdbcPopularPlaceDictionary {
public:
	LdbcPopularPlaceDictionary(const string &dictionary_dir, const LdbcPlaceDictionary &places) {
		for (auto country_id : places.GetCountries()) {
			popular_place_counts[country_id] = 0;
		}
		auto path = LdbcResourcePath(dictionary_dir, "popularPlacesByCountry.txt");
		std::ifstream file(path);
		if (!file.is_open()) {
			throw IOException("Could not open LDBC popular places file '%s'", path);
		}
		string line;
		while (std::getline(file, line)) {
			if (!line.empty() && line.back() == '\r') {
				line.pop_back();
			}
			if (line.empty()) {
				continue;
			}
			auto separator = line.find("  ");
			if (separator == string::npos) {
				continue;
			}
			auto country_id = places.GetCountryId(line.substr(0, separator));
			if (country_id != -1) {
				popular_place_counts[country_id]++;
			}
		}
	}

	void ConsumePopularPlace(LdbcJavaRandom &random, int32_t country_id) const {
		auto entry = popular_place_counts.find(country_id);
		if (entry == popular_place_counts.end() || entry->second == 0) {
			return;
		}
		random.NextInt(entry->second);
	}

private:
	unordered_map<int32_t, int32_t> popular_place_counts;
};

static void ConsumeUniformPosts(const LdbcDatagenConfig &config, const LdbcDateGenerator &dates,
                                const LdbcPersonDictionaries &dictionaries,
                                const LdbcActivityDeleteDistribution &delete_distribution,
                                LdbcRandomGeneratorFarm &random_farm, LdbcForum &forum,
                                const vector<LdbcActivityMembership> &memberships,
                                const vector<LdbcActivityMembership> &forum_memberships, int64_t num_posts_in_forum,
                                int64_t block_id, int64_t &local_message_id) {
	if (memberships.empty()) {
		return;
	}
	for (auto &membership : memberships) {
		auto posts_per_member = static_cast<double>(num_posts_in_forum) / static_cast<double>(memberships.size());
		if (posts_per_member < 1.0) {
			auto probability = random_farm.Get(LdbcRandomAspect::NUM_POST).NextDouble();
			if (probability < posts_per_member) {
				posts_per_member = 1.0;
			}
		} else {
			posts_per_member = std::ceil(posts_per_member);
		}
		auto post_count = static_cast<int32_t>(posts_per_member);
		auto comment_count = random_farm.Get(LdbcRandomAspect::NUM_COMMENT).NextInt(config.max_num_comments + 1);
		for (int32_t post_idx = 0; post_idx < post_count; post_idx++) {
			auto min_creation = membership.creation_date + config.delta;
			auto max_creation = std::min(membership.deletion_date, dates.SimulationEnd());
			if (max_creation - min_creation < 0) {
				continue;
			}
			auto post_creation = dates.RandomDate(random_farm.Get(LdbcRandomAspect::DATE), min_creation, max_creation);
			int64_t post_deletion;
			bool explicitly_deleted = false;
			if (membership.person->message_deleter && random_farm.Get(LdbcRandomAspect::DELETION_POST).NextDouble() <
			                                              LDBC_POST_DELETE_MAPPING[comment_count]) {
				explicitly_deleted = true;
				auto min_deletion = post_creation + config.delta;
				auto max_deletion = std::min(membership.deletion_date, dates.SimulationEnd());
				if (max_deletion - min_deletion < 0) {
					continue;
				}
				post_deletion = delete_distribution.NextDeleteDate(random_farm.Get(LdbcRandomAspect::DATE),
				                                                   min_deletion, max_deletion);
			} else {
				post_deletion = std::min(membership.deletion_date, dates.SimulationEnd());
			}
			bool first_tag = true;
			vector<int32_t> post_tags;
			for (auto tag_id : forum.tags) {
				if (first_tag) {
					first_tag = false;
					post_tags.push_back(tag_id);
				} else {
					if (random_farm.Get(LdbcRandomAspect::TAG).NextDouble() < 0.05) {
						post_tags.push_back(tag_id);
					}
				}
			}
			auto content = GeneratePostText(config, dictionaries, random_farm, *membership.person, post_tags);
			auto content_length = LdbcJavaStringLength(content);
			if (ChangeUsualCountry(config, random_farm.Get(LdbcRandomAspect::DIFF_IP_FOR_TRAVELER), post_creation)) {
				auto country_idx = random_farm.Get(LdbcRandomAspect::COUNTRY)
				                       .NextInt(NumericCast<int32_t>(dictionaries.places.GetCountries().size()));
				auto country_id = dictionaries.places.GetCountries()[country_idx];
				auto ip_address = dictionaries.ips.GetIP(random_farm.Get(LdbcRandomAspect::IP), country_id);
				auto browser_id = GeneratePostBrowser(config, dictionaries, random_farm, membership.person->browser_id);
				auto message_id = FormActivityId(config, dates, local_message_id++, post_creation, block_id);
				forum.posts.push_back({post_creation, post_deletion, explicitly_deleted, message_id, "", ip_address,
				                       dictionaries.browsers.GetName(browser_id),
				                       dictionaries.languages.GetLanguageName(forum.language_id), content,
				                       content_length, membership.person->account_id, forum.id, country_id, post_tags});
			} else {
				auto browser_id = GeneratePostBrowser(config, dictionaries, random_farm, membership.person->browser_id);
				auto message_id = FormActivityId(config, dates, local_message_id++, post_creation, block_id);
				forum.posts.push_back({post_creation, post_deletion, explicitly_deleted, message_id, "",
				                       membership.person->ip_address, dictionaries.browsers.GetName(browser_id),
				                       dictionaries.languages.GetLanguageName(forum.language_id), content,
				                       content_length, membership.person->account_id, forum.id,
				                       membership.person->country_id, post_tags});
			}
			auto &emitted_post = forum.posts.back();
			LdbcActivityMessage post {
			    membership.person, emitted_post.id, emitted_post.id, post_creation, post_deletion, post_tags, true};
			if (random_farm.Get(LdbcRandomAspect::NUM_LIKE).NextDouble() <= 0.1) {
				ConsumeLikes(config, dates, delete_distribution, random_farm, forum, forum_memberships, post, false);
			}
			ConsumeComments(config, dates, dictionaries, delete_distribution, random_farm, forum, forum_memberships,
			                post, comment_count, block_id, local_message_id);
		}
	}
}

static idx_t SearchEarliestForumFlashmobTag(const vector<LdbcFlashmobTag> &tags,
                                            const LdbcActivityMembership &membership, int64_t flashmob_span,
                                            int32_t delta) {
	auto from_date = membership.creation_date + flashmob_span / 2 + delta;
	idx_t lower_bound = 0;
	idx_t upper_bound = tags.size() - 1;
	idx_t mid_point = (upper_bound + lower_bound) / 2;
	while (upper_bound > lower_bound + 1) {
		if (tags[mid_point].date > from_date) {
			upper_bound = mid_point;
		} else {
			lower_bound = mid_point;
		}
		mid_point = (upper_bound + lower_bound) / 2;
	}
	if (tags[mid_point].date < from_date) {
		return DConstants::INVALID_INDEX;
	}
	return mid_point;
}

static idx_t SelectForumFlashmobTag(LdbcJavaRandom &random, const vector<LdbcFlashmobTag> &tags, idx_t index) {
	auto upper_bound = tags.size() - 1;
	auto lower_bound = index;
	auto probability = random.NextDouble() * (tags[upper_bound].probability - tags[lower_bound].probability) +
	                   tags[lower_bound].probability;
	auto mid_point = (upper_bound + lower_bound) / 2;
	while (upper_bound > lower_bound + 1) {
		if (tags[mid_point].probability > probability) {
			upper_bound = mid_point;
		} else {
			lower_bound = mid_point;
		}
		mid_point = (upper_bound + lower_bound) / 2;
	}
	return mid_point;
}

static vector<LdbcFlashmobTag> BuildForumFlashmobTags(const LdbcDatagenConfig &config,
                                                      const LdbcFlashmobDictionary &flashmobs,
                                                      LdbcRandomGeneratorFarm &random_farm, const LdbcForum &forum) {
	std::set<int32_t> interests;
	auto tags = flashmobs.GenerateFlashmobTags(random_farm.Get(LdbcRandomAspect::TAG), interests, forum.creation_date,
	                                           config.prob_interest_flashmob_tag, config.prob_random_per_level);
	std::sort(tags.begin(), tags.end(),
	          [](const LdbcFlashmobTag &left, const LdbcFlashmobTag &right) { return left.date < right.date; });
	double sum_levels = 0.0;
	for (auto &tag : tags) {
		sum_levels += static_cast<double>(tag.level);
	}
	double current_probability = 0.0;
	for (auto &tag : tags) {
		tag.probability = current_probability;
		current_probability += static_cast<double>(tag.level) / sum_levels;
	}
	return tags;
}

static void ConsumeFlashmobPosts(const LdbcDatagenConfig &config, const LdbcDateGenerator &dates,
                                 const LdbcPersonDictionaries &dictionaries, const LdbcFlashmobDictionary &flashmobs,
                                 const LdbcFlashmobDateDistribution &date_distribution,
                                 const LdbcActivityDeleteDistribution &delete_distribution,
                                 LdbcRandomGeneratorFarm &random_farm, LdbcForum &forum,
                                 const vector<LdbcActivityMembership> &memberships,
                                 const vector<LdbcActivityMembership> &forum_memberships, int64_t num_posts_in_forum,
                                 int64_t block_id, int64_t &local_message_id) {
	if (memberships.empty()) {
		return;
	}
	constexpr int64_t FLASHMOB_SPAN = 72LL * 60LL * 60LL * 1000LL;
	vector<LdbcFlashmobTag> forum_flashmob_tags;
	bool flashmob_tags_loaded = false;
	for (auto &membership : memberships) {
		auto posts_per_member = static_cast<double>(num_posts_in_forum) / static_cast<double>(memberships.size());
		if (posts_per_member < 1.0) {
			auto probability = random_farm.Get(LdbcRandomAspect::NUM_POST).NextDouble();
			if (probability < posts_per_member) {
				posts_per_member = 1.0;
			}
		} else {
			posts_per_member = std::ceil(posts_per_member);
		}
		auto post_count = static_cast<int32_t>(posts_per_member);
		auto comment_count = random_farm.Get(LdbcRandomAspect::NUM_COMMENT).NextInt(config.max_num_comments + 1);
		for (int32_t post_idx = 0; post_idx < post_count; post_idx++) {
			if (!flashmob_tags_loaded) {
				forum_flashmob_tags = BuildForumFlashmobTags(config, flashmobs, random_farm, forum);
				flashmob_tags_loaded = true;
			}
			if (forum_flashmob_tags.empty()) {
				continue;
			}
			auto earliest_idx =
			    SearchEarliestForumFlashmobTag(forum_flashmob_tags, membership, FLASHMOB_SPAN, config.delta);
			if (earliest_idx == DConstants::INVALID_INDEX) {
				continue;
			}
			auto flashmob_idx =
			    SelectForumFlashmobTag(random_farm.Get(LdbcRandomAspect::TAG), forum_flashmob_tags, earliest_idx);
			auto flashmob_tag = forum_flashmob_tags[flashmob_idx];
			std::set<int32_t> post_tags;
			post_tags.insert(flashmob_tag.tag);
			for (int32_t tag_idx = 0; tag_idx < config.max_num_tag_per_flashmob_post - 1; tag_idx++) {
				if (random_farm.Get(LdbcRandomAspect::TAG).NextDouble() < 0.05) {
					post_tags.insert(dictionaries.tag_matrix.GetRandomRelated(random_farm.Get(LdbcRandomAspect::TAG),
					                                                          flashmob_tag.tag));
				}
			}
			auto min_creation = membership.creation_date + config.delta;
			auto max_creation = std::min(membership.deletion_date, dates.SimulationEnd());
			if (max_creation - min_creation < 0) {
				continue;
			}
			auto probability = date_distribution.NextDouble(random_farm.Get(LdbcRandomAspect::DATE));
			auto post_creation = flashmob_tag.date - FLASHMOB_SPAN / 2 +
			                     static_cast<int64_t>(probability * static_cast<double>(FLASHMOB_SPAN));
			if (post_creation <= min_creation || post_creation >= max_creation) {
				continue;
			}
			int64_t post_deletion;
			bool explicitly_deleted = false;
			if (membership.person->message_deleter && random_farm.Get(LdbcRandomAspect::DELETION_POST).NextDouble() <
			                                              LDBC_POST_DELETE_MAPPING[comment_count]) {
				explicitly_deleted = true;
				auto min_deletion = post_creation + config.delta;
				auto max_deletion = std::min(membership.deletion_date, dates.SimulationEnd());
				if (max_deletion - min_deletion < 0) {
					continue;
				}
				post_deletion = delete_distribution.NextDeleteDate(random_farm.Get(LdbcRandomAspect::DATE),
				                                                   min_deletion, max_deletion);
			} else {
				post_deletion = std::min(membership.deletion_date, dates.SimulationEnd());
			}
			vector<int32_t> post_tag_list(post_tags.begin(), post_tags.end());
			auto content = GeneratePostText(config, dictionaries, random_farm, *membership.person, post_tag_list);
			auto content_length = LdbcJavaStringLength(content);
			if (ChangeUsualCountry(config, random_farm.Get(LdbcRandomAspect::DIFF_IP_FOR_TRAVELER), post_creation)) {
				auto country_idx = random_farm.Get(LdbcRandomAspect::COUNTRY)
				                       .NextInt(NumericCast<int32_t>(dictionaries.places.GetCountries().size()));
				auto country_id = dictionaries.places.GetCountries()[country_idx];
				auto ip_address = dictionaries.ips.GetIP(random_farm.Get(LdbcRandomAspect::IP), country_id);
				auto browser_id = GeneratePostBrowser(config, dictionaries, random_farm, membership.person->browser_id);
				auto message_id = FormActivityId(config, dates, local_message_id++, post_creation, block_id);
				forum.posts.push_back({post_creation, post_deletion, explicitly_deleted, message_id, "", ip_address,
				                       dictionaries.browsers.GetName(browser_id),
				                       dictionaries.languages.GetLanguageName(forum.language_id), content,
				                       content_length, membership.person->account_id, forum.id, country_id,
				                       post_tag_list});
			} else {
				auto browser_id = GeneratePostBrowser(config, dictionaries, random_farm, membership.person->browser_id);
				auto message_id = FormActivityId(config, dates, local_message_id++, post_creation, block_id);
				forum.posts.push_back({post_creation, post_deletion, explicitly_deleted, message_id, "",
				                       membership.person->ip_address, dictionaries.browsers.GetName(browser_id),
				                       dictionaries.languages.GetLanguageName(forum.language_id), content,
				                       content_length, membership.person->account_id, forum.id,
				                       membership.person->country_id, post_tag_list});
			}
			auto &emitted_post = forum.posts.back();
			LdbcActivityMessage post {membership.person,
			                          emitted_post.id,
			                          emitted_post.id,
			                          post_creation,
			                          post_deletion,
			                          emitted_post.tags,
			                          true};
			if (random_farm.Get(LdbcRandomAspect::NUM_LIKE).NextDouble() <= 0.1) {
				ConsumeLikes(config, dates, delete_distribution, random_farm, forum, forum_memberships, post, false);
			}
			ConsumeComments(config, dates, dictionaries, delete_distribution, random_farm, forum, forum_memberships,
			                post, comment_count, block_id, local_message_id);
		}
	}
}

static void ConsumePhotos(const LdbcDatagenConfig &config, const LdbcDateGenerator &dates,
                          const LdbcPersonDictionaries &dictionaries, const LdbcPopularPlaceDictionary &popular_places,
                          const LdbcActivityDeleteDistribution &delete_distribution,
                          LdbcRandomGeneratorFarm &random_farm, LdbcForum &album, const LdbcPersonCore &moderator,
                          const vector<LdbcActivityMembership> &album_memberships, int32_t photo_count,
                          int64_t block_id, int64_t &local_message_id) {
	auto popular_count = random_farm.Get(LdbcRandomAspect::NUM_POPULAR).NextInt(config.max_num_popular_places + 1);
	for (int32_t idx = 0; idx < popular_count; idx++) {
		popular_places.ConsumePopularPlace(random_farm.Get(LdbcRandomAspect::POPULAR), album.place_id);
	}
	for (int32_t photo_idx = 0; photo_idx < photo_count; photo_idx++) {
		auto creation_date = album.creation_date + config.delta + 1000LL * static_cast<int64_t>(photo_idx + 1);
		if (creation_date >= album.deletion_date) {
			break;
		}
		int64_t deletion_date;
		bool explicitly_deleted = false;
		if (moderator.message_deleter &&
		    random_farm.Get(LdbcRandomAspect::DELETION_POST).NextDouble() < config.prob_photo_deleted) {
			explicitly_deleted = true;
			auto min_deletion = creation_date + config.delta;
			auto max_deletion = std::min(album.deletion_date, dates.SimulationEnd());
			deletion_date =
			    delete_distribution.NextDeleteDate(random_farm.Get(LdbcRandomAspect::DATE), min_deletion, max_deletion);
		} else {
			deletion_date = album.deletion_date;
		}
		int32_t country_id = moderator.country_id;
		string ip_address = moderator.ip_address;
		if (ChangeUsualCountry(config, random_farm.Get(LdbcRandomAspect::DIFF_IP_FOR_TRAVELER), creation_date)) {
			auto country_idx = random_farm.Get(LdbcRandomAspect::COUNTRY)
			                       .NextInt(NumericCast<int32_t>(dictionaries.places.GetCountries().size()));
			country_id = dictionaries.places.GetCountries()[country_idx];
			ip_address = dictionaries.ips.GetIP(random_farm.Get(LdbcRandomAspect::IP), country_id);
		}
		auto message_id = FormActivityId(config, dates, local_message_id++, creation_date, block_id);
		album.posts.push_back({creation_date, deletion_date, explicitly_deleted, message_id,
		                       "photo" + std::to_string(message_id) + ".jpg", ip_address,
		                       dictionaries.browsers.GetName(moderator.browser_id), "", "", 0, moderator.account_id,
		                       album.id, country_id, vector<int32_t>()});
		LdbcActivityMessage photo {&moderator,    message_id,        message_id, creation_date,
		                           deletion_date, vector<int32_t>(), true};
		if (random_farm.Get(LdbcRandomAspect::NUM_LIKE).NextDouble() <= 0.1) {
			ConsumeLikes(config, dates, delete_distribution, random_farm, album, album_memberships, photo, false);
		}
	}
}

static LdbcForum CreateWallForum(const LdbcDatagenConfig &config, const LdbcDateGenerator &dates,
                                 LdbcRandomGeneratorFarm &random_farm, const LdbcPersonCore &person,
                                 const vector<LdbcActivityFriend> &friends, int64_t local_forum_id, int64_t block_id) {
	LdbcForum forum;
	forum.creation_date = person.creation_date + config.delta;
	forum.deletion_date = person.deletion_date;
	forum.explicitly_deleted = false;
	forum.id = FormActivityId(config, dates, local_forum_id, forum.creation_date, block_id);
	forum.title = ClampStringLocal("Wall of " + person.first_name + " " + person.last_name, 256);
	forum.moderator_person_id = person.account_id;
	forum.place_id = person.city_id;
	forum.tags = person.interests;
	forum.language_id = RandomPersonLanguage(random_farm, person);

	for (auto &friend_entry : friends) {
		auto membership_creation_date = friend_entry.creation_date + config.delta;
		auto membership_deletion_date = std::min(forum.deletion_date, friend_entry.deletion_date);
		if (membership_deletion_date - membership_creation_date < 0) {
			continue;
		}
		forum.memberships.push_back(
		    {membership_creation_date, membership_deletion_date, false, forum.id, friend_entry.person->account_id});
	}
	return forum;
}

static bool CreateGroupForum(const LdbcDatagenConfig &config, const LdbcDateGenerator &dates,
                             const LdbcPersonDictionaries &dictionaries, LdbcRandomGeneratorFarm &random_farm,
                             const LdbcPersonCore &moderator, const vector<const LdbcPersonCore *> &block,
                             const vector<LdbcActivityFriend> &friends, int64_t local_forum_id, int64_t block_id,
                             LdbcForum &forum) {
	auto min_creation = moderator.creation_date + config.delta;
	auto max_creation = std::min(moderator.deletion_date, dates.SimulationEnd());
	if (max_creation <= min_creation) {
		return false;
	}
	auto creation_date = dates.RandomDate(random_farm.Get(LdbcRandomAspect::DATE), min_creation, max_creation);
	bool explicitly_deleted =
	    random_farm.Get(LdbcRandomAspect::DELETION_FORUM).NextDouble() < config.prob_forum_deleted;
	int64_t deletion_date;
	if (explicitly_deleted) {
		auto min_deletion = creation_date + config.delta;
		auto max_deletion = dates.SimulationEnd();
		if (max_deletion <= min_deletion) {
			return false;
		}
		deletion_date = dates.RandomDate(random_farm.Get(LdbcRandomAspect::DATE), min_deletion, max_deletion);
	} else {
		deletion_date = dates.NetworkCollapse();
	}
	auto language_id = RandomPersonLanguage(random_farm, moderator);
	auto interest_id = RandomPersonInterest(random_farm, moderator);
	if (interest_id == -1) {
		return false;
	}

	forum.creation_date = creation_date;
	forum.deletion_date = deletion_date;
	forum.explicitly_deleted = explicitly_deleted;
	forum.id = FormActivityId(config, dates, local_forum_id, creation_date, block_id);
	forum.title = ClampStringLocal("Group for " + EscapedTagName(dictionaries, interest_id) + " in " +
	                                   dictionaries.places.GetPlaceName(moderator.city_id),
	                               256);
	forum.moderator_person_id = moderator.account_id;
	forum.place_id = moderator.city_id;
	forum.language_id = language_id;
	forum.tags = {interest_id};

	std::set<int64_t> group_members;
	auto group_size = random_farm.Get(LdbcRandomAspect::NUM_USERS_PER_FORUM).NextInt(config.max_group_size);
	int32_t num_loop = 0;
	while (NumericCast<int32_t>(forum.memberships.size()) < group_size && num_loop < config.block_size) {
		auto friend_probability = random_farm.Get(LdbcRandomAspect::KNOWS_LEVEL).NextDouble();
		const LdbcPersonCore *member = nullptr;
		int64_t member_creation_floor = creation_date;
		int64_t member_deletion_ceil = deletion_date;
		if (friend_probability < 0.3 && !friends.empty()) {
			auto friend_idx =
			    random_farm.Get(LdbcRandomAspect::MEMBERSHIP_INDEX).NextInt(NumericCast<int32_t>(friends.size()));
			auto &friend_entry = friends[friend_idx];
			member = friend_entry.person;
		} else if (!block.empty()) {
			auto candidate_idx =
			    random_farm.Get(LdbcRandomAspect::MEMBERSHIP_INDEX).NextInt(NumericCast<int32_t>(block.size()));
			member = block[candidate_idx];
			if (random_farm.Get(LdbcRandomAspect::MEMBERSHIP).NextDouble() >= 0.1) {
				num_loop++;
				continue;
			}
		}
		if (!member || group_members.find(member->account_id) != group_members.end()) {
			num_loop++;
			continue;
		}
		auto min_member_creation = std::max(member_creation_floor, member->creation_date) + config.delta;
		auto max_member_creation =
		    std::min(std::min(member_deletion_ceil, member->deletion_date), dates.SimulationEnd());
		if (max_member_creation <= min_member_creation) {
			num_loop++;
			continue;
		}
		auto &membership_random = random_farm.Get(LdbcRandomAspect::MEMBERSHIP_INDEX);
		auto membership_creation = dates.RandomDate(membership_random, min_member_creation, max_member_creation);
		bool membership_deleted =
		    random_farm.Get(LdbcRandomAspect::DELETION_MEMB).NextDouble() < config.prob_memb_deleted;
		int64_t membership_deletion;
		if (membership_deleted) {
			auto min_member_deletion = membership_creation + config.delta;
			auto max_member_deletion = std::min(std::min(member->deletion_date, deletion_date), dates.SimulationEnd());
			if (max_member_deletion <= min_member_deletion) {
				num_loop++;
				continue;
			}
			membership_deletion = dates.RandomDate(membership_random, min_member_deletion, max_member_deletion);
		} else {
			membership_deletion = std::min(member->deletion_date, deletion_date);
		}
		forum.memberships.push_back(
		    {membership_creation, membership_deletion, membership_deleted, forum.id, member->account_id});
		group_members.insert(member->account_id);
		num_loop++;
	}
	return true;
}

static bool CreateAlbumForum(const LdbcDatagenConfig &config, const LdbcDateGenerator &dates,
                             const LdbcPersonDictionaries &dictionaries, LdbcRandomGeneratorFarm &random_farm,
                             const LdbcPersonCore &person, const vector<LdbcActivityFriend> &friends,
                             int64_t local_forum_id, int32_t album_number, int64_t block_id, LdbcForum &forum) {
	auto min_creation = person.creation_date + config.delta;
	auto max_creation = std::min(person.deletion_date, dates.SimulationEnd());
	if (max_creation <= min_creation) {
		return false;
	}
	auto creation_date = dates.RandomDate(random_farm.Get(LdbcRandomAspect::DATE), min_creation, max_creation);
	bool explicitly_deleted =
	    random_farm.Get(LdbcRandomAspect::DELETION_FORUM).NextDouble() < config.prob_forum_deleted;
	int64_t deletion_date;
	if (explicitly_deleted) {
		auto min_deletion = creation_date + config.delta;
		auto max_deletion = std::min(person.deletion_date, dates.SimulationEnd());
		if (max_deletion - min_creation < 0) {
			return false;
		}
		deletion_date = RandomDateUnchecked(random_farm.Get(LdbcRandomAspect::DATE), min_deletion, max_deletion);
	} else {
		deletion_date = person.deletion_date;
	}
	auto language_id = RandomPersonLanguage(random_farm, person);
	auto interest_id = RandomPersonInterest(random_farm, person);
	if (interest_id == -1) {
		return false;
	}
	auto album_country_idx = random_farm.Get(LdbcRandomAspect::COUNTRY)
	                             .NextInt(NumericCast<int32_t>(dictionaries.places.GetCountries().size()));
	auto album_country_id = dictionaries.places.GetCountries()[album_country_idx];

	forum.creation_date = creation_date;
	forum.deletion_date = deletion_date;
	forum.explicitly_deleted = explicitly_deleted;
	forum.id = FormActivityId(config, dates, local_forum_id, creation_date, block_id);
	forum.title = ClampStringLocal(
	    "Album " + std::to_string(album_number) + " of " + person.first_name + " " + person.last_name, 256);
	forum.moderator_person_id = person.account_id;
	forum.place_id = album_country_id;
	forum.language_id = language_id;
	forum.tags = {interest_id};

	for (auto &friend_entry : friends) {
		if (random_farm.Get(LdbcRandomAspect::ALBUM_MEMBERSHIP).NextDouble() >= 0.7) {
			continue;
		}
		auto membership_creation = std::max(friend_entry.person->creation_date, creation_date) + config.delta;
		auto membership_deletion = std::min(friend_entry.person->deletion_date, deletion_date);
		if (membership_deletion - membership_creation > 0) {
			forum.memberships.push_back(
			    {membership_creation, membership_deletion, false, forum.id, friend_entry.person->account_id});
		}
	}
	return true;
}

} // namespace

vector<LdbcForum> LdbcGenerateForums(const LdbcDatagenConfig &config, const vector<LdbcPersonCore> &persons,
                                     const vector<LdbcKnowsEdge> &knows_edges) {
	LdbcDateGenerator dates(config);
	LdbcPersonDictionaries dictionaries(config.resource_dir, config.prob_english, config.prob_second_lang,
	                                    config.tag_country_corr_prob, config.prob_uncorrelated_company,
	                                    config.prob_uncorrelated_organisation, config.prob_top_univ);
	LdbcFlashmobDictionary flashmobs(config, dates, dictionaries);
	LdbcFlashmobDateDistribution flashmob_dates(config.resource_dir);
	LdbcActivityDeleteDistribution delete_distribution(config.resource_dir);
	LdbcPopularPlaceDictionary popular_places(LdbcResourcePath(config.resource_dir, "dictionaries"),
	                                          dictionaries.places);

	unordered_map<int64_t, const LdbcPersonCore *> persons_by_id;
	for (auto &person : persons) {
		persons_by_id[person.account_id] = &person;
	}
	unordered_map<int64_t, vector<LdbcActivityFriend>> friends_by_person;
	for (auto &edge : knows_edges) {
		auto left = persons_by_id.find(edge.person1_id);
		auto right = persons_by_id.find(edge.person2_id);
		if (left == persons_by_id.end() || right == persons_by_id.end()) {
			continue;
		}
		friends_by_person[edge.person1_id].push_back({right->second, edge.creation_date, edge.deletion_date});
		friends_by_person[edge.person2_id].push_back({left->second, edge.creation_date, edge.deletion_date});
	}
	for (auto &entry : friends_by_person) {
		std::sort(entry.second.begin(), entry.second.end(),
		          [](const LdbcActivityFriend &left, const LdbcActivityFriend &right) {
			          return left.person->account_id < right.person->account_id;
		          });
	}

	vector<const LdbcPersonCore *> random_ranked_persons;
	random_ranked_persons.reserve(persons.size());
	for (auto &person : persons) {
		random_ranked_persons.push_back(&person);
	}
	std::sort(random_ranked_persons.begin(), random_ranked_persons.end(),
	          [](const LdbcPersonCore *left, const LdbcPersonCore *right) {
		          if (left->random_id != right->random_id) {
			          return left->random_id < right->random_id;
		          }
		          return left->account_id < right->account_id;
	          });

	vector<LdbcForum> forums;
	for (idx_t block_start = 0; block_start < random_ranked_persons.size();
	     block_start += NumericCast<idx_t>(config.block_size)) {
		auto block_end =
		    std::min<idx_t>(random_ranked_persons.size(), block_start + NumericCast<idx_t>(config.block_size));
		auto block_id = block_start / NumericCast<idx_t>(config.block_size);
		LdbcRandomGeneratorFarm random_farm;
		random_farm.Reset(NumericCast<int64_t>(block_id));
		int64_t local_forum_id = 0;
		int64_t local_message_id = 0;

		vector<const LdbcPersonCore *> block;
		for (idx_t idx = block_start; idx < block_end; idx++) {
			block.push_back(random_ranked_persons[idx]);
		}
		std::sort(block.begin(), block.end(), [](const LdbcPersonCore *left, const LdbcPersonCore *right) {
			return left->account_id < right->account_id;
		});

		for (auto *person : block) {
			auto &friends = friends_by_person[person->account_id];
			if (person->deletion_date - person->creation_date + config.delta >= 0) {
				auto wall = CreateWallForum(config, dates, random_farm, *person, friends, local_forum_id++, block_id);
				vector<LdbcActivityMembership> wall_post_memberships {
				    {person, wall.creation_date + config.delta, wall.deletion_date}};
				vector<LdbcActivityMembership> wall_forum_memberships;
				for (auto &membership : wall.memberships) {
					auto member = persons_by_id.find(membership.person_id);
					if (member != persons_by_id.end()) {
						wall_forum_memberships.push_back(
						    {member->second, membership.creation_date, membership.deletion_date});
					}
				}
				auto uniform_posts = NumPostsPerForum(config, dates, random_farm, wall, config.max_num_post_per_month,
				                                      config.max_num_friends);
				auto flashmob_posts = NumPostsPerForum(config, dates, random_farm, wall,
				                                       config.max_num_flashmob_post_per_month, config.max_num_friends);
				ConsumeUniformPosts(config, dates, dictionaries, delete_distribution, random_farm, wall,
				                    wall_post_memberships, wall_forum_memberships, uniform_posts,
				                    NumericCast<int64_t>(block_id), local_message_id);
				ConsumeFlashmobPosts(config, dates, dictionaries, flashmobs, flashmob_dates, delete_distribution,
				                     random_farm, wall, wall_post_memberships, wall_forum_memberships, flashmob_posts,
				                     NumericCast<int64_t>(block_id), local_message_id);
				forums.push_back(std::move(wall));
			} else {
				local_forum_id++;
			}

			auto moderator_probability = random_farm.Get(LdbcRandomAspect::FORUM_MODERATOR).NextDouble();
			auto group_count =
			    random_farm.Get(LdbcRandomAspect::NUM_FORUM).NextInt(config.max_num_group_created_per_person) + 1;
			for (int32_t group_idx = 0; group_idx < group_count; group_idx++) {
				if (moderator_probability >= config.group_moderator_prob) {
					continue;
				}
				LdbcForum forum;
				if (CreateGroupForum(config, dates, dictionaries, random_farm, *person, block, friends, local_forum_id,
				                     block_id, forum)) {
					vector<LdbcActivityMembership> group_post_memberships;
					for (auto &membership : forum.memberships) {
						auto member = persons_by_id.find(membership.person_id);
						if (member != persons_by_id.end()) {
							group_post_memberships.push_back(
							    {member->second, membership.creation_date, membership.deletion_date});
						}
					}
					auto uniform_posts = NumPostsPerForum(config, dates, random_farm, forum,
					                                      config.max_num_group_post_per_month, config.max_group_size);
					auto flashmob_posts =
					    NumPostsPerForum(config, dates, random_farm, forum,
					                     config.max_num_group_flashmob_post_per_month, config.max_group_size);
					ConsumeUniformPosts(config, dates, dictionaries, delete_distribution, random_farm, forum,
					                    group_post_memberships, group_post_memberships, uniform_posts,
					                    NumericCast<int64_t>(block_id), local_message_id);
					ConsumeFlashmobPosts(config, dates, dictionaries, flashmobs, flashmob_dates, delete_distribution,
					                     random_farm, forum, group_post_memberships, group_post_memberships,
					                     flashmob_posts, NumericCast<int64_t>(block_id), local_message_id);
					forums.push_back(std::move(forum));
				}
				local_forum_id++;
			}

			auto month_count = NumericCast<int32_t>(NumberOfMonths(dates, person->creation_date));
			auto album_per_month =
			    random_farm.Get(LdbcRandomAspect::NUM_PHOTO_ALBUM).NextInt(config.max_num_photo_albums_per_month + 1);
			auto album_count = album_per_month == 0 ? 0 : month_count * album_per_month;
			for (int32_t album_idx = 0; album_idx < album_count; album_idx++) {
				LdbcForum forum;
				if (CreateAlbumForum(config, dates, dictionaries, random_farm, *person, friends, local_forum_id,
				                     album_idx, block_id, forum)) {
					vector<LdbcActivityMembership> album_memberships;
					for (auto &membership : forum.memberships) {
						auto member = persons_by_id.find(membership.person_id);
						if (member != persons_by_id.end()) {
							album_memberships.push_back(
							    {member->second, membership.creation_date, membership.deletion_date});
						}
					}
					auto photo_count =
					    random_farm.Get(LdbcRandomAspect::NUM_PHOTO).NextInt(config.max_num_photo_per_albums + 1);
					ConsumePhotos(config, dates, dictionaries, popular_places, delete_distribution, random_farm, forum,
					              *person, album_memberships, photo_count, NumericCast<int64_t>(block_id),
					              local_message_id);
					forums.push_back(std::move(forum));
				}
				local_forum_id++;
			}
		}
	}
	return forums;
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
