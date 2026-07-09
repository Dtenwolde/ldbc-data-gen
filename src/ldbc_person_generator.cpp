#include "ldbc_person_generator.hpp"

#include "ldbc_unicode.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <map>
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

LdbcFacebookDegreeDistribution::LdbcFacebookDegreeDistribution(const LdbcDatagenConfig &config)
    : random_percentile(0) {
	if (config.degree_distribution != "Facebook") {
		throw InvalidInputException("Only the Facebook LDBC degree distribution is currently supported");
	}
	auto path = LdbcResourcePath(LdbcResourcePath(config.resource_dir, "dictionaries"), "facebookBucket100.dat");
	std::ifstream file(path);
	if (!file.is_open()) {
		throw IOException("Could not open LDBC degree distribution file '%s'", path);
	}

	auto mean =
	    std::round(std::pow(static_cast<double>(config.num_persons),
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
	auto deletion_date = explicitly_deleted
	                         ? dates.RandomPersonDeletionDate(random_farm.Get(LdbcRandomAspect::DATE), creation_date,
	                                                          dates.SimulationEnd())
	                         : dates.NetworkCollapse();
	auto account_id = ComposePersonId(sequential_id, creation_date);
	auto main_interest =
	    dictionaries.tags.GetTagByCountry(random_farm.Get(LdbcRandomAspect::TAG_OTHER_COUNTRY),
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
	    random_farm.Get(LdbcRandomAspect::UNCORRELATED_UNIVERSITY_LOCATION), random_farm.Get(LdbcRandomAspect::TOP_UNIVERSITY),
	    random_farm.Get(LdbcRandomAspect::UNIVERSITY), country_id);
	auto random_id = random_farm.Get(LdbcRandomAspect::RANDOM).NextInt(NumericLimits<int32_t>::Maximum()) % 100;
	auto birth_year = Date::ExtractYear(LdbcDateFromEpochMs(birthday));
	auto first_name =
	    dictionaries.names.GetRandomGivenName(random_farm.Get(LdbcRandomAspect::NAME), country_id, gender == 1, birth_year);
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
	auto university_id =
	    university_location_id == -1 ? int64_t(-1) : dictionaries.universities.GetUniversityFromLocation(university_location_id);
	auto company_count = random_farm.Get(LdbcRandomAspect::EXTRA_INFO).NextInt(config.max_companies) + 1;
	auto company_probability = random_farm.Get(LdbcRandomAspect::EXTRA_INFO).NextDouble();
	unordered_map<int64_t, int64_t> companies;
	if (company_probability >= config.missing_ratio) {
		for (int32_t company_idx = 0; company_idx < company_count; company_idx++) {
			auto work_from = dates.GetWorkFromYear(random_farm.Get(LdbcRandomAspect::DATE), class_year, birthday);
			auto company = dictionaries.companies.GetRandomCompany(random_farm.Get(LdbcRandomAspect::UNCORRELATED_COMPANY),
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
	auto step_edges = static_cast<int64_t>(std::ceil(percentages[step_index] * static_cast<float>(person.max_num_knows)));
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
	auto deletion_date = explicitly_deleted
	                         ? dates.RandomKnowsDeletionDate(random_farm.Get(LdbcRandomAspect::DATE),
	                                                         person_a.deletion_date, person_b.deletion_date, creation_date)
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

	std::sort(generated_edges.begin(), generated_edges.end(), [](const LdbcKnowsEdge &left, const LdbcKnowsEdge &right) {
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
	auto language_idx = random_farm.Get(LdbcRandomAspect::LANGUAGE).NextInt(NumericCast<int32_t>(person.language_ids.size()));
	return person.language_ids[language_idx];
}

static int32_t RandomPersonInterest(LdbcRandomGeneratorFarm &random_farm, const LdbcPersonCore &person) {
	if (person.interests.empty()) {
		return -1;
	}
	auto interest_idx = random_farm.Get(LdbcRandomAspect::FORUM_INTEREST).NextInt(NumericCast<int32_t>(person.interests.size()));
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
	forum.tags = person.interests;
	RandomPersonLanguage(random_farm, person);

	for (auto &friend_entry : friends) {
		auto membership_creation_date = friend_entry.creation_date + config.delta;
		auto membership_deletion_date = std::min(forum.deletion_date, friend_entry.deletion_date);
		if (membership_deletion_date - membership_creation_date < 0) {
			continue;
		}
		forum.memberships.push_back({membership_creation_date, membership_deletion_date, false, forum.id,
		                             friend_entry.person->account_id});
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
	bool explicitly_deleted = random_farm.Get(LdbcRandomAspect::DELETION_FORUM).NextDouble() < config.prob_forum_deleted;
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
	auto interest_id = RandomPersonInterest(random_farm, moderator);
	if (interest_id == -1) {
		return false;
	}
	RandomPersonLanguage(random_farm, moderator);

	forum.creation_date = creation_date;
	forum.deletion_date = deletion_date;
	forum.explicitly_deleted = explicitly_deleted;
	forum.id = FormActivityId(config, dates, local_forum_id, creation_date, block_id);
	forum.title = ClampStringLocal("Group for " + EscapedTagName(dictionaries, interest_id) + " in " +
	                                   dictionaries.places.GetPlaceName(moderator.city_id),
	                               256);
	forum.moderator_person_id = moderator.account_id;
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
			auto friend_idx = random_farm.Get(LdbcRandomAspect::MEMBERSHIP_INDEX).NextInt(NumericCast<int32_t>(friends.size()));
			auto &friend_entry = friends[friend_idx];
			member = friend_entry.person;
		} else if (!block.empty()) {
			auto candidate_idx = random_farm.Get(LdbcRandomAspect::MEMBERSHIP_INDEX).NextInt(NumericCast<int32_t>(block.size()));
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
		auto max_member_creation = std::min(std::min(member_deletion_ceil, member->deletion_date), dates.SimulationEnd());
		if (max_member_creation <= min_member_creation) {
			num_loop++;
			continue;
		}
		auto &membership_random = random_farm.Get(LdbcRandomAspect::MEMBERSHIP_INDEX);
		auto membership_creation = dates.RandomDate(membership_random, min_member_creation, max_member_creation);
		bool membership_deleted = random_farm.Get(LdbcRandomAspect::DELETION_MEMB).NextDouble() < config.prob_memb_deleted;
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
                             const LdbcPersonDictionaries &dictionaries,
                             LdbcRandomGeneratorFarm &random_farm, const LdbcPersonCore &person,
                             const vector<LdbcActivityFriend> &friends, int64_t local_forum_id, int32_t album_number,
                             int64_t block_id, LdbcForum &forum) {
	auto min_creation = person.creation_date + config.delta;
	auto max_creation = std::min(person.deletion_date, dates.SimulationEnd());
	if (max_creation <= min_creation) {
		return false;
	}
	auto creation_date = dates.RandomDate(random_farm.Get(LdbcRandomAspect::DATE), min_creation, max_creation);
	bool explicitly_deleted = random_farm.Get(LdbcRandomAspect::DELETION_FORUM).NextDouble() < config.prob_forum_deleted;
	int64_t deletion_date;
	if (explicitly_deleted) {
		auto min_deletion = creation_date + config.delta;
		auto max_deletion = std::min(person.deletion_date, dates.SimulationEnd());
		if (max_deletion - min_creation < 0 || max_deletion <= min_deletion) {
			return false;
		}
		deletion_date = dates.RandomDate(random_farm.Get(LdbcRandomAspect::DATE), min_deletion, max_deletion);
	} else {
		deletion_date = person.deletion_date;
	}
	auto interest_id = RandomPersonInterest(random_farm, person);
	if (interest_id == -1) {
		return false;
	}
	RandomPersonLanguage(random_farm, person);
	random_farm.Get(LdbcRandomAspect::COUNTRY).NextInt(NumericCast<int32_t>(dictionaries.places.GetCountries().size()));

	forum.creation_date = creation_date;
	forum.deletion_date = deletion_date;
	forum.explicitly_deleted = explicitly_deleted;
	forum.id = FormActivityId(config, dates, local_forum_id, creation_date, block_id);
	forum.title =
	    ClampStringLocal("Album " + std::to_string(album_number) + " of " + person.first_name + " " + person.last_name, 256);
	forum.moderator_person_id = person.account_id;
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

	vector<LdbcForum> forums;
	for (int64_t block_start = 0; block_start < config.num_persons; block_start += config.block_size) {
		auto block_end = std::min<int64_t>(config.num_persons, block_start + config.block_size);
		auto block_id = block_start / config.block_size;
		LdbcRandomGeneratorFarm random_farm;
		random_farm.Reset(block_id);
		int64_t local_forum_id = 0;

		vector<const LdbcPersonCore *> block;
		for (int64_t idx = block_start; idx < block_end; idx++) {
			block.push_back(&persons[NumericCast<idx_t>(idx)]);
		}

		for (auto *person : block) {
			auto &friends = friends_by_person[person->account_id];
			if (person->deletion_date - person->creation_date + config.delta >= 0) {
				forums.push_back(CreateWallForum(config, dates, random_farm, *person, friends, local_forum_id++, block_id));
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
				if (CreateAlbumForum(config, dates, dictionaries, random_farm, *person, friends, local_forum_id, album_idx,
				                     block_id, forum)) {
					forums.push_back(std::move(forum));
				}
				local_forum_id++;
				random_farm.Get(LdbcRandomAspect::NUM_PHOTO).NextInt(config.max_num_photo_per_albums + 1);
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
