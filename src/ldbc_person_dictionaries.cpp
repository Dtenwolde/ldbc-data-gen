#include "ldbc_person_dictionaries.hpp"

#include "ldbc_unicode.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <map>
#include <set>
#include <sstream>

namespace duckdb {

namespace {

static string StripCarriageReturnLocal(string value) {
	if (!value.empty() && value.back() == '\r') {
		value.pop_back();
	}
	return value;
}

static vector<string> SplitWhitespaceLocal(const string &line) {
	vector<string> result;
	string current;
	for (auto c : line) {
		if (StringUtil::CharacterIsSpace(c)) {
			if (!current.empty()) {
				result.push_back(current);
				current.clear();
			}
		} else {
			current += c;
		}
	}
	if (!current.empty()) {
		result.push_back(current);
	}
	return result;
}

static string TrimWhitespaceLocal(const string &value) {
	idx_t start = 0;
	while (start < value.size() && StringUtil::CharacterIsSpace(value[start])) {
		start++;
	}
	idx_t end = value.size();
	while (end > start && StringUtil::CharacterIsSpace(value[end - 1])) {
		end--;
	}
	return value.substr(start, end - start);
}

static bool IsBmpOnlyUtf8(const string &value) {
	for (auto character : value) {
		auto byte = static_cast<unsigned char>(character);
		if ((byte & 0xF8U) == 0xF0U) {
			return false;
		}
	}
	return true;
}

static vector<string> SplitByDelimiterLocal(const string &line, const string &delimiter) {
	vector<string> result;
	idx_t offset = 0;
	while (true) {
		auto next = line.find(delimiter, offset);
		if (next == string::npos) {
			result.push_back(TrimWhitespaceLocal(line.substr(offset)));
			return result;
		}
		result.push_back(TrimWhitespaceLocal(line.substr(offset, next - offset)));
		offset = next + delimiter.size();
	}
}

static std::ifstream OpenDictionaryPath(const string &dictionary_dir, const string &file_name) {
	auto path = LdbcResourcePath(dictionary_dir, file_name);
	std::ifstream file(path);
	if (!file.is_open()) {
		throw IOException("Could not open LDBC dictionary file '%s'", path);
	}
	return file;
}

static int32_t JavaRoundToInt(double value) {
	return static_cast<int32_t>(std::floor(value + 0.5));
}

static int32_t ZOrderValue(int32_t x, int32_t y) {
	int32_t result = 0;
	for (int32_t bit = 7; bit >= 0; bit--) {
		result <<= 1;
		result |= (x >> bit) & 1;
		result <<= 1;
		result |= (y >> bit) & 1;
	}
	return result;
}

} // namespace

string LdbcResourcePath(const string &base, const string &path) {
	if (base.empty()) {
		throw InvalidInputException("LDBC resource path base must not be empty");
	}
	if (base.back() == '/') {
		return base + path;
	}
	return base + "/" + path;
}

LdbcBrowserDictionary::LdbcBrowserDictionary(const string &dictionary_dir) {
	auto file = OpenDictionaryPath(dictionary_dir, "browsersDic.txt");
	string line;
	double cumulative = 0.0;
	while (std::getline(file, line)) {
		line = StripCarriageReturnLocal(line);
		if (line.empty()) {
			continue;
		}
		auto separator = line.find("  ");
		if (separator == string::npos) {
			throw InvalidInputException("Malformed browsersDic.txt line: '%s'", line);
		}
		auto browser = TrimWhitespaceLocal(line.substr(0, separator));
		auto probability = TrimWhitespaceLocal(line.substr(separator + 2));
		cumulative += std::stod(probability);
		browsers.push_back(browser);
		cumulative_distribution.push_back(cumulative);
	}
}

int32_t LdbcBrowserDictionary::GetRandomBrowserId(LdbcJavaRandom &random) const {
	auto probability = random.NextDouble();
	auto position = std::lower_bound(cumulative_distribution.begin(), cumulative_distribution.end(), probability);
	if (position == cumulative_distribution.end()) {
		return NumericCast<int32_t>(cumulative_distribution.size() - 1);
	}
	return NumericCast<int32_t>(position - cumulative_distribution.begin());
}

const string &LdbcBrowserDictionary::GetName(int32_t id) const {
	if (id < 0 || static_cast<idx_t>(id) >= browsers.size()) {
		throw InternalException("LDBC browser id out of range");
	}
	return browsers[id];
}

LdbcPlaceDictionary::LdbcPlaceDictionary(const string &dictionary_dir) {
	auto locations = OpenDictionaryPath(dictionary_dir, "dicLocations.txt");
	string line;
	vector<std::pair<int32_t, int32_t>> zorder_by_country;
	while (std::getline(locations, line)) {
		line = StripCarriageReturnLocal(line);
		if (line.empty()) {
			continue;
		}
		auto columns = SplitWhitespaceLocal(line);
		if (columns.size() < 6) {
			throw InvalidInputException("Malformed dicLocations.txt line: '%s'", line);
		}
		auto country_id = NumericCast<int32_t>(countries.size());
		country_names[columns[1]] = country_id;
		country_names_by_id.push_back(columns[1]);
		place_names_by_id.push_back(columns[1]);
		countries.push_back(country_id);
		cities_by_country[country_id] = vector<int32_t>();
		cumulative_distribution.push_back(std::stof(columns[5]));
		auto longitude = std::stod(columns[3]);
		auto latitude = std::stod(columns[2]);
		auto x = (JavaRoundToInt(longitude) + 180) / 2;
		auto y = (JavaRoundToInt(latitude) + 180) / 2;
		zorder_by_country.emplace_back(country_id, ZOrderValue(x, y));
	}
	std::stable_sort(zorder_by_country.begin(), zorder_by_country.end(),
	                 [](const std::pair<int32_t, int32_t> &left, const std::pair<int32_t, int32_t> &right) {
		                 return left.second < right.second;
	                 });
	country_zorder_by_id.resize(countries.size());
	for (idx_t zorder_id = 0; zorder_id < zorder_by_country.size(); zorder_id++) {
		country_zorder_by_id[zorder_by_country[zorder_id].first] = NumericCast<int32_t>(zorder_id);
		place_id_by_zorder.push_back(zorder_by_country[zorder_id].first);
	}

	auto cities = OpenDictionaryPath(dictionary_dir, "citiesByCountry.txt");
	int32_t next_place_id = NumericCast<int32_t>(countries.size());
	while (std::getline(cities, line)) {
		line = StripCarriageReturnLocal(line);
		if (line.empty()) {
			continue;
		}
		auto columns = SplitWhitespaceLocal(line);
		if (columns.size() < 2) {
			throw InvalidInputException("Malformed citiesByCountry.txt line: '%s'", line);
		}
		auto country = country_names.find(columns[0]);
		if (country == country_names.end() || city_names.find(columns[1]) != city_names.end()) {
			continue;
		}
		auto city_id = next_place_id++;
		city_names[columns[1]] = city_id;
		if (static_cast<idx_t>(city_id) >= place_names_by_id.size()) {
			place_names_by_id.resize(NumericCast<idx_t>(city_id) + 1);
		}
		place_names_by_id[city_id] = columns[1];
		cities_by_country[country->second].push_back(city_id);
	}
}

int32_t LdbcPlaceDictionary::GetCountryForPerson(LdbcJavaRandom &random) const {
	auto probability = random.NextFloat();
	auto position = std::lower_bound(cumulative_distribution.begin(), cumulative_distribution.end(), probability);
	if (position == cumulative_distribution.end()) {
		return NumericCast<int32_t>(cumulative_distribution.size() - 1);
	}
	return NumericCast<int32_t>(position - cumulative_distribution.begin());
}

int32_t LdbcPlaceDictionary::GetRandomCity(LdbcJavaRandom &random, int32_t country_id) const {
	auto entry = cities_by_country.find(country_id);
	if (entry == cities_by_country.end() || entry->second.empty()) {
		return -1;
	}
	auto city_index = random.NextInt(NumericCast<int32_t>(entry->second.size()));
	return entry->second[city_index];
}

const vector<int32_t> &LdbcPlaceDictionary::GetCountries() const {
	return countries;
}

const string &LdbcPlaceDictionary::GetCountryName(int32_t country_id) const {
	if (country_id < 0 || static_cast<idx_t>(country_id) >= country_names_by_id.size()) {
		throw InternalException("LDBC country id out of range");
	}
	return country_names_by_id[country_id];
}

const string &LdbcPlaceDictionary::GetPlaceName(int32_t place_id) const {
	if (place_id < 0 || static_cast<idx_t>(place_id) >= place_names_by_id.size()) {
		throw InternalException("LDBC place id out of range");
	}
	return place_names_by_id[place_id];
}

int32_t LdbcPlaceDictionary::GetCountryId(const string &country_name) const {
	auto entry = country_names.find(country_name);
	if (entry == country_names.end()) {
		return -1;
	}
	return entry->second;
}

int32_t LdbcPlaceDictionary::GetCityId(const string &city_name) const {
	auto entry = city_names.find(city_name);
	if (entry == city_names.end()) {
		return -1;
	}
	return entry->second;
}

int32_t LdbcPlaceDictionary::GetZOrderId(int32_t place_id) const {
	if (place_id < 0 || static_cast<idx_t>(place_id) >= country_zorder_by_id.size()) {
		throw InvalidInputException("LDBC place id %d does not have a country z-order id", place_id);
	}
	return country_zorder_by_id[place_id];
}

int32_t LdbcPlaceDictionary::GetPlaceIdFromZOrder(int32_t zorder_id) const {
	if (zorder_id < 0 || static_cast<idx_t>(zorder_id) >= place_id_by_zorder.size()) {
		throw InvalidInputException("LDBC z-order id %d is out of range", zorder_id);
	}
	return place_id_by_zorder[zorder_id];
}

LdbcIPAddressDictionary::LdbcIPAddressDictionary(const string &resource_dir, const LdbcPlaceDictionary &places) {
	unordered_map<string, string> country_abbreviations;
	auto mapping = OpenDictionaryPath(LdbcResourcePath(resource_dir, "dictionaries"), "countryAbbrMapping.txt");
	string line;
	while (std::getline(mapping, line)) {
		line = StripCarriageReturnLocal(line);
		if (line.empty()) {
			continue;
		}
		auto separator = line.find("   ");
		if (separator == string::npos) {
			throw InvalidInputException("Malformed countryAbbrMapping.txt line: '%s'", line);
		}
		auto abbreviation = TrimWhitespaceLocal(line.substr(0, separator));
		auto country = TrimWhitespaceLocal(line.substr(separator + 3));
		std::replace(country.begin(), country.end(), ' ', '_');
		country_abbreviations[country] = abbreviation;
	}

	for (auto country_id : places.GetCountries()) {
		auto country_name = places.GetCountryName(country_id);
		auto abbreviation = country_abbreviations.find(country_name);
		if (abbreviation == country_abbreviations.end()) {
			throw InvalidInputException("Missing IP country abbreviation for '%s'", country_name);
		}

		auto zone_path =
		    LdbcResourcePath(LdbcResourcePath(resource_dir, "ipaddrByCountries"), abbreviation->second + ".zone");
		std::ifstream zone_file(zone_path);
		if (!zone_file.is_open()) {
			throw IOException("Could not open LDBC IP zone file '%s'", zone_path);
		}

		auto &networks = ips_by_country[country_id];
		idx_t zone_count = 0;
		while (zone_count < 100 && std::getline(zone_file, line)) {
			line = StripCarriageReturnLocal(line);
			if (line.empty()) {
				continue;
			}
			std::replace(line.begin(), line.end(), '.', ' ');
			std::replace(line.begin(), line.end(), '/', ' ');
			std::stringstream parser(line);
			uint32_t byte1;
			uint32_t byte2;
			uint32_t byte3;
			uint32_t byte4;
			uint32_t mask_bits;
			parser >> byte1 >> byte2 >> byte3 >> byte4 >> mask_bits;
			if (parser.fail() || mask_bits > 32) {
				throw InvalidInputException("Malformed IP zone line in '%s': '%s'", zone_path, line);
			}
			auto ip = ((byte1 & 0xFFU) << 24U) | ((byte2 & 0xFFU) << 16U) | ((byte3 & 0xFFU) << 8U) | (byte4 & 0xFFU);
			auto mask = mask_bits == 0 ? 0U : (0xFFFFFFFFU << (32U - mask_bits));
			networks.push_back({ip & mask, mask});
			zone_count++;
		}
	}
}

string LdbcIPAddressDictionary::GetIP(LdbcJavaRandom &random, int32_t country_id) const {
	auto entry = ips_by_country.find(country_id);
	if (entry == ips_by_country.end() || entry->second.empty()) {
		throw InvalidInputException("No IP ranges loaded for LDBC country id %d", country_id);
	}
	auto network = entry->second[random.NextInt(NumericCast<int32_t>(entry->second.size()))];
	auto random_bits = static_cast<uint32_t>(random.NextInt());
	auto ip = network.network | ((~network.mask) & random_bits);
	return std::to_string((ip >> 24U) & 0xFFU) + "." + std::to_string((ip >> 16U) & 0xFFU) + "." +
	       std::to_string((ip >> 8U) & 0xFFU) + "." + std::to_string(ip & 0xFFU);
}

LdbcLanguageDictionary::LdbcLanguageDictionary(const string &dictionary_dir, const LdbcPlaceDictionary &places,
                                               double prob_english, double prob_second_lang)
    : prob_english(prob_english), prob_second_lang(prob_second_lang) {
	for (auto country_id : places.GetCountries()) {
		official_languages_by_country[country_id] = vector<int32_t>();
		languages_by_country[country_id] = vector<int32_t>();
	}

	auto file = OpenDictionaryPath(dictionary_dir, "languagesByCountry.txt");
	string line;
	while (std::getline(file, line)) {
		line = StripCarriageReturnLocal(line);
		if (line.empty()) {
			continue;
		}

		vector<string> columns;
		idx_t offset = 0;
		while (true) {
			auto next = line.find("  ", offset);
			if (next == string::npos) {
				columns.push_back(TrimWhitespaceLocal(line.substr(offset)));
				break;
			}
			columns.push_back(TrimWhitespaceLocal(line.substr(offset, next - offset)));
			offset = next + 2;
		}
		if (columns.size() < 2) {
			throw InvalidInputException("Malformed languagesByCountry.txt line: '%s'", line);
		}
		auto country_id = places.GetCountryId(columns[0]);
		if (country_id == -1) {
			continue;
		}

		for (idx_t column_idx = 1; column_idx < columns.size(); column_idx++) {
			auto language_columns = SplitWhitespaceLocal(columns[column_idx]);
			if (language_columns.empty()) {
				continue;
			}
			auto language = language_columns[0];
			auto language_id =
			    NumericCast<int32_t>(std::find(languages.begin(), languages.end(), language) - languages.begin());
			if (language_id == NumericCast<int32_t>(languages.size())) {
				languages.push_back(language);
			}
			if (language_columns.size() == 3) {
				official_languages_by_country[country_id].push_back(language_id);
			}
			languages_by_country[country_id].push_back(language_id);
		}
	}
}

vector<int32_t> LdbcLanguageDictionary::GetLanguages(LdbcJavaRandom &random, int32_t country_id) const {
	vector<int32_t> result;
	auto official = official_languages_by_country.find(country_id);
	auto all = languages_by_country.find(country_id);
	if (all == languages_by_country.end() || all->second.empty()) {
		return result;
	}
	if (official != official_languages_by_country.end() && !official->second.empty()) {
		result.push_back(official->second[random.NextInt(NumericCast<int32_t>(official->second.size()))]);
	} else {
		result.push_back(all->second[random.NextInt(NumericCast<int32_t>(all->second.size()))]);
	}
	if (random.NextDouble() < prob_second_lang) {
		auto language_id = all->second[random.NextInt(NumericCast<int32_t>(all->second.size()))];
		if (std::find(result.begin(), result.end(), language_id) == result.end()) {
			result.push_back(language_id);
		}
	}
	return result;
}

int32_t LdbcLanguageDictionary::GetInternationalLanguage(LdbcJavaRandom &random) const {
	if (random.NextDouble() < prob_english) {
		auto entry = std::find(languages.begin(), languages.end(), "en");
		if (entry != languages.end()) {
			return NumericCast<int32_t>(entry - languages.begin());
		}
	}
	return -1;
}

string LdbcLanguageDictionary::GetLanguageName(int32_t language_id) const {
	if (language_id < 0 || static_cast<idx_t>(language_id) >= languages.size()) {
		return "";
	}
	return languages[language_id];
}

string LdbcLanguageDictionary::GetLanguageList(LdbcJavaRandom &random, int32_t country_id) const {
	auto language_ids = GetLanguages(random, country_id);
	auto international_language = GetInternationalLanguage(random);
	if (international_language != -1 &&
	    std::find(language_ids.begin(), language_ids.end(), international_language) == language_ids.end()) {
		language_ids.push_back(international_language);
	}

	string result;
	for (idx_t idx = 0; idx < language_ids.size(); idx++) {
		if (idx > 0) {
			result += ";";
		}
		result += GetLanguageName(language_ids[idx]);
	}
	return result;
}

LdbcNamesDictionary::LdbcNamesDictionary(const string &dictionary_dir, const LdbcPlaceDictionary &places) {
	for (auto country_id : places.GetCountries()) {
		surnames_by_country[country_id] = vector<string>();
		for (idx_t period = 0; period < BIRTH_YEAR_PERIODS; period++) {
			given_names_male[period][country_id] = vector<string>();
			given_names_female[period][country_id] = vector<string>();
		}
	}

	auto surnames = OpenDictionaryPath(dictionary_dir, "surnameByCountryBirthPlace.txt.freq.sort");
	string line;
	while (std::getline(surnames, line)) {
		line = StripCarriageReturnLocal(line);
		if (line.empty()) {
			continue;
		}
		auto columns = SplitWhitespaceLocal(line);
		auto first_comma = line.find(',');
		auto second_comma = first_comma == string::npos ? string::npos : line.find(',', first_comma + 1);
		if (second_comma == string::npos) {
			throw InvalidInputException("Malformed surnameByCountryBirthPlace.txt.freq.sort line: '%s'", line);
		}
		auto country = line.substr(first_comma + 1, second_comma - first_comma - 1);
		auto country_id = places.GetCountryId(country);
		if (country_id == -1) {
			continue;
		}
		surnames_by_country[country_id].push_back(TrimWhitespaceLocal(line.substr(second_comma + 1)));
	}

	auto given_names = OpenDictionaryPath(dictionary_dir, "givennameByCountryBirthPlace.txt.freq.full");
	while (std::getline(given_names, line)) {
		line = StripCarriageReturnLocal(line);
		if (line.empty()) {
			continue;
		}
		vector<string> columns;
		idx_t offset = 0;
		while (true) {
			auto next = line.find("  ", offset);
			if (next == string::npos) {
				columns.push_back(TrimWhitespaceLocal(line.substr(offset)));
				break;
			}
			columns.push_back(TrimWhitespaceLocal(line.substr(offset, next - offset)));
			offset = next + 2;
		}
		if (columns.size() < 4) {
			throw InvalidInputException("Malformed givennameByCountryBirthPlace.txt.freq.full line: '%s'", line);
		}
		auto country_id = places.GetCountryId(columns[0]);
		if (country_id == -1) {
			continue;
		}
		auto gender = std::stoi(columns[2]);
		auto period = NumericCast<idx_t>(std::stoi(columns[3]));
		if (period >= BIRTH_YEAR_PERIODS) {
			throw InvalidInputException("Unsupported name birth-year period in line: '%s'", line);
		}
		if (gender == 0) {
			given_names_male[period][country_id].push_back(columns[1]);
		} else {
			given_names_female[period][country_id].push_back(columns[1]);
		}
	}
}

int32_t LdbcNamesDictionary::GetGeometricRandomIndex(LdbcJavaRandom &random, idx_t name_count) const {
	if (name_count == 0) {
		return -1;
	}
	constexpr int32_t TOP_N = 30;
	constexpr double GEOMETRIC_P = 0.2;
	auto probability = random.NextDouble();
	auto rank = static_cast<int32_t>(std::ceil(std::log1p(-probability) / std::log1p(-GEOMETRIC_P) - 1.0));
	if (rank < 0) {
		rank = 0;
	}

	if (rank < TOP_N) {
		if (NumericCast<int32_t>(name_count) > rank) {
			return rank;
		}
		return random.NextInt(NumericCast<int32_t>(name_count));
	}

	if (NumericCast<int32_t>(name_count) > rank) {
		return TOP_N + random.NextInt(NumericCast<int32_t>(name_count) - TOP_N);
	}
	return random.NextInt(NumericCast<int32_t>(name_count));
}

string LdbcNamesDictionary::GetRandomGivenName(LdbcJavaRandom &random, int32_t country_id, bool is_male,
                                               int32_t birth_year) const {
	auto period = birth_year < 1985 ? idx_t(0) : idx_t(1);
	auto &target = is_male ? given_names_male : given_names_female;
	auto first_period = target[0].find(country_id);
	if (first_period == target[0].end() || first_period->second.empty()) {
		return "";
	}
	auto name_id = GetGeometricRandomIndex(random, first_period->second.size());
	if (name_id >= 30) {
		return first_period->second[name_id];
	}
	auto selected_period = target[period].find(country_id);
	if (selected_period == target[period].end() || static_cast<idx_t>(name_id) >= selected_period->second.size()) {
		return "";
	}
	return selected_period->second[name_id];
}

string LdbcNamesDictionary::GetRandomSurname(LdbcJavaRandom &random, int32_t country_id) const {
	auto entry = surnames_by_country.find(country_id);
	if (entry == surnames_by_country.end() || entry->second.empty()) {
		return "";
	}
	auto surname_id = GetGeometricRandomIndex(random, entry->second.size());
	return entry->second[surname_id];
}

LdbcEmailDictionary::LdbcEmailDictionary(const string &dictionary_dir) {
	auto file = OpenDictionaryPath(dictionary_dir, "email.txt");
	string line;
	double cumulative = 0.0;
	while (std::getline(file, line)) {
		line = StripCarriageReturnLocal(line);
		if (line.empty()) {
			continue;
		}
		auto columns = SplitWhitespaceLocal(line);
		if (columns.empty()) {
			continue;
		}
		emails.push_back(columns[0]);
		if (columns.size() == 2) {
			cumulative += std::stod(columns[1]);
			cumulative_distribution.push_back(cumulative);
		}
	}
	if (emails.empty() || cumulative_distribution.empty()) {
		throw InvalidInputException("LDBC email dictionary must contain weighted email domains");
	}
}

string LdbcEmailDictionary::GetRandomEmail(LdbcJavaRandom &random_top, LdbcJavaRandom &random_email) const {
	auto min_idx = idx_t(0);
	auto max_idx = cumulative_distribution.size() - 1;
	auto probability = random_top.NextDouble();
	if (probability > cumulative_distribution[max_idx]) {
		auto tail_size = NumericCast<int32_t>(emails.size() - cumulative_distribution.size());
		auto email_idx = random_email.NextInt(tail_size) + NumericCast<int32_t>(cumulative_distribution.size());
		return emails[email_idx];
	}
	if (probability < cumulative_distribution[min_idx]) {
		return emails[min_idx];
	}

	while (max_idx - min_idx > 1) {
		auto middle = min_idx + (max_idx - min_idx) / 2;
		if (probability > cumulative_distribution[middle]) {
			min_idx = middle;
		} else {
			max_idx = middle;
		}
	}
	return emails[max_idx];
}

LdbcPersonDeleteDistribution::LdbcPersonDeleteDistribution(const string &dictionary_dir) {
	auto file = OpenDictionaryPath(dictionary_dir, "personDelete.txt");
	string line;
	while (std::getline(file, line)) {
		line = StripCarriageReturnLocal(line);
		if (!line.empty()) {
			distribution.push_back(std::stod(line));
		}
	}
	if (distribution.empty()) {
		throw InvalidInputException("LDBC personDelete.txt must not be empty");
	}
}

bool LdbcPersonDeleteDistribution::IsDeleted(LdbcJavaRandom &random, int64_t max_knows) const {
	auto index = NumericCast<idx_t>(std::max<int64_t>(0, max_knows));
	if (index >= distribution.size()) {
		index = distribution.size() - 1;
	}
	return random.NextDouble() < distribution[index];
}

LdbcTagDictionary::LdbcTagDictionary(const string &dictionary_dir, idx_t country_count, double tag_country_corr_prob)
    : tag_country_corr_prob(tag_country_corr_prob), tags_by_country(country_count),
      cumulative_distribution_by_country(country_count) {
	auto tags_file = OpenDictionaryPath(dictionary_dir, "tags.txt");
	string line;
	while (std::getline(tags_file, line)) {
		line = StripCarriageReturnLocal(line);
		if (line.empty()) {
			continue;
		}
		auto columns = SplitWhitespaceLocal(line);
		if (columns.size() < 3) {
			throw InvalidInputException("Malformed tags.txt line: '%s'", line);
		}
		auto tag_id = NumericCast<idx_t>(std::stoi(columns[0]));
		if (tag_id >= tag_names.size()) {
			tag_names.resize(tag_id + 1);
		}
		tag_names[tag_id] = columns[2];
	}

	auto file = OpenDictionaryPath(dictionary_dir, "popularTagByCountry.txt");
	while (std::getline(file, line)) {
		line = StripCarriageReturnLocal(line);
		if (line.empty()) {
			continue;
		}
		auto columns = SplitWhitespaceLocal(line);
		if (columns.size() < 3) {
			throw InvalidInputException("Malformed popularTagByCountry.txt line: '%s'", line);
		}
		auto country_id = NumericCast<idx_t>(std::stoi(columns[0]));
		if (country_id >= country_count) {
			throw InvalidInputException("popularTagByCountry.txt country id out of range: '%s'", line);
		}
		tags_by_country[country_id].push_back(std::stoi(columns[1]));
		cumulative_distribution_by_country[country_id].push_back(std::stod(columns[2]));
	}
}

int32_t LdbcTagDictionary::GetTagByCountry(LdbcJavaRandom &random_tag_other_country,
                                           LdbcJavaRandom &random_tag_country_prob, int32_t country_id) const {
	if (country_id < 0 || static_cast<idx_t>(country_id) >= tags_by_country.size()) {
		throw InvalidInputException("LDBC tag country id out of range");
	}
	if (tags_by_country[country_id].empty() || random_tag_other_country.NextDouble() > tag_country_corr_prob) {
		do {
			country_id = random_tag_other_country.NextInt(NumericCast<int32_t>(tags_by_country.size()));
		} while (tags_by_country[country_id].empty());
	}

	auto random_distance = random_tag_country_prob.NextDouble();
	idx_t lower_bound = 0;
	idx_t upper_bound = tags_by_country[country_id].size();
	idx_t current_idx = (upper_bound + lower_bound) / 2;
	while (upper_bound > lower_bound + 1) {
		if (cumulative_distribution_by_country[country_id][current_idx] > random_distance) {
			upper_bound = current_idx;
		} else {
			lower_bound = current_idx;
		}
		current_idx = (upper_bound + lower_bound) / 2;
	}
	return tags_by_country[country_id][current_idx];
}

vector<int32_t> LdbcTagDictionary::GetRandomTags(LdbcJavaRandom &random, int32_t count) const {
	vector<int32_t> result;
	result.reserve(NumericCast<idx_t>(count));
	while (NumericCast<int32_t>(result.size()) < count) {
		auto country_id = random.NextInt(NumericCast<int32_t>(tags_by_country.size()));
		auto &tags = tags_by_country[country_id];
		if (tags.empty()) {
			continue;
		}
		result.push_back(tags[random.NextInt(NumericCast<int32_t>(tags.size()))]);
	}
	return result;
}

const string &LdbcTagDictionary::GetName(int32_t tag_id) const {
	if (tag_id < 0 || static_cast<idx_t>(tag_id) >= tag_names.size() || tag_names[tag_id].empty()) {
		throw InternalException("LDBC tag id out of range");
	}
	return tag_names[tag_id];
}

LdbcTagTextDictionary::LdbcTagTextDictionary(const string &dictionary_dir, const LdbcTagDictionary &tags) : tags(tags) {
	auto file = OpenDictionaryPath(dictionary_dir, "tagText.txt");
	string line;
	while (std::getline(file, line)) {
		line = StripCarriageReturnLocal(line);
		if (line.empty()) {
			continue;
		}
		auto separator = line.find("  ");
		if (separator == string::npos) {
			throw InvalidInputException("Malformed tagText.txt line: '%s'", line);
		}
		auto tag_id = NumericCast<idx_t>(std::stoi(line.substr(0, separator)));
		auto content_start = separator + 2;
		auto content_end = line.find("  ", content_start);
		auto content = content_end == string::npos ? line.substr(content_start)
		                                           : line.substr(content_start, content_end - content_start);
		if (tag_id >= tag_text.size()) {
			tag_text.resize(tag_id + 1);
		}
		tag_text[tag_id] = content;
	}
	tag_text_lengths.resize(tag_text.size(), 0);
	tag_text_bmp_only.resize(tag_text.size(), true);
	tag_prefixes.resize(tag_text.size());
	tag_prefix_lengths.resize(tag_text.size(), 0);
	for (idx_t tag_id = 0; tag_id < tag_text.size(); tag_id++) {
		if (tag_text[tag_id].empty()) {
			continue;
		}
		tag_text_lengths[tag_id] = LdbcJavaStringLength(tag_text[tag_id]);
		tag_text_bmp_only[tag_id] = IsBmpOnlyUtf8(tag_text[tag_id]);
		auto tag_name = tags.GetName(NumericCast<int32_t>(tag_id));
		std::replace(tag_name.begin(), tag_name.end(), '_', ' ');
		string escaped_tag_name;
		for (auto character : tag_name) {
			if (character == '"') {
				escaped_tag_name += "\\\"";
			} else {
				escaped_tag_name += character;
			}
		}
		tag_prefixes[tag_id] = "About " + escaped_tag_name + ", ";
		tag_prefix_lengths[tag_id] = LdbcJavaStringLength(tag_prefixes[tag_id]);
	}
}

int32_t LdbcTagTextDictionary::GetRandomTextSize(LdbcJavaRandom &random_text_size, LdbcJavaRandom &random_reduced_text,
                                                 int32_t min_size, int32_t max_size, double reduced_text_ratio) const {
	if (random_reduced_text.NextDouble() > reduced_text_ratio) {
		return random_text_size.NextInt(max_size - min_size) + min_size;
	}
	return random_text_size.NextInt((max_size >> 1) - min_size) + min_size;
}

int32_t LdbcTagTextDictionary::GetRandomLargeTextSize(LdbcJavaRandom &random_text_size, int32_t min_size,
                                                      int32_t max_size) const {
	return random_text_size.NextInt(max_size - min_size) + min_size;
}

string LdbcTagTextDictionary::GenerateText(LdbcJavaRandom &random_text_size, const vector<int32_t> &tag_ids,
                                           int32_t text_size) const {
	if (tag_ids.empty()) {
		return "";
	}

	string result;
	result.reserve(NumericCast<idx_t>(text_size) + 64);
	int32_t result_length = 0;
	auto text_size_per_tag = static_cast<int32_t>(std::ceil(text_size / static_cast<double>(tag_ids.size())));
	while (result_length < text_size) {
		for (auto tag_id : tag_ids) {
			if (result_length >= text_size) {
				break;
			}
			if (tag_id < 0 || static_cast<idx_t>(tag_id) >= tag_text.size() || tag_text[tag_id].empty()) {
				throw InternalException("LDBC tag text id out of range");
			}
			auto tag_idx = NumericCast<idx_t>(tag_id);
			auto &content = tag_text[tag_idx];
			auto this_tag_text_size = std::min<int32_t>(text_size_per_tag, text_size - result_length);
			auto &prefix = tag_prefixes[tag_idx];
			auto prefix_length = tag_prefix_lengths[tag_idx];
			this_tag_text_size += prefix_length;
			auto content_length = tag_text_lengths[tag_idx];
			if (this_tag_text_size >= content_length) {
				result += content;
				result_length += content_length;
			} else {
				auto starting_pos = random_text_size.NextInt(content_length - this_tag_text_size + prefix_length);
				auto fragment = LdbcJavaSubstring(content, starting_pos, this_tag_text_size - prefix_length);
				result += prefix;
				result += fragment;
				result_length += tag_text_bmp_only[tag_idx] ? this_tag_text_size
				                                            : prefix_length + LdbcJavaStringLength(fragment);
			}
		}
	}

	if (!result.empty() && result.back() != '.') {
		result += ".";
		result_length++;
	}
	if (result_length < text_size - 1) {
		result += " ";
		result_length++;
	}
	if (result_length > text_size) {
		result = LdbcJavaSubstring(result, 0, text_size - 1);
	}
	return result.find('|') == string::npos ? result : StringUtil::Replace(result, "|", " ");
}

LdbcTagMatrix::LdbcTagMatrix(const string &dictionary_dir) {
	auto file = OpenDictionaryPath(dictionary_dir, "tagMatrix.txt");
	string line;
	while (std::getline(file, line)) {
		line = StripCarriageReturnLocal(line);
		if (line.empty()) {
			continue;
		}
		auto columns = SplitWhitespaceLocal(line);
		if (columns.size() < 3) {
			throw InvalidInputException("Malformed tagMatrix.txt line: '%s'", line);
		}
		auto celebrity_id = std::stoi(columns[0]);
		auto topic_id = std::stoi(columns[1]);
		auto cumulative = std::stod(columns[2]);
		related_tags[celebrity_id].push_back(topic_id);
		cumulative_distribution[celebrity_id].push_back(cumulative);
	}
	std::map<int32_t, bool> sorted_tags;
	for (auto &entry : related_tags) {
		sorted_tags[entry.first] = true;
	}
	for (auto &entry : sorted_tags) {
		non_zero_tags.push_back(entry.first);
	}
}

vector<int32_t> LdbcTagMatrix::GetSetOfTags(LdbcJavaRandom &random_topic, LdbcJavaRandom &random_tag,
                                            int32_t popular_tag_id, int32_t tag_count) const {
	std::set<int32_t> result_tags;
	result_tags.insert(popular_tag_id);
	while (NumericCast<int32_t>(result_tags.size()) < tag_count) {
		auto tag_id = popular_tag_id;
		auto related = related_tags.find(tag_id);
		if (related == related_tags.end()) {
			tag_id = non_zero_tags[random_tag.NextInt(NumericCast<int32_t>(non_zero_tags.size()))];
			related = related_tags.find(tag_id);
		}
		auto cumulative = cumulative_distribution.find(tag_id);
		auto random_distance = random_tag.NextDouble();
		idx_t lower_bound = 0;
		idx_t upper_bound = related->second.size();
		idx_t mid_point = (upper_bound + lower_bound) / 2;
		while (upper_bound > lower_bound + 1) {
			if (cumulative->second[mid_point] > random_distance) {
				upper_bound = mid_point;
			} else {
				lower_bound = mid_point;
			}
			mid_point = (upper_bound + lower_bound) / 2;
		}
		result_tags.insert(related->second[mid_point]);
	}
	return vector<int32_t>(result_tags.begin(), result_tags.end());
}

int32_t LdbcTagMatrix::GetRandomRelated(LdbcJavaRandom &random, int32_t tag_id) const {
	auto related = related_tags.find(tag_id);
	if (related == related_tags.end()) {
		tag_id = non_zero_tags[random.NextInt(NumericCast<int32_t>(non_zero_tags.size()))];
		related = related_tags.find(tag_id);
	}
	return related->second[random.NextInt(NumericCast<int32_t>(related->second.size()))];
}

LdbcCompanyDictionary::LdbcCompanyDictionary(const string &dictionary_dir, const LdbcPlaceDictionary &places,
                                             double prob_uncorrelated_company)
    : places(places), prob_uncorrelated_company(prob_uncorrelated_company),
      companies_by_country(places.GetCountries().size()) {
	auto file = OpenDictionaryPath(dictionary_dir, "companiesByCountry.txt");
	string line;
	while (std::getline(file, line)) {
		line = StripCarriageReturnLocal(line);
		if (line.empty()) {
			continue;
		}
		auto columns = SplitByDelimiterLocal(line, "  ");
		if (columns.size() < 2) {
			throw InvalidInputException("Malformed companiesByCountry.txt line: '%s'", line);
		}
		auto country_id = places.GetCountryId(columns[0]);
		if (country_id != -1) {
			companies_by_country[country_id].push_back(NumericCast<int64_t>(company_count));
			company_count++;
		}
	}
}

int64_t LdbcCompanyDictionary::GetRandomCompany(LdbcJavaRandom &random_uncorrelated_company,
                                                LdbcJavaRandom &random_uncorrelated_location,
                                                LdbcJavaRandom &random_company, int32_t country_id) const {
	auto location_id = country_id;
	auto &countries = places.GetCountries();
	if (random_uncorrelated_company.NextDouble() <= prob_uncorrelated_company) {
		location_id = countries[random_uncorrelated_location.NextInt(NumericCast<int32_t>(countries.size()))];
	}
	while (companies_by_country[location_id].empty()) {
		location_id = countries[random_uncorrelated_location.NextInt(NumericCast<int32_t>(countries.size()))];
	}
	auto company_idx = random_company.NextInt(NumericCast<int32_t>(companies_by_country[location_id].size()));
	return companies_by_country[location_id][company_idx];
}

idx_t LdbcCompanyDictionary::GetCompanyCount() const {
	return company_count;
}

LdbcUniversityDictionary::LdbcUniversityDictionary(const string &dictionary_dir, const LdbcPlaceDictionary &places,
                                                   double prob_uncorrelated_university, double prob_top_university,
                                                   int64_t start_index)
    : places(places), prob_uncorrelated_university(prob_uncorrelated_university),
      prob_top_university(prob_top_university), start_index(start_index),
      universities_by_country(places.GetCountries().size()) {
	auto file = OpenDictionaryPath(dictionary_dir, "universities.txt");
	string line;
	int64_t next_university_id = start_index;
	while (std::getline(file, line)) {
		line = StripCarriageReturnLocal(line);
		if (line.empty()) {
			continue;
		}
		auto columns = SplitByDelimiterLocal(line, "  ");
		if (columns.size() < 3) {
			throw InvalidInputException("Malformed universities.txt line: '%s'", line);
		}
		auto country_id = places.GetCountryId(columns[0]);
		auto city_id = places.GetCityId(columns[2]);
		if (country_id != -1 && city_id != -1) {
			universities_by_country[country_id].push_back(next_university_id);
			next_university_id++;
		}
	}
}

int32_t LdbcUniversityDictionary::GetRandomUniversityLocation(LdbcJavaRandom &random_uncorrelated_university,
                                                              LdbcJavaRandom &random_uncorrelated_location,
                                                              LdbcJavaRandom &random_top_university,
                                                              LdbcJavaRandom &random_university,
                                                              int32_t country_id) const {
	auto probability = random_uncorrelated_university.NextDouble();
	auto &countries = places.GetCountries();
	if (random_uncorrelated_university.NextDouble() <= prob_uncorrelated_university) {
		country_id = countries[random_uncorrelated_location.NextInt(NumericCast<int32_t>(countries.size()))];
	}
	while (universities_by_country[country_id].empty()) {
		country_id = countries[random_uncorrelated_location.NextInt(NumericCast<int32_t>(countries.size()))];
	}
	auto range = universities_by_country[country_id].size();
	if (probability > prob_uncorrelated_university && random_top_university.NextDouble() < prob_top_university) {
		range = std::min<idx_t>(range, 10);
	}
	auto random_university_idx = random_university.NextInt(NumericCast<int32_t>(range));
	auto zorder_location = places.GetZOrderId(country_id);
	return (zorder_location << 24) | (random_university_idx << 12);
}

int64_t LdbcUniversityDictionary::GetUniversityFromLocation(int32_t university_location) const {
	auto zorder_location_id = university_location >> 24;
	auto university_id = (university_location >> 12) & 0x0FFF;
	auto location_id = places.GetPlaceIdFromZOrder(zorder_location_id);
	return universities_by_country[location_id][university_id];
}

LdbcPersonDictionaries::LdbcPersonDictionaries(const string &resource_dir, double prob_english, double prob_second_lang,
                                               double tag_country_corr_prob, double prob_uncorrelated_company,
                                               double prob_uncorrelated_university, double prob_top_university)
    : browsers(LdbcResourcePath(resource_dir, "dictionaries")), places(LdbcResourcePath(resource_dir, "dictionaries")),
      ips(resource_dir, places),
      languages(LdbcResourcePath(resource_dir, "dictionaries"), places, prob_english, prob_second_lang),
      names(LdbcResourcePath(resource_dir, "dictionaries"), places),
      emails(LdbcResourcePath(resource_dir, "dictionaries")),
      person_deletes(LdbcResourcePath(resource_dir, "dictionaries")),
      tags(LdbcResourcePath(resource_dir, "dictionaries"), places.GetCountries().size(), tag_country_corr_prob),
      tag_text(LdbcResourcePath(resource_dir, "dictionaries"), tags),
      tag_matrix(LdbcResourcePath(resource_dir, "dictionaries")),
      companies(LdbcResourcePath(resource_dir, "dictionaries"), places, prob_uncorrelated_company),
      universities(LdbcResourcePath(resource_dir, "dictionaries"), places, prob_uncorrelated_university,
                   prob_top_university, NumericCast<int64_t>(companies.GetCompanyCount())) {
}

} // namespace duckdb
