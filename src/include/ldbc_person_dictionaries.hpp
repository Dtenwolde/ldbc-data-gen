#pragma once

#include "ldbc_java_random.hpp"

#include "duckdb/common/common.hpp"

#include <array>
#include <unordered_map>

namespace duckdb {

class LdbcBrowserDictionary {
public:
	explicit LdbcBrowserDictionary(const string &dictionary_dir);

	int32_t GetRandomBrowserId(LdbcJavaRandom &random) const;
	const string &GetName(int32_t id) const;

private:
	vector<string> browsers;
	vector<double> cumulative_distribution;
};

class LdbcPlaceDictionary {
public:
	explicit LdbcPlaceDictionary(const string &dictionary_dir);

	int32_t GetCountryForPerson(LdbcJavaRandom &random) const;
	int32_t GetRandomCity(LdbcJavaRandom &random, int32_t country_id) const;
	const vector<int32_t> &GetCountries() const;
	const string &GetCountryName(int32_t country_id) const;
	int32_t GetCountryId(const string &country_name) const;

private:
	vector<float> cumulative_distribution;
	vector<int32_t> countries;
	vector<string> country_names_by_id;
	unordered_map<string, int32_t> country_names;
	unordered_map<int32_t, vector<int32_t>> cities_by_country;
};

class LdbcIPAddressDictionary {
public:
	LdbcIPAddressDictionary(const string &resource_dir, const LdbcPlaceDictionary &places);

	string GetIP(LdbcJavaRandom &random, int32_t country_id) const;

private:
	struct Network {
		uint32_t network;
		uint32_t mask;
	};

	unordered_map<int32_t, vector<Network>> ips_by_country;
};

class LdbcLanguageDictionary {
public:
	LdbcLanguageDictionary(const string &dictionary_dir, const LdbcPlaceDictionary &places, double prob_english,
	                       double prob_second_lang);

	vector<int32_t> GetLanguages(LdbcJavaRandom &random, int32_t country_id) const;
	int32_t GetInternationalLanguage(LdbcJavaRandom &random) const;
	string GetLanguageName(int32_t language_id) const;
	string GetLanguageList(LdbcJavaRandom &random, int32_t country_id) const;

private:
	vector<string> languages;
	unordered_map<int32_t, vector<int32_t>> official_languages_by_country;
	unordered_map<int32_t, vector<int32_t>> languages_by_country;
	double prob_english;
	double prob_second_lang;
};

class LdbcNamesDictionary {
public:
	LdbcNamesDictionary(const string &dictionary_dir, const LdbcPlaceDictionary &places);

	string GetRandomGivenName(LdbcJavaRandom &random, int32_t country_id, bool is_male, int32_t birth_year) const;
	string GetRandomSurname(LdbcJavaRandom &random, int32_t country_id) const;

private:
	static constexpr idx_t BIRTH_YEAR_PERIODS = 2;

	int32_t GetGeometricRandomIndex(LdbcJavaRandom &random, idx_t name_count) const;

	unordered_map<int32_t, vector<string>> surnames_by_country;
	std::array<unordered_map<int32_t, vector<string>>, BIRTH_YEAR_PERIODS> given_names_male;
	std::array<unordered_map<int32_t, vector<string>>, BIRTH_YEAR_PERIODS> given_names_female;
};

class LdbcEmailDictionary {
public:
	explicit LdbcEmailDictionary(const string &dictionary_dir);

	string GetRandomEmail(LdbcJavaRandom &random_top, LdbcJavaRandom &random_email) const;

private:
	vector<string> emails;
	vector<double> cumulative_distribution;
};

class LdbcPersonDictionaries {
public:
	LdbcPersonDictionaries(const string &resource_dir, double prob_english, double prob_second_lang);

	LdbcBrowserDictionary browsers;
	LdbcPlaceDictionary places;
	LdbcIPAddressDictionary ips;
	LdbcLanguageDictionary languages;
	LdbcNamesDictionary names;
	LdbcEmailDictionary emails;
};

string LdbcResourcePath(const string &base, const string &path);

} // namespace duckdb
