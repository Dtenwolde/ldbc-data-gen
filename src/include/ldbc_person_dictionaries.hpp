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
	const string &GetPlaceName(int32_t place_id) const;
	int32_t GetCountryId(const string &country_name) const;
	int32_t GetCityId(const string &city_name) const;
	int32_t GetZOrderId(int32_t place_id) const;
	int32_t GetPlaceIdFromZOrder(int32_t zorder_id) const;

private:
	vector<float> cumulative_distribution;
	vector<int32_t> countries;
	vector<string> country_names_by_id;
	vector<string> place_names_by_id;
	unordered_map<string, int32_t> country_names;
	unordered_map<string, int32_t> city_names;
	unordered_map<int32_t, vector<int32_t>> cities_by_country;
	vector<int32_t> country_zorder_by_id;
	vector<int32_t> place_id_by_zorder;
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

class LdbcPersonDeleteDistribution {
public:
	explicit LdbcPersonDeleteDistribution(const string &dictionary_dir);

	bool IsDeleted(LdbcJavaRandom &random, int64_t max_knows) const;

private:
	vector<double> distribution;
};

class LdbcTagDictionary {
public:
	LdbcTagDictionary(const string &dictionary_dir, idx_t country_count, double tag_country_corr_prob);

	int32_t GetTagByCountry(LdbcJavaRandom &random_tag_other_country, LdbcJavaRandom &random_tag_country_prob,
	                        int32_t country_id) const;
	vector<int32_t> GetRandomTags(LdbcJavaRandom &random, int32_t count) const;
	const string &GetName(int32_t tag_id) const;

private:
	double tag_country_corr_prob;
	vector<string> tag_names;
	vector<vector<int32_t>> tags_by_country;
	vector<vector<double>> cumulative_distribution_by_country;
};

struct LdbcGeneratedText {
	string content;
	int32_t java_length = 0;
};

class LdbcTagTextDictionary {
public:
	LdbcTagTextDictionary(const string &dictionary_dir, const LdbcTagDictionary &tags);

	int32_t GetRandomTextSize(LdbcJavaRandom &random_text_size, LdbcJavaRandom &random_reduced_text, int32_t min_size,
	                          int32_t max_size, double reduced_text_ratio) const;
	int32_t GetRandomLargeTextSize(LdbcJavaRandom &random_text_size, int32_t min_size, int32_t max_size) const;
	LdbcGeneratedText GenerateText(LdbcJavaRandom &random_text_size, const vector<int32_t> &tag_ids,
	                               int32_t text_size) const;
	LdbcGeneratedText ConsumeText(LdbcJavaRandom &random_text_size, const vector<int32_t> &tag_ids,
	                              int32_t text_size) const;

private:
	const LdbcTagDictionary &tags;
	vector<string> tag_text;
	vector<int32_t> tag_text_lengths;
	vector<bool> tag_text_bmp_only;
	vector<string> tag_prefixes;
	vector<int32_t> tag_prefix_lengths;
};

class LdbcTagMatrix {
public:
	explicit LdbcTagMatrix(const string &dictionary_dir);

	vector<int32_t> GetSetOfTags(LdbcJavaRandom &random_topic, LdbcJavaRandom &random_tag, int32_t popular_tag_id,
	                             int32_t tag_count) const;
	int32_t GetRandomRelated(LdbcJavaRandom &random, int32_t tag_id) const;

private:
	unordered_map<int32_t, vector<int32_t>> related_tags;
	unordered_map<int32_t, vector<double>> cumulative_distribution;
	vector<int32_t> non_zero_tags;
};

class LdbcCompanyDictionary {
public:
	LdbcCompanyDictionary(const string &dictionary_dir, const LdbcPlaceDictionary &places,
	                      double prob_uncorrelated_company);

	int64_t GetRandomCompany(LdbcJavaRandom &random_uncorrelated_company, LdbcJavaRandom &random_uncorrelated_location,
	                         LdbcJavaRandom &random_company, int32_t country_id) const;
	idx_t GetCompanyCount() const;

private:
	const LdbcPlaceDictionary &places;
	double prob_uncorrelated_company;
	vector<vector<int64_t>> companies_by_country;
	idx_t company_count = 0;
};

class LdbcUniversityDictionary {
public:
	LdbcUniversityDictionary(const string &dictionary_dir, const LdbcPlaceDictionary &places,
	                         double prob_uncorrelated_university, double prob_top_university, int64_t start_index);

	int32_t GetRandomUniversityLocation(LdbcJavaRandom &random_uncorrelated_university,
	                                    LdbcJavaRandom &random_uncorrelated_location,
	                                    LdbcJavaRandom &random_top_university, LdbcJavaRandom &random_university,
	                                    int32_t country_id) const;
	int64_t GetUniversityFromLocation(int32_t university_location) const;

private:
	const LdbcPlaceDictionary &places;
	double prob_uncorrelated_university;
	double prob_top_university;
	int64_t start_index;
	vector<vector<int64_t>> universities_by_country;
};

class LdbcPersonDictionaries {
public:
	LdbcPersonDictionaries(const string &resource_dir, double prob_english, double prob_second_lang,
	                       double tag_country_corr_prob, double prob_uncorrelated_company,
	                       double prob_uncorrelated_university, double prob_top_university);

	LdbcBrowserDictionary browsers;
	LdbcPlaceDictionary places;
	LdbcIPAddressDictionary ips;
	LdbcLanguageDictionary languages;
	LdbcNamesDictionary names;
	LdbcEmailDictionary emails;
	LdbcPersonDeleteDistribution person_deletes;
	LdbcTagDictionary tags;
	LdbcTagTextDictionary tag_text;
	LdbcTagMatrix tag_matrix;
	LdbcCompanyDictionary companies;
	LdbcUniversityDictionary universities;
};

string LdbcResourcePath(const string &base, const string &path);

} // namespace duckdb
