#pragma once

#include "ldbc_datagen_config.hpp"
#include "ldbc_java_random.hpp"
#include "ldbc_person_dictionaries.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <array>
#include <unordered_map>

namespace duckdb {

enum class LdbcRandomAspect : uint8_t {
	DATE = 0,
	BIRTH_DAY = 1,
	KNOWS_REQUEST = 2,
	INITIATOR = 3,
	UNIFORM = 4,
	NUM_TAG = 6,
	NUM_KNOWS = 7,
	NUM_COMMENT = 8,
	NUM_PHOTO_ALBUM = 9,
	NUM_PHOTO = 10,
	NUM_FORUM = 11,
	NUM_USERS_PER_FORUM = 12,
	NUM_POPULAR = 13,
	NUM_LIKE = 14,
	NUM_POST = 15,
	KNOWS = 16,
	KNOWS_LEVEL = 17,
	GENDER = 18,
	RANDOM = 19,
	MEMBERSHIP = 20,
	MEMBERSHIP_INDEX = 21,
	FORUM = 22,
	FORUM_MODERATOR = 23,
	FORUM_INTEREST = 24,
	EXTRA_INFO = 25,
	EXACT_LONG_LAT = 26,
	STATUS = 27,
	HAVE_STATUS = 28,
	STATUS_SINGLE = 29,
	USER_AGENT = 30,
	USER_AGENT_SENT = 31,
	FILE_SELECT = 32,
	IP = 33,
	DIFF_IP_FOR_TRAVELER = 34,
	DIFF_IP = 35,
	BROWSER = 36,
	DIFF_BROWSER = 37,
	CITY = 38,
	COUNTRY = 39,
	TAG = 40,
	UNIVERSITY = 41,
	UNCORRELATED_UNIVERSITY = 42,
	UNCORRELATED_UNIVERSITY_LOCATION = 43,
	TOP_UNIVERSITY = 44,
	POPULAR = 45,
	EMAIL = 46,
	TOP_EMAIL = 47,
	COMPANY = 48,
	UNCORRELATED_COMPANY = 49,
	UNCORRELATED_COMPANY_LOCATION = 50,
	LANGUAGE = 51,
	ALBUM = 52,
	ALBUM_MEMBERSHIP = 53,
	NAME = 54,
	SURNAME = 55,
	TAG_OTHER_COUNTRY = 56,
	SET_OF_TAG = 57,
	TEXT_SIZE = 58,
	REDUCED_TEXT = 59,
	LARGE_TEXT = 60,
	MEMBERSHIP_POST_CREATOR = 61,
	REPLY_TO = 62,
	TOPIC = 63,
	DELETION_PERSON = 64,
	DELETION_KNOWS = 65,
	DELETION_FORUM = 66,
	DELETION_MEMB = 67,
	DELETION_POST = 68,
	DELETION_COMM = 69,
	DELETION_LIKES = 70,
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
	int64_t RandomDate(LdbcJavaRandom &random, int64_t min_date, int64_t max_date) const;
	int64_t RandomPersonDeletionDate(LdbcJavaRandom &random, int64_t creation_date, int64_t max_deletion_date) const;
	int64_t RandomKnowsCreationDate(LdbcJavaRandom &random, int64_t person_a_creation, int64_t person_a_deletion,
	                                int64_t person_b_creation, int64_t person_b_deletion) const;
	int64_t RandomKnowsDeletionDate(LdbcJavaRandom &random, int64_t person_a_deletion, int64_t person_b_deletion,
	                                int64_t knows_creation_date) const;
	int64_t RandomClassYear(LdbcJavaRandom &random, int64_t birthday) const;
	int64_t GetWorkFromYear(LdbcJavaRandom &random, int64_t class_year, int64_t birthday) const;
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
	int64_t deletion_date;
	int64_t max_num_knows;
	int32_t main_interest;
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
	bool explicitly_deleted;
	bool large_poster;
	int64_t random_id;
	vector<int32_t> language_ids;
	vector<int32_t> interests;
	int32_t university_location_id;
	int64_t university_id;
	int64_t class_year;
	unordered_map<int64_t, int64_t> companies;
};

struct LdbcKnowsEdge {
	int64_t creation_date;
	int64_t deletion_date;
	bool explicitly_deleted;
	float weight;
	int64_t person1_id;
	int64_t person2_id;
};

struct LdbcForumMembership {
	int64_t creation_date;
	int64_t deletion_date;
	bool explicitly_deleted;
	int64_t forum_id;
	int64_t person_id;
};

struct LdbcForum {
	int64_t creation_date;
	int64_t deletion_date;
	bool explicitly_deleted;
	int64_t id;
	string title;
	int64_t moderator_person_id;
	int32_t place_id;
	vector<int32_t> tags;
	vector<LdbcForumMembership> memberships;
};

class LdbcFacebookDegreeDistribution {
public:
	explicit LdbcFacebookDegreeDistribution(const LdbcDatagenConfig &config);

	void Reset(int64_t seed);
	int64_t NextDegree();

private:
	struct Bucket {
		double min;
		double max;
	};

	vector<Bucket> buckets;
	vector<LdbcJavaRandom> random_degree;
	LdbcJavaRandom random_percentile;
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
	LdbcFacebookDegreeDistribution degree_distribution;
	int64_t current_block = -1;
};

vector<LdbcPersonCore> LdbcGeneratePersons(const LdbcDatagenConfig &config);
vector<LdbcKnowsEdge> LdbcGenerateKnows(const LdbcDatagenConfig &config, const vector<LdbcPersonCore> &persons);
vector<LdbcForum> LdbcGenerateForums(const LdbcDatagenConfig &config, const vector<LdbcPersonCore> &persons,
                                     const vector<LdbcKnowsEdge> &knows_edges);

timestamp_t LdbcTimestampMs(int64_t epoch_ms);
date_t LdbcDateFromEpochMs(int64_t epoch_ms);
int64_t LdbcEpochMsFromDate(int32_t year, int32_t month, int32_t day);

} // namespace duckdb
