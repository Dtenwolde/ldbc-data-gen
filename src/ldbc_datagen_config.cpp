#include "ldbc_datagen_config.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <fstream>
#include <iomanip>
#include <sstream>

namespace duckdb {

namespace {

static string JoinPath(const string &base, const string &path) {
	if (base.empty()) {
		throw InvalidInputException("LDBC resource directory must not be empty");
	}
	if (base.back() == '/') {
		return base + path;
	}
	return base + "/" + path;
}

static string Trim(const string &value) {
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

static unordered_map<string, string> ReadDefaultProperties(const string &resource_dir) {
	auto path = JoinPath(resource_dir, "params_default.ini");
	std::ifstream input(path);
	if (!input.is_open()) {
		throw IOException("Could not open LDBC default parameter file '%s'", path);
	}

	unordered_map<string, string> result;
	string line;
	while (std::getline(input, line)) {
		line = Trim(line);
		if (line.empty() || line[0] == '#') {
			continue;
		}
		auto separator = line.find(':');
		if (separator == string::npos) {
			throw InvalidInputException("Malformed LDBC parameter line in '%s': '%s'", path, line);
		}
		auto key = Trim(line.substr(0, separator));
		auto value = Trim(line.substr(separator + 1));
		if (key.empty()) {
			throw InvalidInputException("Malformed LDBC parameter line in '%s': '%s'", path, line);
		}
		result[key] = value;
	}
	return result;
}

static string ReadTextFile(const string &path) {
	std::ifstream input(path);
	if (!input.is_open()) {
		throw IOException("Could not open LDBC resource file '%s'", path);
	}
	std::stringstream buffer;
	buffer << input.rdbuf();
	return buffer.str();
}

static string ExtractTagValue(const string &block, const string &tag, idx_t start_offset = 0) {
	auto open = block.find("<" + tag + ">", start_offset);
	if (open == string::npos) {
		return "";
	}
	open += tag.size() + 2;
	auto close = block.find("</" + tag + ">", open);
	if (close == string::npos) {
		return "";
	}
	return Trim(block.substr(open, close - open));
}

static string ScaleFactorName(double scale_factor) {
	std::ostringstream stream;
	stream << std::setprecision(15) << scale_factor;
	auto result = stream.str();
	if (result.find('.') != string::npos) {
		while (!result.empty() && result.back() == '0') {
			result.pop_back();
		}
		if (!result.empty() && result.back() == '.') {
			result.pop_back();
		}
	}
	return result;
}

static unordered_map<string, string> ReadScaleFactorProperties(const string &resource_dir, const string &scale_factor) {
	auto path = JoinPath(resource_dir, "scale_factors.xml");
	auto xml = ReadTextFile(path);
	auto marker = "<scale_factor name=\"" + scale_factor + "\">";
	auto start = xml.find(marker);
	if (start == string::npos) {
		throw InvalidInputException("Scale factor '%s' does not exist in '%s'", scale_factor, path);
	}
	auto end = xml.find("</scale_factor>", start);
	if (end == string::npos) {
		throw InvalidInputException("Malformed scale factor '%s' in '%s'", scale_factor, path);
	}
	auto block = xml.substr(start + marker.size(), end - start - marker.size());

	unordered_map<string, string> result;
	idx_t offset = 0;
	while (true) {
		auto property_start = block.find("<property>", offset);
		if (property_start == string::npos) {
			break;
		}
		auto property_end = block.find("</property>", property_start);
		if (property_end == string::npos) {
			throw InvalidInputException("Malformed property for scale factor '%s' in '%s'", scale_factor, path);
		}
		auto property = block.substr(property_start, property_end - property_start);
		auto name = ExtractTagValue(property, "name");
		auto value = ExtractTagValue(property, "value");
		if (name.empty()) {
			throw InvalidInputException("Scale factor '%s' contains a property without a name", scale_factor);
		}
		result[name] = value;
		offset = property_end + 11;
	}
	return result;
}

static const string &RequiredProperty(const unordered_map<string, string> &properties, const string &key) {
	auto entry = properties.find(key);
	if (entry == properties.end()) {
		throw InvalidInputException("Required LDBC parameter '%s' is missing", key);
	}
	return entry->second;
}

static int32_t IntProperty(const unordered_map<string, string> &properties, const string &key) {
	return NumericCast<int32_t>(std::stoll(RequiredProperty(properties, key)));
}

static int64_t BigIntProperty(const unordered_map<string, string> &properties, const string &key) {
	return std::stoll(RequiredProperty(properties, key));
}

static double DoubleProperty(const unordered_map<string, string> &properties, const string &key) {
	return std::stod(RequiredProperty(properties, key));
}

} // namespace

LdbcDatagenConfig LdbcDatagenConfig::Load(double scale_factor, const string &resource_dir) {
	LdbcDatagenConfig config;
	config.scale_factor_name = ScaleFactorName(scale_factor);
	config.resource_dir = resource_dir;
	config.properties = ReadDefaultProperties(resource_dir);

	auto scale_properties = ReadScaleFactorProperties(resource_dir, config.scale_factor_name);
	for (auto &entry : scale_properties) {
		config.properties[entry.first] = entry.second;
	}

	config.num_persons = BigIntProperty(config.properties, "generator.numPersons");
	config.block_size = IntProperty(config.properties, "generator.blockSize");
	config.start_year = IntProperty(config.properties, "generator.startYear");
	config.num_years = IntProperty(config.properties, "generator.numYears");
	config.delta = IntProperty(config.properties, "generator.delta");
	config.degree_distribution = RequiredProperty(config.properties, "generator.degreeDistribution");
	config.knows_generator = RequiredProperty(config.properties, "generator.knowsGenerator");
	config.person_similarity = RequiredProperty(config.properties, "generator.person.similarity");
	config.max_num_friends = IntProperty(config.properties, "generator.maxNumFriends");
	config.min_num_tags_per_person = IntProperty(config.properties, "generator.minNumTagsPerPerson");
	config.max_num_tags_per_person = IntProperty(config.properties, "generator.maxNumTagsPerPerson");
	config.max_emails = IntProperty(config.properties, "generator.maxEmails");
	config.base_prob_correlated = DoubleProperty(config.properties, "generator.baseProbCorrelated");
	config.limit_prob_correlated = DoubleProperty(config.properties, "generator.limitProCorrelated");
	config.prob_knows_deleted = DoubleProperty(config.properties, "generator.probKnowsDeleted");

	// Preserve Spark DatagenParams.readConf quirks for exact parity.
	config.max_companies = IntProperty(config.properties, "generator.maxEmails");
	config.prob_english = DoubleProperty(config.properties, "generator.maxEmails");
	config.prob_second_lang = DoubleProperty(config.properties, "generator.maxEmails");

	config.missing_ratio = DoubleProperty(config.properties, "generator.missingRatio");
	config.prob_another_browser = DoubleProperty(config.properties, "generator.probAnotherBrowser");
	config.prob_uncorrelated_company = DoubleProperty(config.properties, "generator.probUnCorrelatedCompany");
	config.prob_uncorrelated_organisation = DoubleProperty(config.properties, "generator.probUnCorrelatedOrganisation");
	config.prob_top_univ = DoubleProperty(config.properties, "generator.probTopUniv");
	config.tag_country_corr_prob = DoubleProperty(config.properties, "generator.tagCountryCorrProb");
	config.max_num_post_per_month = IntProperty(config.properties, "generator.maxNumPostPerMonth");
	config.max_num_comments = IntProperty(config.properties, "generator.maxNumComments");
	config.max_num_flashmob_post_per_month = IntProperty(config.properties, "generator.maxNumFlashmobPostPerMonth");
	config.max_num_group_created_per_person = IntProperty(config.properties, "generator.maxNumGroupCreatedPerPerson");
	config.max_num_group_flashmob_post_per_month = IntProperty(config.properties, "generator.maxNumGroupFlashmobPostPerMonth");
	config.max_num_group_post_per_month = IntProperty(config.properties, "generator.maxNumGroupPostPerMonth");
	config.max_num_like = IntProperty(config.properties, "generator.maxNumLike");
	config.max_group_size = IntProperty(config.properties, "generator.maxNumMemberGroup");
	config.max_num_photo_albums_per_month = IntProperty(config.properties, "generator.maxNumPhotoAlbumsPerMonth");
	config.max_num_photo_per_albums = IntProperty(config.properties, "generator.maxNumPhotoPerAlbums");
	config.max_num_popular_places = IntProperty(config.properties, "generator.maxNumPopularPlaces");
	config.max_num_tag_per_flashmob_post = IntProperty(config.properties, "generator.maxNumTagPerFlashmobPost");
	config.flashmob_tags_per_month = IntProperty(config.properties, "generator.flashmobTagsPerMonth");
	config.min_text_size = IntProperty(config.properties, "generator.minTextSize");
	config.max_text_size = IntProperty(config.properties, "generator.maxTextSize");
	config.min_comment_size = IntProperty(config.properties, "generator.minCommentSize");
	config.max_comment_size = IntProperty(config.properties, "generator.maxCommentSize");
	config.min_large_post_size = IntProperty(config.properties, "generator.minLargePostSize");
	config.max_large_post_size = IntProperty(config.properties, "generator.maxLargePostSize");
	config.min_large_comment_size = IntProperty(config.properties, "generator.minLargeCommentSize");
	config.max_large_comment_size = IntProperty(config.properties, "generator.maxLargeCommentSize");
	config.group_moderator_prob = DoubleProperty(config.properties, "generator.groupModeratorProb");
	config.prob_another_browser = DoubleProperty(config.properties, "generator.probAnotherBrowser");
	config.prob_diff_ip_travel_season = DoubleProperty(config.properties, "generator.probDiffIPinTravelSeason");
	config.prob_diff_ip_not_travel_season = DoubleProperty(config.properties, "generator.probDiffIPnotTravelSeason");
	config.ratio_reduce_text = DoubleProperty(config.properties, "generator.ratioReduceText");
	config.ratio_large_post = DoubleProperty(config.properties, "generator.ratioLargePost");
	config.ratio_large_comment = DoubleProperty(config.properties, "generator.ratioLargeComment");
	config.prob_interest_flashmob_tag = DoubleProperty(config.properties, "generator.probInterestFlashmobTag");
	config.prob_random_per_level = DoubleProperty(config.properties, "generator.probRandomPerLevel");
	config.flashmob_tag_min_level = DoubleProperty(config.properties, "generator.flashmobTagMinLevel");
	config.flashmob_tag_max_level = DoubleProperty(config.properties, "generator.flashmobTagMaxLevel");
	config.flashmob_tag_dist_exp = DoubleProperty(config.properties, "generator.flashmobTagDistExp");
	config.prob_forum_deleted = DoubleProperty(config.properties, "generator.probForumDeleted");
	config.prob_memb_deleted = DoubleProperty(config.properties, "generator.probMembDeleted");
	config.prob_photo_deleted = DoubleProperty(config.properties, "generator.probPhotoDeleted");
	config.prob_comment_deleted = DoubleProperty(config.properties, "generator.probCommentDeleted");
	config.prob_like_deleted = DoubleProperty(config.properties, "generator.probLikeDeleted");
	return config;
}

} // namespace duckdb
