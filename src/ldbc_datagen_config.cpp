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
	return config;
}

} // namespace duckdb
