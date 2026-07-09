#define DUCKDB_EXTENSION_MAIN

#include "ldbc_data_gen_extension.hpp"
#include "ldbc_datagen_config.hpp"
#include "ldbc_gen.hpp"
#include "ldbc_java_random.hpp"
#include "ldbc_person_generator.hpp"
#include "ldbc_unicode.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/planner/binder.hpp"

#include <fstream>
#include <iomanip>
#include <limits>
#include <set>
#include <sstream>
#include <unordered_map>

namespace duckdb {

struct LdbcSchemaColumn {
	const char *relation_name;
	const char *entity_path;
	const char *kind;
	const char *primary_key;
	const char *column_name;
	const char *logical_type;
	bool nullable;
};

static constexpr LdbcSchemaColumn LDBC_BI_STATIC_SCHEMA[] = {
    {"Organisation", "static/Organisation", "static_node", "id", "id", "INTEGER", false},
    {"Organisation", "static/Organisation", "static_node", "id", "type", "VARCHAR", false},
    {"Organisation", "static/Organisation", "static_node", "id", "name", "VARCHAR", false},
    {"Organisation", "static/Organisation", "static_node", "id", "url", "VARCHAR", false},
    {"Organisation", "static/Organisation", "static_node", "id", "LocationPlaceId", "INTEGER", false},
    {"Place", "static/Place", "static_node", "id", "id", "INTEGER", false},
    {"Place", "static/Place", "static_node", "id", "name", "VARCHAR", false},
    {"Place", "static/Place", "static_node", "id", "url", "VARCHAR", false},
    {"Place", "static/Place", "static_node", "id", "type", "VARCHAR", false},
    {"Place", "static/Place", "static_node", "id", "PartOfPlaceId", "INTEGER", true},
    {"Tag", "static/Tag", "static_node", "id", "id", "INTEGER", false},
    {"Tag", "static/Tag", "static_node", "id", "name", "VARCHAR", false},
    {"Tag", "static/Tag", "static_node", "id", "url", "VARCHAR", false},
    {"Tag", "static/Tag", "static_node", "id", "TypeTagClassId", "INTEGER", false},
    {"TagClass", "static/TagClass", "static_node", "id", "id", "INTEGER", false},
    {"TagClass", "static/TagClass", "static_node", "id", "name", "VARCHAR", false},
    {"TagClass", "static/TagClass", "static_node", "id", "url", "VARCHAR", false},
    {"TagClass", "static/TagClass", "static_node", "id", "SubclassOfTagClassId", "INTEGER", true},
    {"Person", "dynamic/Person", "dynamic_node", "id", "creationDate", "TIMESTAMP_MS", false},
    {"Person", "dynamic/Person", "dynamic_node", "id", "id", "BIGINT", false},
    {"Person", "dynamic/Person", "dynamic_node", "id", "firstName", "VARCHAR", false},
    {"Person", "dynamic/Person", "dynamic_node", "id", "lastName", "VARCHAR", false},
    {"Person", "dynamic/Person", "dynamic_node", "id", "gender", "VARCHAR", false},
    {"Person", "dynamic/Person", "dynamic_node", "id", "birthday", "DATE", false},
    {"Person", "dynamic/Person", "dynamic_node", "id", "locationIP", "VARCHAR", false},
    {"Person", "dynamic/Person", "dynamic_node", "id", "browserUsed", "VARCHAR", false},
    {"Person", "dynamic/Person", "dynamic_node", "id", "LocationCityId", "INTEGER", false},
    {"Person", "dynamic/Person", "dynamic_node", "id", "language", "VARCHAR", false},
    {"Person", "dynamic/Person", "dynamic_node", "id", "email", "VARCHAR", false},
    {"Person_hasInterest_Tag", "dynamic/Person_hasInterest_Tag", "dynamic_edge", "PersonId,TagId", "creationDate",
     "TIMESTAMP_MS", false},
    {"Person_hasInterest_Tag", "dynamic/Person_hasInterest_Tag", "dynamic_edge", "PersonId,TagId", "PersonId",
     "BIGINT", false},
    {"Person_hasInterest_Tag", "dynamic/Person_hasInterest_Tag", "dynamic_edge", "PersonId,TagId", "TagId",
     "INTEGER", false},
    {"Person_knows_Person", "dynamic/Person_knows_Person", "dynamic_edge", "Person1Id,Person2Id", "creationDate",
     "TIMESTAMP_MS", false},
    {"Person_knows_Person", "dynamic/Person_knows_Person", "dynamic_edge", "Person1Id,Person2Id", "Person1Id",
     "BIGINT", false},
    {"Person_knows_Person", "dynamic/Person_knows_Person", "dynamic_edge", "Person1Id,Person2Id", "Person2Id",
     "BIGINT", false},
    {"Person_studyAt_University", "dynamic/Person_studyAt_University", "dynamic_edge", "PersonId,UniversityId",
     "creationDate", "TIMESTAMP_MS", false},
    {"Person_studyAt_University", "dynamic/Person_studyAt_University", "dynamic_edge", "PersonId,UniversityId",
     "PersonId", "BIGINT", false},
    {"Person_studyAt_University", "dynamic/Person_studyAt_University", "dynamic_edge", "PersonId,UniversityId",
     "UniversityId", "BIGINT", false},
    {"Person_studyAt_University", "dynamic/Person_studyAt_University", "dynamic_edge", "PersonId,UniversityId",
     "classYear", "INTEGER", false},
    {"Person_workAt_Company", "dynamic/Person_workAt_Company", "dynamic_edge", "PersonId,CompanyId", "creationDate",
     "TIMESTAMP_MS", false},
    {"Person_workAt_Company", "dynamic/Person_workAt_Company", "dynamic_edge", "PersonId,CompanyId", "PersonId",
     "BIGINT", false},
    {"Person_workAt_Company", "dynamic/Person_workAt_Company", "dynamic_edge", "PersonId,CompanyId", "CompanyId",
     "BIGINT", false},
    {"Person_workAt_Company", "dynamic/Person_workAt_Company", "dynamic_edge", "PersonId,CompanyId", "workFrom",
     "INTEGER", false},
    {"Forum", "dynamic/Forum", "dynamic_node", "id", "creationDate", "TIMESTAMP_MS", false},
    {"Forum", "dynamic/Forum", "dynamic_node", "id", "id", "BIGINT", false},
    {"Forum", "dynamic/Forum", "dynamic_node", "id", "title", "VARCHAR", false},
    {"Forum", "dynamic/Forum", "dynamic_node", "id", "ModeratorPersonId", "BIGINT", false},
    {"Forum_hasMember_Person", "dynamic/Forum_hasMember_Person", "dynamic_edge", "ForumId,PersonId", "creationDate",
     "TIMESTAMP_MS", false},
    {"Forum_hasMember_Person", "dynamic/Forum_hasMember_Person", "dynamic_edge", "ForumId,PersonId", "ForumId",
     "BIGINT", false},
    {"Forum_hasMember_Person", "dynamic/Forum_hasMember_Person", "dynamic_edge", "ForumId,PersonId", "PersonId",
     "BIGINT", false},
    {"Forum_hasTag_Tag", "dynamic/Forum_hasTag_Tag", "dynamic_edge", "ForumId,TagId", "creationDate", "TIMESTAMP_MS",
     false},
    {"Forum_hasTag_Tag", "dynamic/Forum_hasTag_Tag", "dynamic_edge", "ForumId,TagId", "ForumId", "BIGINT", false},
    {"Forum_hasTag_Tag", "dynamic/Forum_hasTag_Tag", "dynamic_edge", "ForumId,TagId", "TagId", "INTEGER", false},
    {"Comment", "dynamic/Comment", "dynamic_node", "id", "creationDate", "TIMESTAMP_MS", false},
    {"Comment", "dynamic/Comment", "dynamic_node", "id", "id", "BIGINT", false},
    {"Comment", "dynamic/Comment", "dynamic_node", "id", "locationIP", "VARCHAR", false},
    {"Comment", "dynamic/Comment", "dynamic_node", "id", "browserUsed", "VARCHAR", false},
    {"Comment", "dynamic/Comment", "dynamic_node", "id", "content", "VARCHAR", false},
    {"Comment", "dynamic/Comment", "dynamic_node", "id", "length", "INTEGER", false},
    {"Comment", "dynamic/Comment", "dynamic_node", "id", "CreatorPersonId", "BIGINT", false},
    {"Comment", "dynamic/Comment", "dynamic_node", "id", "LocationCountryId", "INTEGER", false},
    {"Comment", "dynamic/Comment", "dynamic_node", "id", "ParentPostId", "BIGINT", true},
    {"Comment", "dynamic/Comment", "dynamic_node", "id", "ParentCommentId", "BIGINT", true},
    {"Comment_hasTag_Tag", "dynamic/Comment_hasTag_Tag", "dynamic_edge", "CommentId,TagId", "creationDate",
     "TIMESTAMP_MS", false},
    {"Comment_hasTag_Tag", "dynamic/Comment_hasTag_Tag", "dynamic_edge", "CommentId,TagId", "CommentId", "BIGINT",
     false},
    {"Comment_hasTag_Tag", "dynamic/Comment_hasTag_Tag", "dynamic_edge", "CommentId,TagId", "TagId", "INTEGER",
     false},
    {"Post", "dynamic/Post", "dynamic_node", "id", "creationDate", "TIMESTAMP_MS", false},
    {"Post", "dynamic/Post", "dynamic_node", "id", "id", "BIGINT", false},
    {"Post", "dynamic/Post", "dynamic_node", "id", "imageFile", "VARCHAR", true},
    {"Post", "dynamic/Post", "dynamic_node", "id", "locationIP", "VARCHAR", false},
    {"Post", "dynamic/Post", "dynamic_node", "id", "browserUsed", "VARCHAR", false},
    {"Post", "dynamic/Post", "dynamic_node", "id", "language", "VARCHAR", true},
    {"Post", "dynamic/Post", "dynamic_node", "id", "content", "VARCHAR", true},
    {"Post", "dynamic/Post", "dynamic_node", "id", "length", "INTEGER", false},
    {"Post", "dynamic/Post", "dynamic_node", "id", "CreatorPersonId", "BIGINT", false},
    {"Post", "dynamic/Post", "dynamic_node", "id", "ContainerForumId", "BIGINT", false},
    {"Post", "dynamic/Post", "dynamic_node", "id", "LocationCountryId", "BIGINT", false},
    {"Post_hasTag_Tag", "dynamic/Post_hasTag_Tag", "dynamic_edge", "PostId,TagId", "creationDate", "TIMESTAMP_MS",
     false},
    {"Post_hasTag_Tag", "dynamic/Post_hasTag_Tag", "dynamic_edge", "PostId,TagId", "PostId", "BIGINT", false},
    {"Post_hasTag_Tag", "dynamic/Post_hasTag_Tag", "dynamic_edge", "PostId,TagId", "TagId", "INTEGER", false},
    {"Person_likes_Post", "dynamic/Person_likes_Post", "dynamic_edge", "PersonId,PostId", "creationDate",
     "TIMESTAMP_MS", false},
    {"Person_likes_Post", "dynamic/Person_likes_Post", "dynamic_edge", "PersonId,PostId", "PersonId", "BIGINT",
     false},
    {"Person_likes_Post", "dynamic/Person_likes_Post", "dynamic_edge", "PersonId,PostId", "PostId", "BIGINT", false},
    {"Person_likes_Comment", "dynamic/Person_likes_Comment", "dynamic_edge", "PersonId,CommentId", "creationDate",
     "TIMESTAMP_MS", false},
    {"Person_likes_Comment", "dynamic/Person_likes_Comment", "dynamic_edge", "PersonId,CommentId", "PersonId",
     "BIGINT", false},
    {"Person_likes_Comment", "dynamic/Person_likes_Comment", "dynamic_edge", "PersonId,CommentId", "CommentId",
     "BIGINT", false},
};

struct LdbcGenBindData : public TableFunctionData {
	double scale_factor = 1.0;
	string catalog = INVALID_CATALOG;
	string output_dir;
	string target = "tables";
	string schema = "main";
	string format = "parquet";
	string dictionary_dir = "third_party/ldbc_snb_datagen_spark/src/main/resources/dictionaries";
	bool overwrite = false;
};

struct LdbcGenGlobalState : public GlobalTableFunctionState {
	bool materialized = false;
	idx_t offset = 0;
	unordered_map<string, idx_t> row_counts;
};

struct LdbcGenSchemaBindData : public TableFunctionData {
	string format = "parquet";
};

struct LdbcGenSchemaGlobalState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

struct LdbcJavaRandomBindData : public TableFunctionData {
	int64_t seed = 0;
	idx_t rows = 1;
};

struct LdbcJavaRandomGlobalState : public GlobalTableFunctionState {
	explicit LdbcJavaRandomGlobalState(int64_t seed)
	    : next_long(seed), next_double(seed), next_float(seed), next_int_100(seed), next_int_max(seed) {
	}

	idx_t offset = 0;
	LdbcJavaRandom next_long;
	LdbcJavaRandom next_double;
	LdbcJavaRandom next_float;
	LdbcJavaRandom next_int_100;
	LdbcJavaRandom next_int_max;
};

struct LdbcGenConfigEntry {
	const char *parameter;
	string value;
	const char *logical_type;
	const char *source;
};

struct LdbcGenConfigBindData : public TableFunctionData {
	vector<LdbcGenConfigEntry> entries;
};

struct LdbcGenConfigGlobalState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

struct LdbcGenPersonCoreBindData : public TableFunctionData {
	LdbcDatagenConfig config;
	idx_t rows = 10;
};

struct LdbcGenPersonCoreGlobalState : public GlobalTableFunctionState {
	explicit LdbcGenPersonCoreGlobalState(const LdbcDatagenConfig &config) : generator(config) {
	}

	idx_t offset = 0;
	LdbcPersonGenerator generator;
};

static string GetStringParameter(TableFunctionBindInput &input, const string &name, const string &default_value) {
	auto entry = input.named_parameters.find(name);
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return default_value;
	}
	return entry->second.GetValue<string>();
}

static double GetDoubleParameter(TableFunctionBindInput &input, const string &name, double default_value) {
	auto entry = input.named_parameters.find(name);
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return default_value;
	}
	return entry->second.GetValue<double>();
}

static bool GetBooleanParameter(TableFunctionBindInput &input, const string &name, bool default_value) {
	auto entry = input.named_parameters.find(name);
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return default_value;
	}
	return entry->second.GetValue<bool>();
}

static int64_t GetBigIntParameter(TableFunctionBindInput &input, const string &name, int64_t default_value) {
	auto entry = input.named_parameters.find(name);
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return default_value;
	}
	return entry->second.GetValue<int64_t>();
}

static LogicalType LdbcLogicalType(const string &type) {
	if (type == "BIGINT") {
		return LogicalType::BIGINT;
	}
	if (type == "INTEGER") {
		return LogicalType::INTEGER;
	}
	if (type == "VARCHAR") {
		return LogicalType::VARCHAR;
	}
	if (type == "DATE") {
		return LogicalType::DATE;
	}
	if (type == "TIMESTAMP_MS") {
		return LogicalType::TIMESTAMP_MS;
	}
	throw InternalException("Unsupported LDBC schema type: %s", type);
}

static idx_t LdbcSchemaSize() {
	return sizeof(LDBC_BI_STATIC_SCHEMA) / sizeof(LDBC_BI_STATIC_SCHEMA[0]);
}

static vector<string> SplitPrimaryKey(const string &primary_key) {
	vector<string> result;
	string current;
	for (auto c : primary_key) {
		if (c == ',') {
			result.push_back(current);
			current.clear();
		} else {
			current += c;
		}
	}
	if (!current.empty()) {
		result.push_back(current);
	}
	return result;
}

static void CreateSchemaIfNeeded(ClientContext &context, const string &catalog_name, const string &schema) {
	auto &catalog = Catalog::GetCatalog(context, catalog_name);
	CreateSchemaInfo info;
	info.catalog = catalog_name;
	info.schema = schema;
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	catalog.CreateSchema(context, info);
}

static void DropTableIfNeeded(ClientContext &context, const string &catalog_name, const string &schema,
                              const string &table_name) {
	DropInfo drop_info;
	drop_info.type = CatalogType::TABLE_ENTRY;
	drop_info.catalog = catalog_name;
	drop_info.schema = schema;
	drop_info.name = table_name;
	drop_info.if_not_found = OnEntryNotFound::RETURN_NULL;
	Catalog::GetCatalog(context, catalog_name).DropEntry(context, drop_info);
}

static void CreateLdbcTable(ClientContext &context, const LdbcSchemaColumn *begin, const LdbcSchemaColumn *end,
                            const string &catalog_name, const string &schema, bool overwrite) {
	auto table_name = string(begin->relation_name);
	if (overwrite) {
		DropTableIfNeeded(context, catalog_name, schema, table_name);
	}

	auto table_info = make_uniq<CreateTableInfo>(catalog_name, schema, table_name);
	idx_t column_index = 0;
	for (auto column = begin; column != end; column++) {
		table_info->columns.AddColumn(ColumnDefinition(column->column_name, LdbcLogicalType(column->logical_type)));
		if (!column->nullable) {
			table_info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(column_index)));
		}
		column_index++;
	}
	table_info->constraints.push_back(make_uniq<UniqueConstraint>(SplitPrimaryKey(begin->primary_key), true));
	Catalog::GetCatalog(context, catalog_name).CreateTable(context, std::move(table_info));
}

static string DictionaryPath(const LdbcGenBindData &bind_data, const string &file_name) {
	if (bind_data.dictionary_dir.empty()) {
		throw InvalidInputException("ldbcgen parameter dictionary_dir must not be empty");
	}
	if (bind_data.dictionary_dir.back() == '/') {
		return bind_data.dictionary_dir + file_name;
	}
	return bind_data.dictionary_dir + "/" + file_name;
}

static std::ifstream OpenDictionaryFile(const LdbcGenBindData &bind_data, const string &file_name) {
	auto path = DictionaryPath(bind_data, file_name);
	std::ifstream file(path);
	if (!file.is_open()) {
		throw IOException("Could not open LDBC dictionary file '%s'", path);
	}
	return file;
}

static string ResourceDirFromDictionaryDir(const string &dictionary_dir) {
	const string suffix = "/dictionaries";
	if (dictionary_dir.size() >= suffix.size() &&
	    dictionary_dir.substr(dictionary_dir.size() - suffix.size()) == suffix) {
		return dictionary_dir.substr(0, dictionary_dir.size() - suffix.size());
	}
	return dictionary_dir;
}

static string StripCarriageReturn(string value) {
	if (!value.empty() && value.back() == '\r') {
		value.pop_back();
	}
	return value;
}

static string TrimWhitespace(const string &value) {
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

static string ClampString(const string &value, idx_t max_length) {
	if (value.size() <= max_length) {
		return value;
	}
	return value.substr(0, max_length);
}

static string ReplaceAll(string value, const string &needle, const string &replacement) {
	idx_t offset = 0;
	while ((offset = value.find(needle, offset)) != string::npos) {
		value.replace(offset, needle.size(), replacement);
		offset += replacement.size();
	}
	return value;
}

static vector<string> SplitByWhitespace(const string &line) {
	vector<string> result;
	string current;
	for (auto c : line) {
		if (c == ' ' || c == '\t') {
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

static vector<string> SplitByDelimiter(const string &line, const string &delimiter) {
	vector<string> result;
	idx_t offset = 0;
	while (true) {
		auto next = line.find(delimiter, offset);
		if (next == string::npos) {
			result.push_back(line.substr(offset));
			return result;
		}
		result.push_back(line.substr(offset, next - offset));
		offset = next + delimiter.size();
	}
}

struct StaticPlaceRecord {
	StaticPlaceRecord(int32_t id, string name, string type, int32_t part_of_place_id)
	    : id(id), name(std::move(name)), type(std::move(type)), part_of_place_id(part_of_place_id) {
	}

	int32_t id;
	string name;
	string type;
	int32_t part_of_place_id = -1;
};

struct StaticDictionaryData {
	vector<StaticPlaceRecord> places;
	unordered_map<string, int32_t> country_ids;
	unordered_map<string, int32_t> city_ids;
	unordered_map<int32_t, string> tag_class_names;
	unordered_map<int32_t, int32_t> tag_class_parent;
	unordered_map<int32_t, string> tag_names;
	unordered_map<int32_t, int32_t> tag_classes;
};

static StaticDictionaryData LoadStaticDictionaryData(const LdbcGenBindData &bind_data) {
	StaticDictionaryData data;
	unordered_map<string, int32_t> continent_ids;

	string line;
	auto locations = OpenDictionaryFile(bind_data, "dicLocations.txt");
	while (std::getline(locations, line)) {
		line = StripCarriageReturn(line);
		if (line.empty()) {
			continue;
		}
		auto columns = SplitByWhitespace(line);
		if (columns.size() < 2) {
			throw InvalidInputException("Malformed dicLocations.txt line: '%s'", line);
		}
		auto &country = columns[1];
		if (data.country_ids.find(country) == data.country_ids.end()) {
			auto id = NumericCast<int32_t>(data.places.size());
			data.country_ids[country] = id;
			data.places.push_back(StaticPlaceRecord(id, country, "Country", -1));
		}
	}

	auto cities = OpenDictionaryFile(bind_data, "citiesByCountry.txt");
	while (std::getline(cities, line)) {
		line = StripCarriageReturn(line);
		if (line.empty()) {
			continue;
		}
		auto columns = SplitByWhitespace(line);
		if (columns.size() < 2) {
			throw InvalidInputException("Malformed citiesByCountry.txt line: '%s'", line);
		}
		auto country_entry = data.country_ids.find(columns[0]);
		if (country_entry != data.country_ids.end() && data.city_ids.find(columns[1]) == data.city_ids.end()) {
			auto id = NumericCast<int32_t>(data.places.size());
			data.city_ids[columns[1]] = id;
			data.places.push_back(StaticPlaceRecord(id, columns[1], "City", country_entry->second));
		}
	}

	locations = OpenDictionaryFile(bind_data, "dicLocations.txt");
	while (std::getline(locations, line)) {
		line = StripCarriageReturn(line);
		if (line.empty()) {
			continue;
		}
		auto columns = SplitByWhitespace(line);
		if (columns.size() < 2) {
			throw InvalidInputException("Malformed dicLocations.txt line: '%s'", line);
		}
		auto &continent = columns[0];
		if (continent_ids.find(continent) == continent_ids.end()) {
			auto id = NumericCast<int32_t>(data.places.size());
			continent_ids[continent] = id;
			data.places.push_back(StaticPlaceRecord(id, continent, "Continent", -1));
		}
		data.places[data.country_ids[columns[1]]].part_of_place_id = continent_ids[continent];
	}

	auto tag_classes = OpenDictionaryFile(bind_data, "tagClasses.txt");
	while (std::getline(tag_classes, line)) {
		line = StripCarriageReturn(line);
		if (line.empty()) {
			continue;
		}
		auto columns = SplitByDelimiter(line, "\t");
		if (columns.size() < 2) {
			throw InvalidInputException("Malformed tagClasses.txt line: '%s'", line);
		}
		data.tag_class_names[std::stoi(columns[0])] = columns[1];
	}

	auto tag_hierarchy = OpenDictionaryFile(bind_data, "tagClassHierarchy.txt");
	while (std::getline(tag_hierarchy, line)) {
		line = StripCarriageReturn(line);
		if (line.empty()) {
			continue;
		}
		auto columns = SplitByDelimiter(line, "\t");
		if (columns.size() < 2) {
			throw InvalidInputException("Malformed tagClassHierarchy.txt line: '%s'", line);
		}
		data.tag_class_parent[std::stoi(columns[0])] = std::stoi(columns[1]);
	}

	auto tags = OpenDictionaryFile(bind_data, "tags.txt");
	while (std::getline(tags, line)) {
		line = StripCarriageReturn(line);
		if (line.empty()) {
			continue;
		}
		auto columns = SplitByDelimiter(line, "\t");
		if (columns.size() < 3) {
			throw InvalidInputException("Malformed tags.txt line: '%s'", line);
		}
		auto tag_id = std::stoi(columns[0]);
		data.tag_classes[tag_id] = std::stoi(columns[1]);
		data.tag_names[tag_id] = columns[2];
	}

	return data;
}

static unique_ptr<InternalAppender> MakeStaticAppender(ClientContext &context, const LdbcGenBindData &bind_data,
                                                       const string &table_name) {
	auto &catalog = Catalog::GetCatalog(context, bind_data.catalog);
	auto &table = catalog.GetEntry<TableCatalogEntry>(context, bind_data.schema, table_name);
	if (!table.IsDuckTable()) {
		throw InvalidInputException("ldbcgen can only append generated data to DuckDB tables");
	}
	return make_uniq<InternalAppender>(context, table);
}

static idx_t AppendPlaces(ClientContext &context, const LdbcGenBindData &bind_data, const StaticDictionaryData &data) {
	auto appender = MakeStaticAppender(context, bind_data, "Place");
	for (auto &place : data.places) {
		appender->BeginRow();
		appender->Append<int32_t>(place.id);
		appender->Append(Value(ClampString(place.name, 256)));
		appender->Append(Value("http://dbpedia.org/resource/" + place.name));
		appender->Append(Value(place.type));
		if (place.part_of_place_id == -1) {
			appender->Append(Value());
		} else {
			appender->Append<int32_t>(place.part_of_place_id);
		}
		appender->EndRow();
	}
	appender->Close();
	return data.places.size();
}

static idx_t AppendTags(ClientContext &context, const LdbcGenBindData &bind_data, const StaticDictionaryData &data) {
	auto appender = MakeStaticAppender(context, bind_data, "Tag");
	std::set<int32_t> tag_ids;
	for (auto &entry : data.tag_names) {
		tag_ids.insert(entry.first);
	}
	for (auto tag_id : tag_ids) {
		auto name = ReplaceAll(ClampString(data.tag_names.at(tag_id), 256), "\"", "\\\"");
		appender->BeginRow();
		appender->Append<int32_t>(tag_id);
		appender->Append(Value(name));
		appender->Append(Value("http://dbpedia.org/resource/" + name));
		appender->Append<int32_t>(data.tag_classes.at(tag_id));
		appender->EndRow();
	}
	appender->Close();
	return tag_ids.size();
}

static idx_t AppendTagClasses(ClientContext &context, const LdbcGenBindData &bind_data,
                              const StaticDictionaryData &data) {
	std::set<int32_t> reachable_classes;
	for (auto &tag_class : data.tag_classes) {
		auto class_id = tag_class.second;
		while (class_id != -1) {
			if (!reachable_classes.insert(class_id).second) {
				break;
			}
			auto parent = data.tag_class_parent.find(class_id);
			class_id = parent == data.tag_class_parent.end() ? -1 : parent->second;
		}
	}

	auto appender = MakeStaticAppender(context, bind_data, "TagClass");
	for (auto class_id : reachable_classes) {
		auto name = ClampString(data.tag_class_names.at(class_id), 256);
		appender->BeginRow();
		appender->Append<int32_t>(class_id);
		appender->Append(Value(name));
		appender->Append(Value(name == "Thing" ? string("http://www.w3.org/2002/07/owl#Thing")
		                                      : "http://dbpedia.org/ontology/" + name));
		auto parent = data.tag_class_parent.find(class_id);
		if (parent == data.tag_class_parent.end()) {
			appender->Append(Value());
		} else {
			appender->Append<int32_t>(parent->second);
		}
		appender->EndRow();
	}
	appender->Close();
	return reachable_classes.size();
}

static idx_t AppendOrganisations(ClientContext &context, const LdbcGenBindData &bind_data,
                                 const StaticDictionaryData &data) {
	auto appender = MakeStaticAppender(context, bind_data, "Organisation");
	idx_t row_count = 0;
	string line;

	auto companies = OpenDictionaryFile(bind_data, "companiesByCountry.txt");
	while (std::getline(companies, line)) {
		line = StripCarriageReturn(line);
		if (line.empty()) {
			continue;
		}
		auto columns = SplitByDelimiter(line, "  ");
		if (columns.size() < 2) {
			throw InvalidInputException("Malformed companiesByCountry.txt line: '%s'", line);
		}
		auto country = data.country_ids.find(columns[0]);
		if (country == data.country_ids.end()) {
			continue;
		}
		auto name = ClampString(TrimWhitespace(columns[1]), 256);
		appender->BeginRow();
		appender->Append<int32_t>(NumericCast<int32_t>(row_count));
		appender->Append(Value("Company"));
		appender->Append(Value(name));
		appender->Append(Value("http://dbpedia.org/resource/" + name));
		appender->Append<int32_t>(country->second);
		appender->EndRow();
		row_count++;
	}

	auto universities = OpenDictionaryFile(bind_data, "universities.txt");
	while (std::getline(universities, line)) {
		line = StripCarriageReturn(line);
		if (line.empty()) {
			continue;
		}
		auto columns = SplitByDelimiter(line, "  ");
		if (columns.size() < 3) {
			throw InvalidInputException("Malformed universities.txt line: '%s'", line);
		}
		auto country = data.country_ids.find(columns[0]);
		auto city = data.city_ids.find(columns[2]);
		if (country == data.country_ids.end() || city == data.city_ids.end()) {
			continue;
		}
		auto name = ClampString(TrimWhitespace(columns[1]), 256);
		appender->BeginRow();
		appender->Append<int32_t>(NumericCast<int32_t>(row_count));
		appender->Append(Value("University"));
		appender->Append(Value(name));
		appender->Append(Value("http://dbpedia.org/resource/" + name));
		appender->Append<int32_t>(city->second);
		appender->EndRow();
		row_count++;
	}
	appender->Close();
	return row_count;
}

struct PersonOwnedRowCounts {
	idx_t persons = 0;
	idx_t interests = 0;
	idx_t study_at = 0;
	idx_t work_at = 0;
	idx_t knows = 0;
};

static PersonOwnedRowCounts AppendPersonOwnedTables(ClientContext &context, const LdbcGenBindData &bind_data) {
	auto resource_dir = ResourceDirFromDictionaryDir(bind_data.dictionary_dir);
	auto config = LdbcDatagenConfig::Load(bind_data.scale_factor, resource_dir);
	LdbcDateGenerator dates(config);
	auto persons = LdbcGeneratePersons(config);
	auto person_appender = MakeStaticAppender(context, bind_data, "Person");
	auto interest_appender = MakeStaticAppender(context, bind_data, "Person_hasInterest_Tag");
	auto study_appender = MakeStaticAppender(context, bind_data, "Person_studyAt_University");
	auto work_appender = MakeStaticAppender(context, bind_data, "Person_workAt_Company");
	auto knows_appender = MakeStaticAppender(context, bind_data, "Person_knows_Person");
	auto bulkload_threshold =
	    dates.SimulationEnd() - static_cast<int64_t>(static_cast<double>(dates.SimulationEnd() - dates.SimulationStart()) *
	                                                 (1.0 - config.bulkload_portion));

	PersonOwnedRowCounts row_counts;
	for (auto &person : persons) {
		if (person.creation_date >= bulkload_threshold) {
			continue;
		}

		person_appender->BeginRow();
		person_appender->Append(Value::TIMESTAMP(LdbcTimestampMs(person.creation_date)));
		person_appender->Append<int64_t>(person.account_id);
		person_appender->Append(Value(person.first_name));
		person_appender->Append(Value(person.last_name));
		person_appender->Append(Value(person.gender == 0 ? string("female") : string("male")));
		person_appender->Append(Value::DATE(LdbcDateFromEpochMs(person.birthday)));
		person_appender->Append(Value(person.ip_address));
		person_appender->Append(Value(person.browser_name));
		person_appender->Append<int32_t>(person.city_id);
		person_appender->Append(Value(person.languages));
		person_appender->Append(Value(person.emails));
		person_appender->EndRow();
		row_counts.persons++;

		for (auto tag_id : person.interests) {
			interest_appender->BeginRow();
			interest_appender->Append(Value::TIMESTAMP(LdbcTimestampMs(person.creation_date)));
			interest_appender->Append<int64_t>(person.account_id);
			interest_appender->Append<int32_t>(tag_id);
			interest_appender->EndRow();
			row_counts.interests++;
		}

		if (person.university_id != -1 && person.class_year != -1) {
			study_appender->BeginRow();
			study_appender->Append(Value::TIMESTAMP(LdbcTimestampMs(person.creation_date)));
			study_appender->Append<int64_t>(person.account_id);
			study_appender->Append<int64_t>(person.university_id);
			study_appender->Append<int32_t>(Date::ExtractYear(LdbcDateFromEpochMs(person.class_year)));
			study_appender->EndRow();
			row_counts.study_at++;
		}

		for (auto &company : person.companies) {
			work_appender->BeginRow();
			work_appender->Append(Value::TIMESTAMP(LdbcTimestampMs(person.creation_date)));
			work_appender->Append<int64_t>(person.account_id);
			work_appender->Append<int64_t>(company.first);
			work_appender->Append<int32_t>(Date::ExtractYear(LdbcDateFromEpochMs(company.second)));
			work_appender->EndRow();
			row_counts.work_at++;
		}
	}

	auto knows_edges = LdbcGenerateKnows(config, persons);
	for (auto &edge : knows_edges) {
		if (edge.creation_date >= bulkload_threshold) {
			continue;
		}
		knows_appender->BeginRow();
		knows_appender->Append(Value::TIMESTAMP(LdbcTimestampMs(edge.creation_date)));
		knows_appender->Append<int64_t>(edge.person1_id);
		knows_appender->Append<int64_t>(edge.person2_id);
		knows_appender->EndRow();
		row_counts.knows++;
	}
	person_appender->Close();
	interest_appender->Close();
	study_appender->Close();
	work_appender->Close();
	knows_appender->Close();
	return row_counts;
}

static unordered_map<string, idx_t> PopulateStaticTables(ClientContext &context, const LdbcGenBindData &bind_data) {
	auto data = LoadStaticDictionaryData(bind_data);
	unordered_map<string, idx_t> row_counts;
	row_counts["Place"] = AppendPlaces(context, bind_data, data);
	row_counts["TagClass"] = AppendTagClasses(context, bind_data, data);
	row_counts["Tag"] = AppendTags(context, bind_data, data);
	row_counts["Organisation"] = AppendOrganisations(context, bind_data, data);
	auto person_counts = AppendPersonOwnedTables(context, bind_data);
	row_counts["Person"] = person_counts.persons;
	row_counts["Person_hasInterest_Tag"] = person_counts.interests;
	row_counts["Person_knows_Person"] = person_counts.knows;
	row_counts["Person_studyAt_University"] = person_counts.study_at;
	row_counts["Person_workAt_Company"] = person_counts.work_at;
	return row_counts;
}

static idx_t LdbcRelationCount() {
	idx_t count = 0;
	const char *previous_relation = nullptr;
	for (idx_t row_idx = 0; row_idx < LdbcSchemaSize(); row_idx++) {
		auto &row = LDBC_BI_STATIC_SCHEMA[row_idx];
		if (!previous_relation || string(previous_relation) != row.relation_name) {
			count++;
			previous_relation = row.relation_name;
		}
	}
	return count;
}

static const LdbcSchemaColumn &LdbcRelationAt(idx_t relation_index) {
	idx_t current_relation = DConstants::INVALID_INDEX;
	const char *previous_relation = nullptr;
	for (idx_t row_idx = 0; row_idx < LdbcSchemaSize(); row_idx++) {
		auto &row = LDBC_BI_STATIC_SCHEMA[row_idx];
		if (!previous_relation || string(previous_relation) != row.relation_name) {
			current_relation++;
			previous_relation = row.relation_name;
			if (current_relation == relation_index) {
				return row;
			}
		}
	}
	throw InternalException("LDBC relation index out of range");
}

static unordered_map<string, idx_t> MaterializeLdbcTables(ClientContext &context, const LdbcGenBindData &bind_data) {
	CreateSchemaIfNeeded(context, bind_data.catalog, bind_data.schema);

	idx_t relation_start = 0;
	for (idx_t row_idx = 1; row_idx <= LdbcSchemaSize(); row_idx++) {
		if (row_idx == LdbcSchemaSize() ||
		    string(LDBC_BI_STATIC_SCHEMA[row_idx].relation_name) != LDBC_BI_STATIC_SCHEMA[relation_start].relation_name) {
			CreateLdbcTable(context, LDBC_BI_STATIC_SCHEMA + relation_start, LDBC_BI_STATIC_SCHEMA + row_idx,
			                bind_data.catalog, bind_data.schema, bind_data.overwrite);
			relation_start = row_idx;
		}
	}
	return PopulateStaticTables(context, bind_data);
}

namespace ldbc {

void LDBCGenWrapper::CreateLDBCSchema(ClientContext &context, string catalog, string schema, bool overwrite) {
	CreateSchemaIfNeeded(context, catalog, schema);

	idx_t relation_start = 0;
	for (idx_t row_idx = 1; row_idx <= LdbcSchemaSize(); row_idx++) {
		if (row_idx == LdbcSchemaSize() ||
		    string(LDBC_BI_STATIC_SCHEMA[row_idx].relation_name) != LDBC_BI_STATIC_SCHEMA[relation_start].relation_name) {
			CreateLdbcTable(context, LDBC_BI_STATIC_SCHEMA + relation_start, LDBC_BI_STATIC_SCHEMA + row_idx, catalog,
			                schema, overwrite);
			relation_start = row_idx;
		}
	}
}

unordered_map<string, idx_t> LDBCGenWrapper::LoadLDBCData(ClientContext &context, string catalog, string schema,
                                                          string dictionary_dir, double scale_factor) {
	LdbcGenBindData bind_data;
	bind_data.catalog = std::move(catalog);
	bind_data.schema = std::move(schema);
	bind_data.dictionary_dir = std::move(dictionary_dir);
	bind_data.scale_factor = scale_factor;
	return PopulateStaticTables(context, bind_data);
}

idx_t LDBCGenWrapper::RelationCount() {
	return LdbcRelationCount();
}

string LDBCGenWrapper::RelationName(idx_t relation_index) {
	return LdbcRelationAt(relation_index).relation_name;
}

} // namespace ldbc

static unique_ptr<FunctionData> LdbcGenBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	if (!input.inputs.empty()) {
		throw BinderException("ldbcgen only accepts named parameters");
	}

	auto result = make_uniq<LdbcGenBindData>();
	result->catalog = DatabaseManager::GetDefaultDatabase(context);
	result->schema = ClientData::Get(context).catalog_search_path->GetDefault().schema;
	result->scale_factor = GetDoubleParameter(input, "sf", 1.0);
	result->catalog = GetStringParameter(input, "catalog", result->catalog);
	result->output_dir = GetStringParameter(input, "output_dir", "");
	result->target = StringUtil::Lower(GetStringParameter(input, "target", "tables"));
	result->schema = GetStringParameter(input, "schema", result->schema);
	result->format = StringUtil::Lower(GetStringParameter(input, "format", "parquet"));
	result->dictionary_dir = GetStringParameter(input, "dictionary_dir", result->dictionary_dir);
	result->overwrite = GetBooleanParameter(input, "overwrite", false);

	if (result->scale_factor <= 0) {
		throw BinderException("ldbcgen parameter sf must be greater than zero");
	}
	if (result->target != "tables" && result->target != "files") {
		throw BinderException("ldbcgen parameter target must be either 'tables' or 'files'");
	}
	if (result->target == "tables" && result->schema.empty()) {
		throw BinderException("ldbcgen parameter schema must not be empty when target is 'tables'");
	}
	if (result->target == "files" && result->output_dir.empty()) {
		throw BinderException("ldbcgen parameter output_dir must be set when target is 'files'");
	}
	if (result->format != "parquet" && result->format != "csv") {
		throw BinderException("ldbcgen parameter format must be either 'parquet' or 'csv'");
	}
	if (input.binder && result->target == "tables") {
		auto &catalog = Catalog::GetCatalog(context, result->catalog);
		auto &properties = input.binder->GetStatementProperties();
		DatabaseModificationType modification;
		modification |= DatabaseModificationType::CREATE_CATALOG_ENTRY;
		modification |= DatabaseModificationType::INSERT_DATA;
		properties.RegisterDBModify(catalog, context, modification);
	}

	names.emplace_back("relation_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("row_count");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("checksum");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("format");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> LdbcGenInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<LdbcGenGlobalState>();
}

static void LdbcGenFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<LdbcGenBindData>();
	auto &state = data_p.global_state->Cast<LdbcGenGlobalState>();

	if (bind_data.target == "tables" && !state.materialized) {
		ldbc::LDBCGenWrapper::CreateLDBCSchema(context, bind_data.catalog, bind_data.schema, bind_data.overwrite);
		state.row_counts =
		    ldbc::LDBCGenWrapper::LoadLDBCData(context, bind_data.catalog, bind_data.schema, bind_data.dictionary_dir,
		                                       bind_data.scale_factor);
		state.materialized = true;
	}

	if (state.offset >= (bind_data.target == "tables" ? ldbc::LDBCGenWrapper::RelationCount() : 1)) {
		return;
	}

	idx_t count = 0;
	if (bind_data.target == "files") {
		output.data[0].SetValue(0, "ldbc_snb_bi_static");
		output.data[1].SetValue(0, bind_data.output_dir);
		output.data[2].SetValue(0, Value(LogicalType::BIGINT));
		output.data[3].SetValue(0, Value(LogicalType::VARCHAR));
		output.data[4].SetValue(0, bind_data.format);
		output.data[5].SetValue(0, "planned");
		count = 1;
		state.offset = 1;
	} else {
		while (state.offset < ldbc::LDBCGenWrapper::RelationCount() && count < STANDARD_VECTOR_SIZE) {
			auto relation_name = ldbc::LDBCGenWrapper::RelationName(state.offset);
			output.data[0].SetValue(count, relation_name);
			output.data[1].SetValue(count, bind_data.catalog + "." + bind_data.schema + "." + relation_name);
			auto row_count = state.row_counts.find(relation_name);
			output.data[2].SetValue(count, Value::BIGINT(row_count == state.row_counts.end() ? 0 : row_count->second));
			output.data[3].SetValue(count, Value(LogicalType::VARCHAR));
			output.data[4].SetValue(count, "table");
			output.data[5].SetValue(count, bind_data.overwrite ? "recreated" : "created");
			count++;
			state.offset++;
		}
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> LdbcGenSchemaBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	if (!input.inputs.empty()) {
		throw BinderException("ldbcgen_schema only accepts named parameters");
	}

	auto result = make_uniq<LdbcGenSchemaBindData>();
	result->format = StringUtil::Lower(GetStringParameter(input, "format", "parquet"));
	if (result->format != "parquet" && result->format != "csv") {
		throw BinderException("ldbcgen_schema parameter format must be either 'parquet' or 'csv'");
	}

	names.emplace_back("relation_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("entity_path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("kind");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("snapshot_path");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("column_index");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("column_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("logical_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("nullable");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("primary_key");
	return_types.emplace_back(LogicalType::VARCHAR);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> LdbcGenSchemaInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<LdbcGenSchemaGlobalState>();
}

static void LdbcGenSchemaFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<LdbcGenSchemaBindData>();
	auto &state = data_p.global_state->Cast<LdbcGenSchemaGlobalState>();
	idx_t count = 0;
	idx_t column_index = 0;
	const char *previous_relation = nullptr;

	for (idx_t row_idx = 0; row_idx < state.offset; row_idx++) {
		auto &row = LDBC_BI_STATIC_SCHEMA[row_idx];
		if (!previous_relation || string(previous_relation) != row.relation_name) {
			previous_relation = row.relation_name;
			column_index = 0;
		} else {
			column_index++;
		}
	}

	for (idx_t row_idx = state.offset; row_idx < sizeof(LDBC_BI_STATIC_SCHEMA) / sizeof(LDBC_BI_STATIC_SCHEMA[0]) &&
	                                   count < STANDARD_VECTOR_SIZE;
	     row_idx++) {
		auto &row = LDBC_BI_STATIC_SCHEMA[row_idx];
		if (!previous_relation || string(previous_relation) != row.relation_name) {
			previous_relation = row.relation_name;
			column_index = 0;
		} else {
			column_index++;
		}

		string snapshot_path = "graphs/" + bind_data.format + "/bi/composite-merged-fk/initial_snapshot/" +
		                       string(row.entity_path);
		output.data[0].SetValue(count, row.relation_name);
		output.data[1].SetValue(count, row.entity_path);
		output.data[2].SetValue(count, row.kind);
		output.data[3].SetValue(count, snapshot_path);
		output.data[4].SetValue(count, Value::INTEGER(NumericCast<int32_t>(column_index)));
		output.data[5].SetValue(count, row.column_name);
		output.data[6].SetValue(count, row.logical_type);
		output.data[7].SetValue(count, Value::BOOLEAN(row.nullable));
		output.data[8].SetValue(count, row.primary_key);
		count++;
	}

	state.offset += count;
	output.SetCardinality(count);
}

static string DoubleToConfigString(double value) {
	std::ostringstream stream;
	stream << std::setprecision(15) << value;
	return stream.str();
}

static void AddConfigEntry(vector<LdbcGenConfigEntry> &entries, const char *parameter, string value,
                           const char *logical_type, const char *source) {
	entries.push_back({parameter, std::move(value), logical_type, source});
}

static vector<LdbcGenConfigEntry> BuildConfigEntries(const LdbcDatagenConfig &config) {
	vector<LdbcGenConfigEntry> entries;
	AddConfigEntry(entries, "scale_factor", config.scale_factor_name, "VARCHAR", "scale_factors.xml");
	AddConfigEntry(entries, "resource_dir", config.resource_dir, "VARCHAR", "argument");
	AddConfigEntry(entries, "num_persons", std::to_string(config.num_persons), "BIGINT", "scale_factors.xml");
	AddConfigEntry(entries, "block_size", std::to_string(config.block_size), "INTEGER", "params_default.ini");
	AddConfigEntry(entries, "start_year", std::to_string(config.start_year), "INTEGER", "scale_factors.xml");
	AddConfigEntry(entries, "num_years", std::to_string(config.num_years), "INTEGER", "scale_factors.xml");
	AddConfigEntry(entries, "delta", std::to_string(config.delta), "INTEGER", "params_default.ini");
	AddConfigEntry(entries, "degree_distribution", config.degree_distribution, "VARCHAR", "scale_factors.xml");
	AddConfigEntry(entries, "knows_generator", config.knows_generator, "VARCHAR", "params_default.ini");
	AddConfigEntry(entries, "person_similarity", config.person_similarity, "VARCHAR", "params_default.ini");
	AddConfigEntry(entries, "max_num_friends", std::to_string(config.max_num_friends), "INTEGER", "params_default.ini");
	AddConfigEntry(entries, "min_num_tags_per_person", std::to_string(config.min_num_tags_per_person), "INTEGER",
	               "params_default.ini");
	AddConfigEntry(entries, "max_num_tags_per_person", std::to_string(config.max_num_tags_per_person), "INTEGER",
	               "params_default.ini");
	AddConfigEntry(entries, "max_emails", std::to_string(config.max_emails), "INTEGER", "params_default.ini");
	AddConfigEntry(entries, "max_companies", std::to_string(config.max_companies), "INTEGER",
	               "params_default.ini:generator.maxEmails");
	AddConfigEntry(entries, "prob_english", DoubleToConfigString(config.prob_english), "DOUBLE",
	               "params_default.ini:generator.maxEmails");
	AddConfigEntry(entries, "prob_second_lang", DoubleToConfigString(config.prob_second_lang), "DOUBLE",
	               "params_default.ini:generator.maxEmails");
	AddConfigEntry(entries, "missing_ratio", DoubleToConfigString(config.missing_ratio), "DOUBLE", "params_default.ini");
	AddConfigEntry(entries, "prob_another_browser", DoubleToConfigString(config.prob_another_browser), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "prob_uncorrelated_company", DoubleToConfigString(config.prob_uncorrelated_company), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "prob_uncorrelated_organisation", DoubleToConfigString(config.prob_uncorrelated_organisation),
	               "DOUBLE", "params_default.ini");
	AddConfigEntry(entries, "prob_top_univ", DoubleToConfigString(config.prob_top_univ), "DOUBLE", "params_default.ini");
	AddConfigEntry(entries, "tag_country_corr_prob", DoubleToConfigString(config.tag_country_corr_prob), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "alpha", DoubleToConfigString(LdbcDatagenConfig::ALPHA), "DOUBLE", "DatagenParams.java");
	AddConfigEntry(entries, "bulkload_portion", DoubleToConfigString(config.bulkload_portion), "DOUBLE",
	               "LdbcDatagen.scala");
	return entries;
}

static unique_ptr<FunctionData> LdbcGenConfigBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	if (!input.inputs.empty()) {
		throw BinderException("ldbcgen_config only accepts named parameters");
	}

	auto result = make_uniq<LdbcGenConfigBindData>();
	auto scale_factor = GetDoubleParameter(input, "sf", 1.0);
	if (scale_factor <= 0) {
		throw BinderException("ldbcgen_config parameter sf must be greater than zero");
	}
	auto resource_dir = GetStringParameter(input, "resource_dir", LdbcDatagenConfig::DEFAULT_RESOURCE_DIR);
	result->entries = BuildConfigEntries(LdbcDatagenConfig::Load(scale_factor, resource_dir));

	names.emplace_back("parameter");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("value");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("logical_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("source");
	return_types.emplace_back(LogicalType::VARCHAR);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> LdbcGenConfigInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<LdbcGenConfigGlobalState>();
}

static void LdbcGenConfigFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<LdbcGenConfigBindData>();
	auto &state = data_p.global_state->Cast<LdbcGenConfigGlobalState>();
	idx_t count = 0;

	while (state.offset < bind_data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = bind_data.entries[state.offset];
		output.data[0].SetValue(count, entry.parameter);
		output.data[1].SetValue(count, entry.value);
		output.data[2].SetValue(count, entry.logical_type);
		output.data[3].SetValue(count, entry.source);
		state.offset++;
		count++;
	}

	output.SetCardinality(count);
}

static unique_ptr<FunctionData> LdbcGenPersonCoreBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	if (!input.inputs.empty()) {
		throw BinderException("ldbcgen_person_core only accepts named parameters");
	}

	auto scale_factor = GetDoubleParameter(input, "sf", 1.0);
	if (scale_factor <= 0) {
		throw BinderException("ldbcgen_person_core parameter sf must be greater than zero");
	}

	auto result = make_uniq<LdbcGenPersonCoreBindData>();
	auto resource_dir = GetStringParameter(input, "resource_dir", LdbcDatagenConfig::DEFAULT_RESOURCE_DIR);
	result->config = LdbcDatagenConfig::Load(scale_factor, resource_dir);
	auto rows = GetBigIntParameter(input, "rows", 10);
	if (rows < 0) {
		throw BinderException("ldbcgen_person_core parameter rows must be non-negative");
	}
	result->rows = NumericCast<idx_t>(std::min<int64_t>(rows, result->config.num_persons));

	names.emplace_back("sequential_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("block_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("block_offset");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("creation_date_ms");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("creationDate");
	return_types.emplace_back(LogicalType::TIMESTAMP_MS);
	names.emplace_back("id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("deletion_date_ms");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("deletionDate");
	return_types.emplace_back(LogicalType::TIMESTAMP_MS);
	names.emplace_back("explicitly_deleted");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("max_num_knows");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("birthday_ms");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("birthday");
	return_types.emplace_back(LogicalType::DATE);
	names.emplace_back("gender_code");
	return_types.emplace_back(LogicalType::TINYINT);
	names.emplace_back("country_id");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("city_id");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("browser_id");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("browser");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("ip_address");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("language");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("first_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("last_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("email");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("message_deleter");
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("random_id");
	return_types.emplace_back(LogicalType::BIGINT);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> LdbcGenPersonCoreInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<LdbcGenPersonCoreBindData>();
	return make_uniq<LdbcGenPersonCoreGlobalState>(bind_data.config);
}

static void LdbcGenPersonCoreFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<LdbcGenPersonCoreBindData>();
	auto &state = data_p.global_state->Cast<LdbcGenPersonCoreGlobalState>();

	idx_t count = 0;
	while (state.offset < bind_data.rows && count < STANDARD_VECTOR_SIZE) {
		auto person = state.generator.GenerateCore(NumericCast<int64_t>(state.offset));
		output.data[0].SetValue(count, Value::BIGINT(person.sequential_id));
		output.data[1].SetValue(count, Value::BIGINT(person.block_id));
		output.data[2].SetValue(count, Value::BIGINT(person.block_offset));
		output.data[3].SetValue(count, Value::BIGINT(person.creation_date));
		output.data[4].SetValue(count, Value::TIMESTAMP(LdbcTimestampMs(person.creation_date)));
		output.data[5].SetValue(count, Value::BIGINT(person.account_id));
		output.data[6].SetValue(count, Value::BIGINT(person.deletion_date));
		output.data[7].SetValue(count, Value::TIMESTAMP(LdbcTimestampMs(person.deletion_date)));
		output.data[8].SetValue(count, Value::BOOLEAN(person.explicitly_deleted));
		output.data[9].SetValue(count, Value::BIGINT(person.max_num_knows));
		output.data[10].SetValue(count, Value::BIGINT(person.birthday));
		output.data[11].SetValue(count, Value::DATE(LdbcDateFromEpochMs(person.birthday)));
		output.data[12].SetValue(count, Value::TINYINT(person.gender));
		output.data[13].SetValue(count, Value::INTEGER(person.country_id));
		output.data[14].SetValue(count, Value::INTEGER(person.city_id));
		output.data[15].SetValue(count, Value::INTEGER(person.browser_id));
		output.data[16].SetValue(count, person.browser_name);
		output.data[17].SetValue(count, person.ip_address);
		output.data[18].SetValue(count, person.languages);
		output.data[19].SetValue(count, person.first_name);
		output.data[20].SetValue(count, person.last_name);
		output.data[21].SetValue(count, person.emails);
		output.data[22].SetValue(count, Value::BOOLEAN(person.message_deleter));
		output.data[23].SetValue(count, Value::BIGINT(person.random_id));
		count++;
		state.offset++;
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> LdbcJavaRandomBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	if (!input.inputs.empty()) {
		throw BinderException("ldbc_java_random only accepts named parameters");
	}

	auto result = make_uniq<LdbcJavaRandomBindData>();
	result->seed = GetBigIntParameter(input, "seed", 0);
	auto rows = GetBigIntParameter(input, "rows", 1);
	if (rows < 0) {
		throw BinderException("ldbc_java_random parameter rows must be non-negative");
	}
	result->rows = UnsafeNumericCast<idx_t>(rows);

	names.emplace_back("draw_index");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("next_long");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("next_double");
	return_types.emplace_back(LogicalType::DOUBLE);
	names.emplace_back("next_float");
	return_types.emplace_back(LogicalType::FLOAT);
	names.emplace_back("next_int_100");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("next_int_max");
	return_types.emplace_back(LogicalType::INTEGER);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> LdbcJavaRandomInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<LdbcJavaRandomBindData>();
	return make_uniq<LdbcJavaRandomGlobalState>(bind_data.seed);
}

static void LdbcJavaRandomFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<LdbcJavaRandomBindData>();
	auto &state = data_p.global_state->Cast<LdbcJavaRandomGlobalState>();
	idx_t count = 0;

	while (state.offset < bind_data.rows && count < STANDARD_VECTOR_SIZE) {
		output.data[0].SetValue(count, Value::INTEGER(NumericCast<int32_t>(state.offset)));
		output.data[1].SetValue(count, Value::BIGINT(state.next_long.NextLong()));
		output.data[2].SetValue(count, Value::DOUBLE(state.next_double.NextDouble()));
		output.data[3].SetValue(count, Value::FLOAT(state.next_float.NextFloat()));
		output.data[4].SetValue(count, Value::INTEGER(state.next_int_100.NextInt(100)));
		output.data[5].SetValue(count, Value::INTEGER(state.next_int_max.NextInt(std::numeric_limits<int32_t>::max())));
		state.offset++;
		count++;
	}

	output.SetCardinality(count);
}

static void LdbcEmailBaseFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(input, result, args.size(), [&](string_t value) {
		return StringVector::AddString(result, LdbcEmailBaseFromFirstName(value.GetString()));
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction ldbcgen("ldbcgen", {}, LdbcGenFunction, LdbcGenBind, LdbcGenInit);
	ldbcgen.named_parameters["sf"] = LogicalType::DOUBLE;
	ldbcgen.named_parameters["catalog"] = LogicalType::VARCHAR;
	ldbcgen.named_parameters["output_dir"] = LogicalType::VARCHAR;
	ldbcgen.named_parameters["target"] = LogicalType::VARCHAR;
	ldbcgen.named_parameters["schema"] = LogicalType::VARCHAR;
	ldbcgen.named_parameters["format"] = LogicalType::VARCHAR;
	ldbcgen.named_parameters["dictionary_dir"] = LogicalType::VARCHAR;
	ldbcgen.named_parameters["overwrite"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(ldbcgen);

	TableFunction ldbcgen_schema("ldbcgen_schema", {}, LdbcGenSchemaFunction, LdbcGenSchemaBind, LdbcGenSchemaInit);
	ldbcgen_schema.named_parameters["format"] = LogicalType::VARCHAR;
	loader.RegisterFunction(ldbcgen_schema);

	TableFunction ldbcgen_config("ldbcgen_config", {}, LdbcGenConfigFunction, LdbcGenConfigBind, LdbcGenConfigInit);
	ldbcgen_config.named_parameters["sf"] = LogicalType::DOUBLE;
	ldbcgen_config.named_parameters["resource_dir"] = LogicalType::VARCHAR;
	loader.RegisterFunction(ldbcgen_config);

	TableFunction ldbcgen_person_core("ldbcgen_person_core", {}, LdbcGenPersonCoreFunction, LdbcGenPersonCoreBind,
	                                  LdbcGenPersonCoreInit);
	ldbcgen_person_core.named_parameters["sf"] = LogicalType::DOUBLE;
	ldbcgen_person_core.named_parameters["resource_dir"] = LogicalType::VARCHAR;
	ldbcgen_person_core.named_parameters["rows"] = LogicalType::BIGINT;
	loader.RegisterFunction(ldbcgen_person_core);

	TableFunction java_random("ldbc_java_random", {}, LdbcJavaRandomFunction, LdbcJavaRandomBind, LdbcJavaRandomInit);
	java_random.named_parameters["seed"] = LogicalType::BIGINT;
	java_random.named_parameters["rows"] = LogicalType::BIGINT;
	loader.RegisterFunction(java_random);

	ScalarFunction email_base("ldbc_email_base", {LogicalType::VARCHAR}, LogicalType::VARCHAR, LdbcEmailBaseFunction);
	loader.RegisterFunction(email_base);
}

void LdbcDataGenExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string LdbcDataGenExtension::Name() {
	return "ldbc_data_gen";
}

std::string LdbcDataGenExtension::Version() const {
#ifdef EXT_VERSION_LDBC_DATA_GEN
	return EXT_VERSION_LDBC_DATA_GEN;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(ldbc_data_gen, loader) {
	duckdb::LoadInternal(loader);
}
}
