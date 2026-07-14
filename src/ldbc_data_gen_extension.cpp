#define DUCKDB_EXTENSION_MAIN

#include "ldbc_data_gen_extension.hpp"
#include "ldbc_bi_queries.hpp"
#include "ldbc_datagen_config.hpp"
#include "ldbc_gen.hpp"
#include "ldbc_java_random.hpp"
#include "ldbc_person_generator.hpp"
#include "ldbc_unicode.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector/string_vector.hpp"
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
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "parquet_writer.hpp"
#include "zstd_file_system.hpp"
#include "writer/primitive_column_writer.hpp"

#include <atomic>
#include <array>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <unordered_map>

#include <sys/resource.h>

namespace duckdb {

static bool LdbcProfileEnabled() {
	static bool enabled = std::getenv("LDBCGEN_PROFILE") != nullptr;
	return enabled;
}

static double LdbcProfileNowMs() {
	using namespace std::chrono;
	return static_cast<double>(duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count()) / 1000.0;
}

class LdbcProfileTimer {
public:
	explicit LdbcProfileTimer(const char *name_p) : name(name_p), enabled(LdbcProfileEnabled()) {
		if (enabled) {
			start_ms = LdbcProfileNowMs();
		}
	}

	~LdbcProfileTimer() {
		if (enabled) {
			std::cerr << "[ldbcgen] " << name << ": " << std::fixed << std::setprecision(3)
			          << (LdbcProfileNowMs() - start_ms) << " ms" << std::endl;
		}
	}

private:
	const char *name;
	bool enabled;
	double start_ms = 0;
};

static bool LdbcPhaseProfileEnabled() {
	static bool enabled = std::getenv("LDBCGEN_PHASE_PROFILE") != nullptr;
	return enabled;
}

static bool LdbcAppendProfileEnabled() {
	static bool enabled = std::getenv("LDBCGEN_APPEND_PROFILE") != nullptr;
	return enabled;
}

static bool LdbcChunkProfileEnabled() {
	static bool enabled = std::getenv("LDBCGEN_CHUNK_PROFILE") != nullptr;
	return enabled;
}

static bool LdbcDirectSnapshotCollectionsEnabled() {
	static bool enabled = std::getenv("LDBCGEN_DIRECT_SNAPSHOT_CHUNKS") == nullptr;
	return enabled;
}

static double ldbc_chunk_flush_ms = 0;
static idx_t ldbc_chunk_flush_calls = 0;

struct LdbcProfileSample {
	double wall_ms = 0;
	double user_ms = 0;
	double system_ms = 0;
};

static double LdbcTimevalMs(const timeval &time) {
	return static_cast<double>(time.tv_sec) * 1000.0 + static_cast<double>(time.tv_usec) / 1000.0;
}

static LdbcProfileSample LdbcProfileSampleNow() {
	LdbcProfileSample sample;
	sample.wall_ms = LdbcProfileNowMs();
	rusage usage;
	if (getrusage(RUSAGE_SELF, &usage) == 0) {
		sample.user_ms = LdbcTimevalMs(usage.ru_utime);
		sample.system_ms = LdbcTimevalMs(usage.ru_stime);
	}
	return sample;
}

static LdbcProfileSample LdbcProfileSampleDelta(const LdbcProfileSample &end, const LdbcProfileSample &start) {
	return {end.wall_ms - start.wall_ms, end.user_ms - start.user_ms, end.system_ms - start.system_ms};
}

static double LdbcProfileCpuPercent(const LdbcProfileSample &sample) {
	if (sample.wall_ms <= 0) {
		return 0;
	}
	return 100.0 * (sample.user_ms + sample.system_ms) / sample.wall_ms;
}

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
    {"Person_hasInterest_Tag", "dynamic/Person_hasInterest_Tag", "dynamic_edge", "PersonId,TagId", "PersonId", "BIGINT",
     false},
    {"Person_hasInterest_Tag", "dynamic/Person_hasInterest_Tag", "dynamic_edge", "PersonId,TagId", "TagId", "INTEGER",
     false},
    {"Person_knows_Person", "dynamic/Person_knows_Person", "dynamic_edge", "Person1Id,Person2Id", "creationDate",
     "TIMESTAMP_MS", false},
    {"Person_knows_Person", "dynamic/Person_knows_Person", "dynamic_edge", "Person1Id,Person2Id", "Person1Id", "BIGINT",
     false},
    {"Person_knows_Person", "dynamic/Person_knows_Person", "dynamic_edge", "Person1Id,Person2Id", "Person2Id", "BIGINT",
     false},
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
    {"Comment_hasTag_Tag", "dynamic/Comment_hasTag_Tag", "dynamic_edge", "CommentId,TagId", "TagId", "INTEGER", false},
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
    {"Person_likes_Post", "dynamic/Person_likes_Post", "dynamic_edge", "PersonId,PostId", "PersonId", "BIGINT", false},
    {"Person_likes_Post", "dynamic/Person_likes_Post", "dynamic_edge", "PersonId,PostId", "PostId", "BIGINT", false},
    {"Person_likes_Comment", "dynamic/Person_likes_Comment", "dynamic_edge", "PersonId,CommentId", "creationDate",
     "TIMESTAMP_MS", false},
    {"Person_likes_Comment", "dynamic/Person_likes_Comment", "dynamic_edge", "PersonId,CommentId", "PersonId", "BIGINT",
     false},
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
	idx_t threads = 1;
	bool overwrite = false;
	bool primary_keys = false;
	bool emit_updates = false;
	bool visible_updates = false;
	bool direct_forum_parquet = false;
	bool direct_person_parquet = false;
	bool direct_forum_update_parquet = false;
	bool forum_update_copy_table_function = false;
	string forum_update_chunk_token;
};

struct LdbcForumUpdateChunkRegistryEntry {
	unordered_map<string, vector<unique_ptr<DataChunk>>> chunks;
};

static std::mutex ldbc_forum_update_chunk_registry_lock;
static unordered_map<string, shared_ptr<LdbcForumUpdateChunkRegistryEntry>> ldbc_forum_update_chunk_registry;
static void ExecuteLdbcSQL(ClientContext &context, const string &sql);

static bool HasLdbcForumUpdateChunks(const string &token, const string &operation, const string &relation_name) {
	std::lock_guard<std::mutex> lock(ldbc_forum_update_chunk_registry_lock);
	auto token_entry = ldbc_forum_update_chunk_registry.find(token);
	if (token_entry == ldbc_forum_update_chunk_registry.end()) {
		return false;
	}
	return token_entry->second->chunks.find(operation + ":" + relation_name) != token_entry->second->chunks.end();
}

static void ExecuteLdbcProfiledForumUpdateCopy(ClientContext &context, const string &sql, const string &operation,
                                               const string &relation_name) {
	if (!LdbcPhaseProfileEnabled()) {
		ExecuteLdbcSQL(context, sql);
		return;
	}
	auto start = LdbcProfileSampleNow();
	ExecuteLdbcSQL(context, sql);
	auto delta = LdbcProfileSampleDelta(LdbcProfileSampleNow(), start);
	std::cerr << "[ldbcgen] file.copy_forum_update_chunks." << operation << "." << relation_name
	          << ": wall=" << std::fixed << std::setprecision(3) << delta.wall_ms << " user=" << delta.user_ms
	          << " system=" << delta.system_ms << " cpu=" << LdbcProfileCpuPercent(delta) << "%\n";
}

class LdbcLoadGenerator;

static const LdbcSchemaColumn &LdbcRelationAt(idx_t relation_index);
static idx_t LdbcRelationIndexByName(const string &relation_name);
static string LdbcSparkRelationBlockPartPath(ClientContext &context, const LdbcGenBindData &bind_data,
                                             const string &operation, const LdbcSchemaColumn &relation, idx_t block_id);
static string LdbcSparkRelationPartitionBlockPartPath(ClientContext &context, const LdbcGenBindData &bind_data,
                                                      const string &operation, const LdbcSchemaColumn &relation,
                                                      const string &partition_value, idx_t block_id);
static vector<string> LdbcRelationColumnNames(const string &relation_name);
static vector<LogicalType> LdbcRelationTypes(const string &relation_name);
static vector<LogicalType> LdbcInsertTypes(const string &relation_name);
static vector<LogicalType> LdbcDeleteTypes(const string &relation_name);
static vector<string> LdbcInsertColumnNames(const string &relation_name);
static vector<string> LdbcDeleteColumnNames(const string &relation_name);
static void WriteLdbcChunksToParquet(ClientContext &context, const string &path, const vector<LogicalType> &types,
                                     const vector<string> &names, vector<unique_ptr<DataChunk>> &chunks);
static void WriteLdbcCollectionToParquet(ClientContext &context, const string &path, const vector<LogicalType> &types,
                                         const vector<string> &names, ColumnDataCollection &collection);

struct LdbcGenGlobalState : public GlobalTableFunctionState {
	bool schema_created = false;
	std::atomic<bool> load_started {false};
	std::atomic<bool> materialized {false};
	std::atomic<bool> finished {false};
	idx_t offset = 0;
	std::atomic<double> progress {0.0};
	unordered_map<string, idx_t> row_counts;
	unordered_map<string, string> output_paths;
	unique_ptr<LdbcGenBindData> file_bind_data;
	unique_ptr<DuckDB> file_database;
	unique_ptr<Connection> file_connection;
	unique_ptr<LdbcLoadGenerator> load_generator;
};

class LdbcGenYieldTask : public AsyncTask {
public:
	void Execute() override {
	}
};

static AsyncResult LdbcGenYield() {
	vector<unique_ptr<AsyncTask>> tasks;
	tasks.push_back(make_uniq<LdbcGenYieldTask>());
	return AsyncResult(std::move(tasks));
}

static void SetLdbcGenProgress(LdbcGenGlobalState *state, double progress) {
	if (!state) {
		return;
	}
	if (progress < 0.0) {
		progress = 0.0;
	} else if (progress > 100.0) {
		progress = 100.0;
	}
	state->progress.store(progress);
}

static double LdbcGenProgressRange(double start, double end, idx_t done, idx_t total) {
	if (total == 0) {
		return end;
	}
	return start + ((end - start) * (static_cast<double>(done) / static_cast<double>(total)));
}

static void ExecuteLdbcSQL(ClientContext &context, const string &sql) {
	auto result = context.Query(sql, QueryResultOutputType::FORCE_MATERIALIZED);
	if (result->HasError()) {
		throw InvalidInputException("ldbcgen failed to execute SQL '%s': %s", sql, result->GetError());
	}
}

static void MarkLdbcTransactionReadWrite(ClientContext &context, const string &catalog_name) {
	auto &catalog = Catalog::GetCatalog(context, Identifier(catalog_name));
	auto &transaction = Transaction::Get(context, catalog);
	transaction.SetReadWrite();
	DatabaseModificationType modification;
	modification |= DatabaseModificationType::CREATE_CATALOG_ENTRY;
	modification |= DatabaseModificationType::INSERT_DATA;
	modification |= DatabaseModificationType::DROP_CATALOG_ENTRY;
	transaction.SetModifications(modification);
}

struct LdbcGenSchemaBindData : public TableFunctionData {
	string format = "parquet";
};

struct LdbcGenSchemaGlobalState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

struct LdbcForumUpdateChunkBindData : public TableFunctionData {
	string token;
	string operation;
	string relation_name;
};

struct LdbcForumUpdateChunkGlobalState : public GlobalTableFunctionState {
	idx_t MaxThreads() const override {
		return chunks.empty() ? 1 : chunks.size();
	}

	std::atomic<idx_t> offset {0};
	vector<unique_ptr<DataChunk>> chunks;
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
	auto entry = input.named_parameters.find(Identifier(name));
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return default_value;
	}
	return entry->second.GetValue<string>();
}

static double GetDoubleParameter(TableFunctionBindInput &input, const string &name, double default_value) {
	auto entry = input.named_parameters.find(Identifier(name));
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return default_value;
	}
	return entry->second.GetValue<double>();
}

static bool GetBooleanParameter(TableFunctionBindInput &input, const string &name, bool default_value) {
	auto entry = input.named_parameters.find(Identifier(name));
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return default_value;
	}
	return entry->second.GetValue<bool>();
}

static int64_t GetBigIntParameter(TableFunctionBindInput &input, const string &name, int64_t default_value) {
	auto entry = input.named_parameters.find(Identifier(name));
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

static string LdbcVisibleUpdateTableSuffix(const string &relation_name) {
	return StringUtil::Lower(relation_name);
}

static string LdbcInsertTableName(const LdbcGenBindData &bind_data, const string &relation_name) {
	if (bind_data.visible_updates) {
		return "inserts_" + LdbcVisibleUpdateTableSuffix(relation_name);
	}
	return "__ldbc_insert_" + relation_name;
}

static string LdbcDeleteTableName(const LdbcGenBindData &bind_data, const string &relation_name) {
	if (bind_data.visible_updates) {
		return "deletes_" + LdbcVisibleUpdateTableSuffix(relation_name);
	}
	return "__ldbc_delete_" + relation_name;
}

static bool LdbcRelationHasDeletes(const string &relation_name) {
	return relation_name == "Person" || relation_name == "Person_knows_Person" || relation_name == "Forum" ||
	       relation_name == "Forum_hasMember_Person" || relation_name == "Post" || relation_name == "Comment" ||
	       relation_name == "Person_likes_Post" || relation_name == "Person_likes_Comment";
}

static vector<Identifier> SplitPrimaryKey(const string &primary_key) {
	vector<Identifier> result;
	string current;
	for (auto c : primary_key) {
		if (c == ',') {
			result.push_back(Identifier(current));
			current.clear();
		} else {
			current += c;
		}
	}
	if (!current.empty()) {
		result.push_back(Identifier(current));
	}
	return result;
}

static vector<string> SplitPrimaryKeyNames(const string &primary_key) {
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
	auto &catalog = Catalog::GetCatalog(context, Identifier(catalog_name));
	CreateSchemaInfo info;
	info.SetQualifiedName(QualifiedName(Identifier(catalog_name), Identifier(schema), Identifier()));
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	catalog.CreateSchema(context, info);
}

static void DropTableIfNeeded(ClientContext &context, const string &catalog_name, const string &schema,
                              const string &table_name) {
	DropInfo drop_info;
	drop_info.type = CatalogType::TABLE_ENTRY;
	drop_info.SetQualifiedName(Identifier(catalog_name), Identifier(schema), Identifier(table_name));
	drop_info.if_not_found = OnEntryNotFound::RETURN_NULL;
	Catalog::GetCatalog(context, Identifier(catalog_name)).DropEntry(context, drop_info);
}

static void CreateLdbcTableNamed(ClientContext &context, const LdbcSchemaColumn *begin, const LdbcSchemaColumn *end,
                                 const string &catalog_name, const string &schema, const string &table_name,
                                 bool overwrite, bool primary_keys) {
	if (overwrite) {
		DropTableIfNeeded(context, catalog_name, schema, table_name);
	}

	auto table_info =
	    make_uniq<CreateTableInfo>(QualifiedName(Identifier(catalog_name), Identifier(schema), Identifier(table_name)));
	idx_t column_index = 0;
	for (auto column = begin; column != end; column++) {
		table_info->columns.AddColumn(ColumnDefinition(column->column_name, LdbcLogicalType(column->logical_type)));
		if (!column->nullable) {
			table_info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(column_index)));
		}
		column_index++;
	}
	if (primary_keys) {
		table_info->constraints.push_back(make_uniq<UniqueConstraint>(SplitPrimaryKey(begin->primary_key), true));
	}
	Catalog::GetCatalog(context, Identifier(catalog_name)).CreateTable(context, std::move(table_info));
}

static void CreateLdbcTable(ClientContext &context, const LdbcSchemaColumn *begin, const LdbcSchemaColumn *end,
                            const string &catalog_name, const string &schema, bool overwrite, bool primary_keys) {
	CreateLdbcTableNamed(context, begin, end, catalog_name, schema, string(begin->relation_name), overwrite,
	                     primary_keys);
}

static void CreateLdbcInsertTable(ClientContext &context, const LdbcGenBindData &bind_data,
                                  const LdbcSchemaColumn *begin, const LdbcSchemaColumn *end) {
	auto table_name = LdbcInsertTableName(bind_data, begin->relation_name);
	if (bind_data.overwrite) {
		DropTableIfNeeded(context, bind_data.catalog, bind_data.schema, table_name);
	}

	auto table_info = make_uniq<CreateTableInfo>(
	    QualifiedName(Identifier(bind_data.catalog), Identifier(bind_data.schema), Identifier(table_name)));
	table_info->columns.AddColumn(ColumnDefinition("batch_id", LogicalType::DATE));
	table_info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(0)));
	idx_t column_index = 1;
	for (auto column = begin; column != end; column++) {
		table_info->columns.AddColumn(ColumnDefinition(column->column_name, LdbcLogicalType(column->logical_type)));
		if (!column->nullable) {
			table_info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(column_index)));
		}
		column_index++;
	}
	Catalog::GetCatalog(context, Identifier(bind_data.catalog)).CreateTable(context, std::move(table_info));
}

static void CreateLdbcDeleteTable(ClientContext &context, const LdbcGenBindData &bind_data,
                                  const LdbcSchemaColumn *begin) {
	auto table_name = LdbcDeleteTableName(bind_data, begin->relation_name);
	if (bind_data.overwrite) {
		DropTableIfNeeded(context, bind_data.catalog, bind_data.schema, table_name);
	}

	auto table_info = make_uniq<CreateTableInfo>(
	    QualifiedName(Identifier(bind_data.catalog), Identifier(bind_data.schema), Identifier(table_name)));
	table_info->columns.AddColumn(ColumnDefinition("batch_id", LogicalType::DATE));
	table_info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(0)));
	table_info->columns.AddColumn(ColumnDefinition("deletionDate", LogicalType::TIMESTAMP_MS));
	table_info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(1)));
	idx_t column_index = 2;
	for (auto &key : SplitPrimaryKeyNames(begin->primary_key)) {
		bool found = false;
		const auto *column = begin;
		while (column < LDBC_BI_STATIC_SCHEMA + LdbcSchemaSize() &&
		       string(column->relation_name) == begin->relation_name) {
			if (key == column->column_name) {
				table_info->columns.AddColumn(
				    ColumnDefinition(column->column_name, LdbcLogicalType(column->logical_type)));
				table_info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(column_index++)));
				found = true;
				break;
			}
			column++;
		}
		if (!found) {
			throw InternalException("Could not resolve primary key column %s for relation %s", key,
			                        begin->relation_name);
		}
	}
	Catalog::GetCatalog(context, Identifier(bind_data.catalog)).CreateTable(context, std::move(table_info));
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
	auto &catalog = Catalog::GetCatalog(context, Identifier(bind_data.catalog));
	auto &table = catalog.GetEntry<TableCatalogEntry>(context, Identifier(bind_data.schema), Identifier(table_name));
	if (!table.IsDuckTable()) {
		throw InvalidInputException("ldbcgen can only append generated data to DuckDB tables");
	}
	return make_uniq<InternalAppender>(context, table);
}

class LdbcChunkAppender {
public:
	explicit LdbcChunkAppender(unique_ptr<InternalAppender> appender_p, vector<LogicalType> types)
	    : appender(std::move(appender_p)) {
		chunk.Initialize(Allocator::DefaultAllocator(), types);
	}

	void AppendTimestamp(idx_t column, idx_t row_index, timestamp_t value) {
		FlatVector::GetDataMutable<timestamp_ms_t>(chunk.data[column])[row_index] =
		    timestamp_ms_t(Timestamp::GetEpochMs(value));
		FlatVector::SetNull(chunk.data[column], row_index, false);
	}

	void AppendTimestampMs(idx_t column, idx_t row_index, int64_t value) {
		FlatVector::GetDataMutable<timestamp_ms_t>(chunk.data[column])[row_index] = timestamp_ms_t(value);
		FlatVector::SetNull(chunk.data[column], row_index, false);
	}

	void AppendDate(idx_t column, idx_t row_index, date_t value) {
		FlatVector::GetDataMutable<date_t>(chunk.data[column])[row_index] = value;
		FlatVector::SetNull(chunk.data[column], row_index, false);
	}

	void AppendInt32(idx_t column, idx_t row_index, int32_t value) {
		FlatVector::GetDataMutable<int32_t>(chunk.data[column])[row_index] = value;
	}

	void AppendInt64(idx_t column, idx_t row_index, int64_t value) {
		FlatVector::GetDataMutable<int64_t>(chunk.data[column])[row_index] = value;
		FlatVector::SetNull(chunk.data[column], row_index, false);
	}

	void AppendString(idx_t column, idx_t row_index, const string &value) {
		FlatVector::GetDataMutable<string_t>(chunk.data[column])[row_index] =
		    StringVector::AddStringOrBlob(chunk.data[column], value);
		FlatVector::SetNull(chunk.data[column], row_index, false);
	}

	void AppendNull(idx_t column, idx_t row_index) {
		FlatVector::SetNull(chunk.data[column], row_index, true);
	}

	void AppendTimestampMsInt64Int64(int64_t creation_date, int64_t id1, int64_t id2) {
		auto row_index = row;
		FlatVector::GetDataMutable<timestamp_ms_t>(chunk.data[0])[row_index] = timestamp_ms_t(creation_date);
		FlatVector::GetDataMutable<int64_t>(chunk.data[1])[row_index] = id1;
		FlatVector::GetDataMutable<int64_t>(chunk.data[2])[row_index] = id2;
		EndRow();
	}

	void AppendTimestampMsInt64Int32(int64_t creation_date, int64_t id1, int32_t id2) {
		auto row_index = row;
		FlatVector::GetDataMutable<timestamp_ms_t>(chunk.data[0])[row_index] = timestamp_ms_t(creation_date);
		FlatVector::GetDataMutable<int64_t>(chunk.data[1])[row_index] = id1;
		FlatVector::GetDataMutable<int32_t>(chunk.data[2])[row_index] = id2;
		EndRow();
	}

	idx_t CurrentRow() const {
		return row;
	}

	void EndRow() {
		row++;
		if (row == STANDARD_VECTOR_SIZE) {
			Flush();
		}
	}

	void Close() {
		Flush();
		appender->Close();
	}

	void AppendChunk(DataChunk &source) {
		auto profile = LdbcChunkProfileEnabled();
		auto start_ms = profile ? LdbcProfileNowMs() : 0;
		appender->AppendDataChunk(source);
		if (profile) {
			ldbc_chunk_flush_ms += LdbcProfileNowMs() - start_ms;
			ldbc_chunk_flush_calls++;
		}
	}

private:
	void Flush() {
		if (row == 0) {
			return;
		}
		auto profile = LdbcChunkProfileEnabled();
		auto start_ms = profile ? LdbcProfileNowMs() : 0;
		chunk.SetCardinality(row);
		appender->AppendDataChunk(chunk);
		if (profile) {
			ldbc_chunk_flush_ms += LdbcProfileNowMs() - start_ms;
			ldbc_chunk_flush_calls++;
		}
		chunk.Reset();
		row = 0;
	}

private:
	unique_ptr<InternalAppender> appender;
	DataChunk chunk;
	idx_t row = 0;
};

class LdbcChunkBuilder {
public:
	explicit LdbcChunkBuilder(vector<LogicalType> types_p, ClientContext *collection_context = nullptr)
	    : types(std::move(types_p)) {
		chunk.Initialize(Allocator::DefaultAllocator(), types);
		if (collection_context) {
			collection = make_uniq<ColumnDataCollection>(*collection_context, types);
			collection->InitializeAppend(collection_append_state);
		}
	}

	void AppendTimestampMs(idx_t column, idx_t row_index, int64_t value) {
		FlatVector::GetDataMutable<timestamp_ms_t>(chunk.data[column])[row_index] = timestamp_ms_t(value);
		FlatVector::SetNull(chunk.data[column], row_index, false);
	}

	void AppendDate(idx_t column, idx_t row_index, date_t value) {
		FlatVector::GetDataMutable<date_t>(chunk.data[column])[row_index] = value;
		FlatVector::SetNull(chunk.data[column], row_index, false);
	}

	void AppendInt32(idx_t column, idx_t row_index, int32_t value) {
		FlatVector::GetDataMutable<int32_t>(chunk.data[column])[row_index] = value;
	}

	void AppendInt64(idx_t column, idx_t row_index, int64_t value) {
		FlatVector::GetDataMutable<int64_t>(chunk.data[column])[row_index] = value;
		FlatVector::SetNull(chunk.data[column], row_index, false);
	}

	void AppendString(idx_t column, idx_t row_index, const string &value) {
		FlatVector::GetDataMutable<string_t>(chunk.data[column])[row_index] =
		    StringVector::AddStringOrBlob(chunk.data[column], value);
		FlatVector::SetNull(chunk.data[column], row_index, false);
	}

	void AppendNull(idx_t column, idx_t row_index) {
		FlatVector::SetNull(chunk.data[column], row_index, true);
	}

	void AppendTimestampMsInt64Int64(int64_t creation_date, int64_t id1, int64_t id2) {
		auto row_index = row;
		FlatVector::GetDataMutable<timestamp_ms_t>(chunk.data[0])[row_index] = timestamp_ms_t(creation_date);
		FlatVector::GetDataMutable<int64_t>(chunk.data[1])[row_index] = id1;
		FlatVector::GetDataMutable<int64_t>(chunk.data[2])[row_index] = id2;
		EndRow();
	}

	void AppendTimestampMsInt64Int32(int64_t creation_date, int64_t id1, int32_t id2) {
		auto row_index = row;
		FlatVector::GetDataMutable<timestamp_ms_t>(chunk.data[0])[row_index] = timestamp_ms_t(creation_date);
		FlatVector::GetDataMutable<int64_t>(chunk.data[1])[row_index] = id1;
		FlatVector::GetDataMutable<int32_t>(chunk.data[2])[row_index] = id2;
		EndRow();
	}

	idx_t CurrentRow() const {
		return row;
	}

	void EndRow() {
		row++;
		if (row == STANDARD_VECTOR_SIZE) {
			Flush();
		}
	}

	vector<unique_ptr<DataChunk>> Finish() {
		Flush();
		return std::move(chunks);
	}

	unique_ptr<ColumnDataCollection> TakeCollection() {
		Flush();
		return std::move(collection);
	}

private:
	void Flush() {
		if (row == 0) {
			return;
		}
		chunk.SetCardinality(row);
		if (collection) {
			collection->Append(collection_append_state, chunk);
			chunk.Reset();
		} else {
			auto flushed_chunk = make_uniq<DataChunk>();
			flushed_chunk->Move(chunk);
			chunks.push_back(std::move(flushed_chunk));
			chunk.Initialize(Allocator::DefaultAllocator(), types);
		}
		row = 0;
	}

private:
	vector<LogicalType> types;
	DataChunk chunk;
	vector<unique_ptr<DataChunk>> chunks;
	unique_ptr<ColumnDataCollection> collection;
	ColumnDataAppendState collection_append_state;
	idx_t row = 0;
};

static LdbcChunkAppender MakeTimestampInt64Int64Appender(ClientContext &context, const LdbcGenBindData &bind_data,
                                                         const string &table_name) {
	return LdbcChunkAppender(MakeStaticAppender(context, bind_data, table_name),
	                         {LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::BIGINT});
}

static LdbcChunkAppender MakeTimestampInt64Int32Appender(ClientContext &context, const LdbcGenBindData &bind_data,
                                                         const string &table_name) {
	return LdbcChunkAppender(MakeStaticAppender(context, bind_data, table_name),
	                         {LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::INTEGER});
}

static unique_ptr<LdbcChunkAppender> MakePostAppender(ClientContext &context, const LdbcGenBindData &bind_data,
                                                      const string &table_name = "Post") {
	return make_uniq<LdbcChunkAppender>(
	    MakeStaticAppender(context, bind_data, table_name),
	    vector<LogicalType> {LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                         LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER,
	                         LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT});
}

static unique_ptr<LdbcChunkAppender> MakeForumAppender(ClientContext &context, const LdbcGenBindData &bind_data,
                                                       const string &table_name = "Forum") {
	return make_uniq<LdbcChunkAppender>(MakeStaticAppender(context, bind_data, table_name),
	                                    vector<LogicalType> {LogicalType::TIMESTAMP_MS, LogicalType::BIGINT,
	                                                         LogicalType::VARCHAR, LogicalType::BIGINT});
}

static unique_ptr<LdbcChunkAppender> MakeCommentAppender(ClientContext &context, const LdbcGenBindData &bind_data,
                                                         const string &table_name = "Comment") {
	return make_uniq<LdbcChunkAppender>(
	    MakeStaticAppender(context, bind_data, table_name),
	    vector<LogicalType> {LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                         LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::INTEGER,
	                         LogicalType::BIGINT, LogicalType::BIGINT});
}

static void AppendTimestampInt64Int64Row(LdbcChunkAppender &appender, timestamp_t creation_date, int64_t id1,
                                         int64_t id2) {
	auto row = appender.CurrentRow();
	appender.AppendTimestamp(0, row, creation_date);
	appender.AppendInt64(1, row, id1);
	appender.AppendInt64(2, row, id2);
	appender.EndRow();
}

static void AppendTimestampInt64Int32Row(LdbcChunkAppender &appender, timestamp_t creation_date, int64_t id1,
                                         int32_t id2) {
	auto row = appender.CurrentRow();
	appender.AppendTimestamp(0, row, creation_date);
	appender.AppendInt64(1, row, id1);
	appender.AppendInt32(2, row, id2);
	appender.EndRow();
}

template <class APPENDER>
static void AppendTimestampMsInt64Int64Row(APPENDER &appender, int64_t creation_date, int64_t id1, int64_t id2) {
	appender.AppendTimestampMsInt64Int64(creation_date, id1, id2);
}

template <class APPENDER>
static void AppendTimestampMsInt64Int32Row(APPENDER &appender, int64_t creation_date, int64_t id1, int32_t id2) {
	appender.AppendTimestampMsInt64Int32(creation_date, id1, id2);
}

template <class APPENDER>
static void AppendPersonRow(APPENDER &appender, const LdbcPersonCore &person) {
	auto row = appender.CurrentRow();
	appender.AppendTimestampMs(0, row, person.creation_date);
	appender.AppendInt64(1, row, person.account_id);
	appender.AppendString(2, row, person.first_name);
	appender.AppendString(3, row, person.last_name);
	appender.AppendString(4, row, person.gender == 0 ? string("female") : string("male"));
	appender.AppendDate(5, row, LdbcDateFromEpochMs(person.birthday));
	appender.AppendString(6, row, person.ip_address);
	appender.AppendString(7, row, person.browser_name);
	appender.AppendInt32(8, row, person.city_id);
	appender.AppendString(9, row, person.languages);
	appender.AppendString(10, row, person.emails);
	appender.EndRow();
}

template <class APPENDER>
static void AppendStudyRow(APPENDER &appender, const LdbcPersonCore &person) {
	auto row = appender.CurrentRow();
	appender.AppendTimestampMs(0, row, person.creation_date);
	appender.AppendInt64(1, row, person.account_id);
	appender.AppendInt64(2, row, person.university_id);
	appender.AppendInt32(3, row, Date::ExtractYear(LdbcDateFromEpochMs(person.class_year)));
	appender.EndRow();
}

template <class APPENDER>
static void AppendWorkRow(APPENDER &appender, const LdbcPersonCore &person,
                          const std::pair<const int64_t, int64_t> &company) {
	auto row = appender.CurrentRow();
	appender.AppendTimestampMs(0, row, person.creation_date);
	appender.AppendInt64(1, row, person.account_id);
	appender.AppendInt64(2, row, company.first);
	appender.AppendInt32(3, row, Date::ExtractYear(LdbcDateFromEpochMs(company.second)));
	appender.EndRow();
}

template <class APPENDER>
static void AppendForumRow(APPENDER &appender, const LdbcForum &forum) {
	auto row = appender.CurrentRow();
	appender.AppendTimestampMs(0, row, forum.creation_date);
	appender.AppendInt64(1, row, forum.id);
	appender.AppendString(2, row, forum.title);
	appender.AppendInt64(3, row, forum.moderator_person_id);
	appender.EndRow();
}

template <class APPENDER>
static void AppendPostRow(APPENDER &appender, const LdbcPost &post) {
	auto row = appender.CurrentRow();
	appender.AppendTimestampMs(0, row, post.creation_date);
	appender.AppendInt64(1, row, post.id);
	if (post.image_file.empty()) {
		appender.AppendNull(2, row);
	} else {
		appender.AppendString(2, row, post.image_file);
	}
	appender.AppendString(3, row, post.location_ip);
	appender.AppendString(4, row, post.browser_used);
	if (post.language.empty()) {
		appender.AppendNull(5, row);
	} else {
		appender.AppendString(5, row, post.language);
	}
	if (post.image_file.empty()) {
		appender.AppendString(6, row, post.content);
	} else {
		appender.AppendNull(6, row);
	}
	appender.AppendInt32(7, row, post.length);
	appender.AppendInt64(8, row, post.creator_person_id);
	appender.AppendInt64(9, row, post.forum_id);
	appender.AppendInt64(10, row, post.location_country_id);
	appender.EndRow();
}

template <class APPENDER>
static void AppendCommentRow(APPENDER &appender, const LdbcComment &comment) {
	auto row = appender.CurrentRow();
	appender.AppendTimestampMs(0, row, comment.creation_date);
	appender.AppendInt64(1, row, comment.id);
	appender.AppendString(2, row, comment.location_ip);
	appender.AppendString(3, row, comment.browser_used);
	appender.AppendString(4, row, comment.content);
	appender.AppendInt32(5, row, comment.length);
	appender.AppendInt64(6, row, comment.creator_person_id);
	appender.AppendInt32(7, row, comment.location_country_id);
	if (comment.parent_post_id == -1) {
		appender.AppendNull(8, row);
	} else {
		appender.AppendInt64(8, row, comment.parent_post_id);
	}
	if (comment.parent_comment_id == -1) {
		appender.AppendNull(9, row);
	} else {
		appender.AppendInt64(9, row, comment.parent_comment_id);
	}
	appender.EndRow();
}

template <class APPENDER>
static void AppendTimestampMsInt64Int32InsertRow(APPENDER &appender, int64_t creation_date, int64_t id1, int32_t id2) {
	auto row = appender.CurrentRow();
	appender.AppendDate(0, row, LdbcDateFromEpochMs(creation_date));
	appender.AppendTimestampMs(1, row, creation_date);
	appender.AppendInt64(2, row, id1);
	appender.AppendInt32(3, row, id2);
	appender.EndRow();
}

template <class APPENDER>
static void AppendTimestampMsInt64Int64InsertRow(APPENDER &appender, int64_t creation_date, int64_t id1, int64_t id2) {
	auto row = appender.CurrentRow();
	appender.AppendDate(0, row, LdbcDateFromEpochMs(creation_date));
	appender.AppendTimestampMs(1, row, creation_date);
	appender.AppendInt64(2, row, id1);
	appender.AppendInt64(3, row, id2);
	appender.EndRow();
}

template <class APPENDER>
static void AppendNodeDeleteRow(APPENDER &appender, int64_t deletion_date, int64_t id) {
	auto row = appender.CurrentRow();
	appender.AppendDate(0, row, LdbcDateFromEpochMs(deletion_date));
	appender.AppendTimestampMs(1, row, deletion_date);
	appender.AppendInt64(2, row, id);
	appender.EndRow();
}

template <class APPENDER>
static void AppendEdgeDeleteRow(APPENDER &appender, int64_t deletion_date, int64_t id1, int64_t id2) {
	auto row = appender.CurrentRow();
	appender.AppendDate(0, row, LdbcDateFromEpochMs(deletion_date));
	appender.AppendTimestampMs(1, row, deletion_date);
	appender.AppendInt64(2, row, id1);
	appender.AppendInt64(3, row, id2);
	appender.EndRow();
}

template <class APPENDER>
static void AppendForumInsertRow(APPENDER &appender, const LdbcForum &forum) {
	auto row = appender.CurrentRow();
	appender.AppendDate(0, row, LdbcDateFromEpochMs(forum.creation_date));
	appender.AppendTimestampMs(1, row, forum.creation_date);
	appender.AppendInt64(2, row, forum.id);
	appender.AppendString(3, row, forum.title);
	appender.AppendInt64(4, row, forum.moderator_person_id);
	appender.EndRow();
}

template <class APPENDER>
static void AppendPostInsertRow(APPENDER &appender, const LdbcPost &post) {
	auto row = appender.CurrentRow();
	appender.AppendDate(0, row, LdbcDateFromEpochMs(post.creation_date));
	appender.AppendTimestampMs(1, row, post.creation_date);
	appender.AppendInt64(2, row, post.id);
	if (post.image_file.empty()) {
		appender.AppendNull(3, row);
	} else {
		appender.AppendString(3, row, post.image_file);
	}
	appender.AppendString(4, row, post.location_ip);
	appender.AppendString(5, row, post.browser_used);
	if (post.language.empty()) {
		appender.AppendNull(6, row);
	} else {
		appender.AppendString(6, row, post.language);
	}
	if (post.image_file.empty()) {
		appender.AppendString(7, row, post.content);
	} else {
		appender.AppendNull(7, row);
	}
	appender.AppendInt32(8, row, post.length);
	appender.AppendInt64(9, row, post.creator_person_id);
	appender.AppendInt64(10, row, post.forum_id);
	appender.AppendInt64(11, row, post.location_country_id);
	appender.EndRow();
}

template <class APPENDER>
static void AppendCommentInsertRow(APPENDER &appender, const LdbcComment &comment) {
	auto row = appender.CurrentRow();
	appender.AppendDate(0, row, LdbcDateFromEpochMs(comment.creation_date));
	appender.AppendTimestampMs(1, row, comment.creation_date);
	appender.AppendInt64(2, row, comment.id);
	appender.AppendString(3, row, comment.location_ip);
	appender.AppendString(4, row, comment.browser_used);
	appender.AppendString(5, row, comment.content);
	appender.AppendInt32(6, row, comment.length);
	appender.AppendInt64(7, row, comment.creator_person_id);
	appender.AppendInt32(8, row, comment.location_country_id);
	if (comment.parent_post_id == -1) {
		appender.AppendNull(9, row);
	} else {
		appender.AppendInt64(9, row, comment.parent_post_id);
	}
	if (comment.parent_comment_id == -1) {
		appender.AppendNull(10, row);
	} else {
		appender.AppendInt64(10, row, comment.parent_comment_id);
	}
	appender.EndRow();
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
	idx_t forums = 0;
	idx_t forum_members = 0;
	idx_t forum_tags = 0;
	idx_t posts = 0;
	idx_t post_tags = 0;
	idx_t comments = 0;
	idx_t comment_tags = 0;
	idx_t post_likes = 0;
	idx_t comment_likes = 0;
};

static PersonOwnedRowCounts AppendPersonOwnedTables(ClientContext &context, const LdbcGenBindData &bind_data,
                                                    LdbcGenGlobalState *progress_state) {
	LdbcProfileTimer timer("populate.dynamic.total");
	auto resource_dir = ResourceDirFromDictionaryDir(bind_data.dictionary_dir);
	auto config = LdbcDatagenConfig::Load(bind_data.scale_factor, resource_dir);
	LdbcDateGenerator dates(config);
	vector<LdbcPersonCore> persons;
	{
		LdbcProfileTimer phase("generate.persons");
		persons = LdbcGeneratePersons(config);
	}
	SetLdbcGenProgress(progress_state, 25.0);
	LdbcProfileTimer append_timer("append.dynamic.total");
	auto person_appender = MakeStaticAppender(context, bind_data, "Person");
	auto interest_appender = MakeTimestampInt64Int32Appender(context, bind_data, "Person_hasInterest_Tag");
	auto study_appender = MakeStaticAppender(context, bind_data, "Person_studyAt_University");
	auto work_appender = MakeStaticAppender(context, bind_data, "Person_workAt_Company");
	auto knows_appender = MakeTimestampInt64Int64Appender(context, bind_data, "Person_knows_Person");
	auto forum_appender = MakeStaticAppender(context, bind_data, "Forum");
	auto forum_member_appender = MakeTimestampInt64Int64Appender(context, bind_data, "Forum_hasMember_Person");
	auto forum_tag_appender = MakeTimestampInt64Int32Appender(context, bind_data, "Forum_hasTag_Tag");
	auto post_appender = MakeStaticAppender(context, bind_data, "Post");
	auto post_tag_appender = MakeTimestampInt64Int32Appender(context, bind_data, "Post_hasTag_Tag");
	auto comment_appender = MakeStaticAppender(context, bind_data, "Comment");
	auto comment_tag_appender = MakeTimestampInt64Int32Appender(context, bind_data, "Comment_hasTag_Tag");
	auto post_like_appender = MakeTimestampInt64Int64Appender(context, bind_data, "Person_likes_Post");
	auto comment_like_appender = MakeTimestampInt64Int64Appender(context, bind_data, "Person_likes_Comment");
	auto bulkload_threshold =
	    dates.SimulationEnd() -
	    static_cast<int64_t>(static_cast<double>(dates.SimulationEnd() - dates.SimulationStart()) *
	                         (1.0 - config.bulkload_portion));

	PersonOwnedRowCounts row_counts;
	{
		LdbcProfileTimer phase("append.person_owned");
		for (idx_t person_idx = 0; person_idx < persons.size(); person_idx++) {
			auto &person = persons[person_idx];
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
				AppendTimestampMsInt64Int32Row(interest_appender, person.creation_date, person.account_id, tag_id);
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
			if ((person_idx & 1023) == 0) {
				SetLdbcGenProgress(progress_state, LdbcGenProgressRange(25.0, 45.0, person_idx + 1, persons.size()));
			}
		}
	}
	SetLdbcGenProgress(progress_state, 45.0);

	vector<LdbcKnowsEdge> knows_edges;
	{
		LdbcProfileTimer phase("generate.knows");
		knows_edges = LdbcGenerateKnows(config, persons);
	}
	SetLdbcGenProgress(progress_state, 55.0);
	{
		LdbcProfileTimer phase("append.knows");
		for (idx_t edge_idx = 0; edge_idx < knows_edges.size(); edge_idx++) {
			auto &edge = knows_edges[edge_idx];
			if (edge.creation_date >= bulkload_threshold) {
				continue;
			}
			AppendTimestampMsInt64Int64Row(knows_appender, edge.creation_date, edge.person1_id, edge.person2_id);
			row_counts.knows++;
			if ((edge_idx & 4095) == 0) {
				SetLdbcGenProgress(progress_state, LdbcGenProgressRange(55.0, 65.0, edge_idx + 1, knows_edges.size()));
			}
		}
	}
	SetLdbcGenProgress(progress_state, 65.0);

	{
		LdbcProfileTimer phase("generate.forums");
		auto append_forum = [&](LdbcForum &&forum) {
			if (forum.creation_date < bulkload_threshold) {
				forum_appender->BeginRow();
				forum_appender->Append(Value::TIMESTAMP(LdbcTimestampMs(forum.creation_date)));
				forum_appender->Append<int64_t>(forum.id);
				forum_appender->Append(Value(forum.title));
				forum_appender->Append<int64_t>(forum.moderator_person_id);
				forum_appender->EndRow();
				row_counts.forums++;

				for (auto tag_id : forum.tags) {
					AppendTimestampMsInt64Int32Row(forum_tag_appender, forum.creation_date, forum.id, tag_id);
					row_counts.forum_tags++;
				}
			}

			for (auto &membership : forum.memberships) {
				if (membership.creation_date >= bulkload_threshold) {
					continue;
				}
				AppendTimestampMsInt64Int64Row(forum_member_appender, membership.creation_date, membership.forum_id,
				                               membership.person_id);
				row_counts.forum_members++;
			}

			for (auto &post : forum.posts) {
				if (post.creation_date >= bulkload_threshold) {
					continue;
				}
				post_appender->BeginRow();
				post_appender->Append(Value::TIMESTAMP(LdbcTimestampMs(post.creation_date)));
				post_appender->Append<int64_t>(post.id);
				post_appender->Append(post.image_file.empty() ? Value(LogicalType::VARCHAR) : Value(post.image_file));
				post_appender->Append(Value(post.location_ip));
				post_appender->Append(Value(post.browser_used));
				post_appender->Append(post.language.empty() ? Value(LogicalType::VARCHAR) : Value(post.language));
				post_appender->Append(post.image_file.empty() ? Value(post.content) : Value(LogicalType::VARCHAR));
				post_appender->Append<int32_t>(post.length);
				post_appender->Append<int64_t>(post.creator_person_id);
				post_appender->Append<int64_t>(post.forum_id);
				post_appender->Append<int64_t>(post.location_country_id);
				post_appender->EndRow();
				row_counts.posts++;

				for (auto tag_id : post.tags) {
					AppendTimestampMsInt64Int32Row(post_tag_appender, post.creation_date, post.id, tag_id);
					row_counts.post_tags++;
				}
			}

			for (auto &comment : forum.comments) {
				if (comment.creation_date >= bulkload_threshold) {
					continue;
				}
				comment_appender->BeginRow();
				comment_appender->Append(Value::TIMESTAMP(LdbcTimestampMs(comment.creation_date)));
				comment_appender->Append<int64_t>(comment.id);
				comment_appender->Append(Value(comment.location_ip));
				comment_appender->Append(Value(comment.browser_used));
				comment_appender->Append(Value(comment.content));
				comment_appender->Append<int32_t>(comment.length);
				comment_appender->Append<int64_t>(comment.creator_person_id);
				comment_appender->Append<int64_t>(comment.location_country_id);
				comment_appender->Append(comment.parent_post_id == -1 ? Value(LogicalType::BIGINT)
				                                                      : Value::BIGINT(comment.parent_post_id));
				comment_appender->Append(comment.parent_comment_id == -1 ? Value(LogicalType::BIGINT)
				                                                         : Value::BIGINT(comment.parent_comment_id));
				comment_appender->EndRow();
				row_counts.comments++;

				for (auto tag_id : comment.tags) {
					AppendTimestampMsInt64Int32Row(comment_tag_appender, comment.creation_date, comment.id, tag_id);
					row_counts.comment_tags++;
				}
			}

			for (auto &like : forum.post_likes) {
				if (like.creation_date >= bulkload_threshold) {
					continue;
				}
				AppendTimestampMsInt64Int64Row(post_like_appender, like.creation_date, like.person_id, like.message_id);
				row_counts.post_likes++;
			}

			for (auto &like : forum.comment_likes) {
				if (like.creation_date >= bulkload_threshold) {
					continue;
				}
				AppendTimestampMsInt64Int64Row(comment_like_appender, like.creation_date, like.person_id,
				                               like.message_id);
				row_counts.comment_likes++;
			}
		};
		LdbcGenerateForums(config, persons, knows_edges, append_forum, [&](idx_t done, idx_t total) {
			SetLdbcGenProgress(progress_state, LdbcGenProgressRange(65.0, 98.0, done, total));
		});
	}
	SetLdbcGenProgress(progress_state, 98.0);
	person_appender->Close();
	interest_appender.Close();
	study_appender->Close();
	work_appender->Close();
	knows_appender.Close();
	forum_appender->Close();
	forum_member_appender.Close();
	forum_tag_appender.Close();
	post_appender->Close();
	post_tag_appender.Close();
	comment_appender->Close();
	comment_tag_appender.Close();
	post_like_appender.Close();
	comment_like_appender.Close();
	return row_counts;
}

static unordered_map<string, idx_t> PopulateStaticTables(ClientContext &context, const LdbcGenBindData &bind_data,
                                                         LdbcGenGlobalState *progress_state = nullptr) {
	LdbcProfileTimer timer("populate.total");
	StaticDictionaryData data;
	{
		LdbcProfileTimer phase("load.static_dictionaries");
		data = LoadStaticDictionaryData(bind_data);
	}
	SetLdbcGenProgress(progress_state, 4.0);
	unordered_map<string, idx_t> row_counts;
	{
		LdbcProfileTimer phase("append.static.Place");
		row_counts["Place"] = AppendPlaces(context, bind_data, data);
	}
	SetLdbcGenProgress(progress_state, 8.0);
	{
		LdbcProfileTimer phase("append.static.TagClass");
		row_counts["TagClass"] = AppendTagClasses(context, bind_data, data);
	}
	SetLdbcGenProgress(progress_state, 12.0);
	{
		LdbcProfileTimer phase("append.static.Tag");
		row_counts["Tag"] = AppendTags(context, bind_data, data);
	}
	SetLdbcGenProgress(progress_state, 16.0);
	{
		LdbcProfileTimer phase("append.static.Organisation");
		row_counts["Organisation"] = AppendOrganisations(context, bind_data, data);
	}
	SetLdbcGenProgress(progress_state, 20.0);
	auto person_counts = AppendPersonOwnedTables(context, bind_data, progress_state);
	row_counts["Person"] = person_counts.persons;
	row_counts["Person_hasInterest_Tag"] = person_counts.interests;
	row_counts["Person_knows_Person"] = person_counts.knows;
	row_counts["Person_studyAt_University"] = person_counts.study_at;
	row_counts["Person_workAt_Company"] = person_counts.work_at;
	row_counts["Forum"] = person_counts.forums;
	row_counts["Forum_hasMember_Person"] = person_counts.forum_members;
	row_counts["Forum_hasTag_Tag"] = person_counts.forum_tags;
	row_counts["Post"] = person_counts.posts;
	row_counts["Post_hasTag_Tag"] = person_counts.post_tags;
	row_counts["Comment"] = person_counts.comments;
	row_counts["Comment_hasTag_Tag"] = person_counts.comment_tags;
	row_counts["Person_likes_Post"] = person_counts.post_likes;
	row_counts["Person_likes_Comment"] = person_counts.comment_likes;
	return row_counts;
}

static bool LdbcDirectPersonParquetRelation(const string &relation_name);

class LdbcLoadGenerator {
public:
	LdbcLoadGenerator(ClientContext &context_p, LdbcGenBindData bind_data_p, LdbcGenGlobalState &progress_state_p)
	    : context(context_p), bind_data(std::move(bind_data_p)), progress_state(progress_state_p) {
	}

	bool GenerateNext() {
		switch (phase) {
		case Phase::LOAD_STATIC: {
			LdbcScopedPhaseProfile phase_profile(*this, phase);
			LoadStatic();
		}
			phase = Phase::GENERATE_PERSONS;
			return false;
		case Phase::GENERATE_PERSONS:
			if (!RunProfiledPhase([&]() { return GeneratePersons(); })) {
				return false;
			}
			phase = Phase::APPEND_PERSONS;
			return false;
		case Phase::APPEND_PERSONS:
			if (!RunProfiledPhase([&]() {
				    auto done = AppendPersons();
				    if (done) {
					    FlushPersonRowsSnapshotParquet();
				    }
				    return done;
			    })) {
				return false;
			}
			phase = Phase::GENERATE_KNOWS;
			return false;
		case Phase::GENERATE_KNOWS:
			if (!RunProfiledPhase([&]() { return GenerateKnows(); })) {
				return false;
			}
			phase = Phase::APPEND_KNOWS;
			return false;
		case Phase::APPEND_KNOWS:
			if (!RunProfiledPhase([&]() {
				    auto done = AppendKnows();
				    if (done) {
					    FlushKnowsSnapshotParquet();
				    }
				    return done;
			    })) {
				return false;
			}
			phase = Phase::GENERATE_FORUMS;
			return false;
		case Phase::GENERATE_FORUMS:
			if (!RunProfiledPhase([&]() { return GenerateForums(); })) {
				return false;
			}
			phase = Phase::CLOSE;
			return false;
		case Phase::CLOSE: {
			LdbcScopedPhaseProfile phase_profile(*this, phase);
			Close();
		}
			PrintPhaseProfile();
			phase = Phase::DONE;
			return true;
		case Phase::DONE:
			return true;
		default:
			throw InternalException("Unexpected LDBC load generator phase");
		}
	}

	unordered_map<string, idx_t> ReleaseRowCounts() {
		return std::move(row_counts);
	}

private:
	enum class Phase : uint8_t {
		LOAD_STATIC,
		GENERATE_PERSONS,
		APPEND_PERSONS,
		GENERATE_KNOWS,
		APPEND_KNOWS,
		GENERATE_FORUMS,
		CLOSE,
		DONE
	};

	static constexpr idx_t PHASE_COUNT = 7;

	enum class ForumAppendPart : uint8_t { FORUM, MEMBERSHIPS, POSTS, COMMENTS, POST_LIKES, COMMENT_LIKES };

	static constexpr idx_t FORUM_APPEND_PART_COUNT = 6;

	static const char *PhaseName(Phase phase) {
		switch (phase) {
		case Phase::LOAD_STATIC:
			return "load_static";
		case Phase::GENERATE_PERSONS:
			return "generate_persons";
		case Phase::APPEND_PERSONS:
			return "append_persons";
		case Phase::GENERATE_KNOWS:
			return "generate_knows";
		case Phase::APPEND_KNOWS:
			return "append_knows";
		case Phase::GENERATE_FORUMS:
			return "generate_forums";
		case Phase::CLOSE:
			return "close_appenders";
		case Phase::DONE:
			return "done";
		default:
			return "unknown";
		}
	}

	static idx_t PhaseIndex(Phase phase) {
		return static_cast<idx_t>(phase);
	}

	static const char *ForumAppendPartName(ForumAppendPart part) {
		switch (part) {
		case ForumAppendPart::FORUM:
			return "forum";
		case ForumAppendPart::MEMBERSHIPS:
			return "memberships";
		case ForumAppendPart::POSTS:
			return "posts";
		case ForumAppendPart::COMMENTS:
			return "comments";
		case ForumAppendPart::POST_LIKES:
			return "post_likes";
		case ForumAppendPart::COMMENT_LIKES:
			return "comment_likes";
		default:
			return "unknown";
		}
	}

	class LdbcScopedPhaseProfile {
	public:
		LdbcScopedPhaseProfile(LdbcLoadGenerator &generator_p, Phase phase_p)
		    : generator(generator_p), phase(phase_p), enabled(LdbcPhaseProfileEnabled()) {
			if (enabled) {
				start = LdbcProfileSampleNow();
			}
		}

		~LdbcScopedPhaseProfile() {
			if (!enabled) {
				return;
			}
			generator.RecordPhaseProfile(phase, LdbcProfileSampleDelta(LdbcProfileSampleNow(), start));
		}

	private:
		LdbcLoadGenerator &generator;
		Phase phase;
		bool enabled;
		LdbcProfileSample start;
	};

	template <class FUNC>
	bool RunProfiledPhase(FUNC function) {
		LdbcScopedPhaseProfile phase_profile(*this, phase);
		return function();
	}

	void RecordPhaseProfile(Phase phase, const LdbcProfileSample &delta) {
		auto index = PhaseIndex(phase);
		if (index >= PHASE_COUNT) {
			return;
		}
		auto &target = phase_profiles[index];
		target.wall_ms += delta.wall_ms;
		target.user_ms += delta.user_ms;
		target.system_ms += delta.system_ms;
		phase_profile_calls[index]++;
	}

	void PrintPhaseProfile() const {
		if (!LdbcPhaseProfileEnabled()) {
			return;
		}
		std::cerr << "[ldbcgen] phase profile summary (wall/user/system ms, cpu%, calls)\n";
		LdbcProfileSample total;
		for (idx_t index = 0; index < PHASE_COUNT; index++) {
			total.wall_ms += phase_profiles[index].wall_ms;
			total.user_ms += phase_profiles[index].user_ms;
			total.system_ms += phase_profiles[index].system_ms;
		}
		for (idx_t index = 0; index < PHASE_COUNT; index++) {
			auto phase = static_cast<Phase>(index);
			auto &sample = phase_profiles[index];
			std::cerr << "[ldbcgen] phase." << PhaseName(phase) << ": wall=" << std::fixed << std::setprecision(3)
			          << sample.wall_ms << " user=" << sample.user_ms << " system=" << sample.system_ms
			          << " cpu=" << LdbcProfileCpuPercent(sample) << "% calls=" << phase_profile_calls[index] << "\n";
		}
		std::cerr << "[ldbcgen] phase.total: wall=" << std::fixed << std::setprecision(3) << total.wall_ms
		          << " user=" << total.user_ms << " system=" << total.system_ms
		          << " cpu=" << LdbcProfileCpuPercent(total) << "%\n";
		auto forum_phase = phase_profiles[PhaseIndex(Phase::GENERATE_FORUMS)];
		if (forum_append_profile.wall_ms > 0) {
			auto forum_non_append = LdbcProfileSampleDelta(forum_phase, forum_append_profile);
			std::cerr << "[ldbcgen] phase.generate_forums.append_callback: wall=" << std::fixed << std::setprecision(3)
			          << forum_append_profile.wall_ms << " user=" << forum_append_profile.user_ms
			          << " system=" << forum_append_profile.system_ms
			          << " cpu=" << LdbcProfileCpuPercent(forum_append_profile)
			          << "% calls=" << forum_append_profile_calls << "\n";
			std::cerr << "[ldbcgen] phase.generate_forums.non_append: wall=" << std::fixed << std::setprecision(3)
			          << forum_non_append.wall_ms << " user=" << forum_non_append.user_ms
			          << " system=" << forum_non_append.system_ms << " cpu=" << LdbcProfileCpuPercent(forum_non_append)
			          << "%\n";
		}
		if (forum_materialize_profile.wall_ms > 0) {
			std::cerr << "[ldbcgen] phase.generate_forums.materialize_chunks: wall=" << std::fixed
			          << std::setprecision(3) << forum_materialize_profile.wall_ms
			          << " user=" << forum_materialize_profile.user_ms
			          << " system=" << forum_materialize_profile.system_ms
			          << " cpu=" << LdbcProfileCpuPercent(forum_materialize_profile)
			          << "% calls=" << forum_materialize_profile_calls << "\n";
		}
		if (forum_append_chunks_profile.wall_ms > 0) {
			std::cerr << "[ldbcgen] phase.generate_forums.append_chunks: wall=" << std::fixed << std::setprecision(3)
			          << forum_append_chunks_profile.wall_ms << " user=" << forum_append_chunks_profile.user_ms
			          << " system=" << forum_append_chunks_profile.system_ms
			          << " cpu=" << LdbcProfileCpuPercent(forum_append_chunks_profile)
			          << "% calls=" << forum_append_chunks_profile_calls << "\n";
		}
		if (LdbcAppendProfileEnabled()) {
			for (idx_t index = 0; index < FORUM_APPEND_PART_COUNT; index++) {
				auto part = static_cast<ForumAppendPart>(index);
				std::cerr << "[ldbcgen] append.forums." << ForumAppendPartName(part) << ": wall=" << std::fixed
				          << std::setprecision(3) << forum_append_part_ms[index]
				          << " ms calls=" << forum_append_part_calls[index] << "\n";
			}
		}
		if (LdbcChunkProfileEnabled()) {
			std::cerr << "[ldbcgen] chunk.flush: wall=" << std::fixed << std::setprecision(3) << ldbc_chunk_flush_ms
			          << " ms calls=" << ldbc_chunk_flush_calls << "\n";
		}
	}

	class LdbcScopedForumAppendProfile {
	public:
		explicit LdbcScopedForumAppendProfile(LdbcLoadGenerator &generator_p)
		    : generator(generator_p), enabled(LdbcPhaseProfileEnabled()) {
			if (enabled) {
				start = LdbcProfileSampleNow();
			}
		}

		~LdbcScopedForumAppendProfile() {
			if (!enabled) {
				return;
			}
			auto delta = LdbcProfileSampleDelta(LdbcProfileSampleNow(), start);
			generator.forum_append_profile.wall_ms += delta.wall_ms;
			generator.forum_append_profile.user_ms += delta.user_ms;
			generator.forum_append_profile.system_ms += delta.system_ms;
			generator.forum_append_profile_calls++;
		}

	private:
		LdbcLoadGenerator &generator;
		bool enabled;
		LdbcProfileSample start;
	};

	class LdbcScopedForumAppendPartProfile {
	public:
		LdbcScopedForumAppendPartProfile(LdbcLoadGenerator &generator_p, ForumAppendPart part_p)
		    : generator(generator_p), part(part_p), enabled(LdbcAppendProfileEnabled()) {
			if (enabled) {
				start_ms = LdbcProfileNowMs();
			}
		}

		~LdbcScopedForumAppendPartProfile() {
			if (!enabled) {
				return;
			}
			auto index = static_cast<idx_t>(part);
			generator.forum_append_part_ms[index] += LdbcProfileNowMs() - start_ms;
			generator.forum_append_part_calls[index]++;
		}

	private:
		LdbcLoadGenerator &generator;
		ForumAppendPart part;
		bool enabled;
		double start_ms = 0;
	};

	struct ForumChunkBatch {
		vector<unique_ptr<DataChunk>> forums;
		vector<unique_ptr<DataChunk>> forum_tags;
		vector<unique_ptr<DataChunk>> forum_members;
		vector<unique_ptr<DataChunk>> posts;
		vector<unique_ptr<DataChunk>> post_tags;
		vector<unique_ptr<DataChunk>> comments;
		vector<unique_ptr<DataChunk>> comment_tags;
		vector<unique_ptr<DataChunk>> post_likes;
		vector<unique_ptr<DataChunk>> comment_likes;
		unique_ptr<ColumnDataCollection> forum_collection;
		unique_ptr<ColumnDataCollection> forum_tag_collection;
		unique_ptr<ColumnDataCollection> forum_member_collection;
		unique_ptr<ColumnDataCollection> post_collection;
		unique_ptr<ColumnDataCollection> post_tag_collection;
		unique_ptr<ColumnDataCollection> comment_collection;
		unique_ptr<ColumnDataCollection> comment_tag_collection;
		unique_ptr<ColumnDataCollection> post_like_collection;
		unique_ptr<ColumnDataCollection> comment_like_collection;
		vector<unique_ptr<DataChunk>> insert_forums;
		vector<unique_ptr<DataChunk>> insert_forum_tags;
		vector<unique_ptr<DataChunk>> insert_forum_members;
		vector<unique_ptr<DataChunk>> insert_posts;
		vector<unique_ptr<DataChunk>> insert_post_tags;
		vector<unique_ptr<DataChunk>> insert_comments;
		vector<unique_ptr<DataChunk>> insert_comment_tags;
		vector<unique_ptr<DataChunk>> insert_post_likes;
		vector<unique_ptr<DataChunk>> insert_comment_likes;
		vector<unique_ptr<DataChunk>> delete_forums;
		vector<unique_ptr<DataChunk>> delete_forum_members;
		vector<unique_ptr<DataChunk>> delete_posts;
		vector<unique_ptr<DataChunk>> delete_comments;
		vector<unique_ptr<DataChunk>> delete_post_likes;
		vector<unique_ptr<DataChunk>> delete_comment_likes;
		PersonOwnedRowCounts counts;
	};

	struct PersonSnapshotChunkBatch {
		vector<unique_ptr<DataChunk>> persons;
		vector<unique_ptr<DataChunk>> interests;
		vector<unique_ptr<DataChunk>> study_at;
		vector<unique_ptr<DataChunk>> work_at;
		vector<unique_ptr<DataChunk>> knows;
		unique_ptr<ColumnDataCollection> persons_collection;
		unique_ptr<ColumnDataCollection> interests_collection;
		unique_ptr<ColumnDataCollection> study_at_collection;
		unique_ptr<ColumnDataCollection> work_at_collection;
		unique_ptr<ColumnDataCollection> knows_collection;
	};

	struct ForumChunkBuilders {
		LdbcChunkBuilder forums;
		LdbcChunkBuilder forum_tags;
		LdbcChunkBuilder forum_members;
		LdbcChunkBuilder posts;
		LdbcChunkBuilder post_tags;
		LdbcChunkBuilder comments;
		LdbcChunkBuilder comment_tags;
		LdbcChunkBuilder post_likes;
		LdbcChunkBuilder comment_likes;
		LdbcChunkBuilder insert_forums;
		LdbcChunkBuilder insert_forum_tags;
		LdbcChunkBuilder insert_forum_members;
		LdbcChunkBuilder insert_posts;
		LdbcChunkBuilder insert_post_tags;
		LdbcChunkBuilder insert_comments;
		LdbcChunkBuilder insert_comment_tags;
		LdbcChunkBuilder insert_post_likes;
		LdbcChunkBuilder insert_comment_likes;
		LdbcChunkBuilder delete_forums;
		LdbcChunkBuilder delete_forum_members;
		LdbcChunkBuilder delete_posts;
		LdbcChunkBuilder delete_comments;
		LdbcChunkBuilder delete_post_likes;
		LdbcChunkBuilder delete_comment_likes;

		ForumChunkBuilders(ClientContext *snapshot_context)
		    : forums({LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::BIGINT},
		             snapshot_context),
		      forum_tags({LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::INTEGER}, snapshot_context),
		      forum_members({LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::BIGINT}, snapshot_context),
		      posts({LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
		             LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER,
		             LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
		            snapshot_context),
		      post_tags({LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::INTEGER}, snapshot_context),
		      comments({LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
		                LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::INTEGER,
		                LogicalType::BIGINT, LogicalType::BIGINT},
		               snapshot_context),
		      comment_tags({LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::INTEGER}, snapshot_context),
		      post_likes({LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::BIGINT}, snapshot_context),
		      comment_likes({LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::BIGINT}, snapshot_context),
		      insert_forums({LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::VARCHAR,
		                     LogicalType::BIGINT}),
		      insert_forum_tags(
		          {LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::INTEGER}),
		      insert_forum_members(
		          {LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::BIGINT}),
		      insert_posts({LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::VARCHAR,
		                    LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
		                    LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT}),
		      insert_post_tags(
		          {LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::INTEGER}),
		      insert_comments({LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::VARCHAR,
		                       LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::BIGINT,
		                       LogicalType::INTEGER, LogicalType::BIGINT, LogicalType::BIGINT}),
		      insert_comment_tags(
		          {LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::INTEGER}),
		      insert_post_likes(
		          {LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::BIGINT}),
		      insert_comment_likes(
		          {LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::BIGINT}),
		      delete_forums({LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT}),
		      delete_forum_members(
		          {LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::BIGINT}),
		      delete_posts({LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT}),
		      delete_comments({LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT}),
		      delete_post_likes(
		          {LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::BIGINT}),
		      delete_comment_likes(
		          {LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::BIGINT}) {
		}

		ForumChunkBatch Finish() {
			ForumChunkBatch batch;
			batch.forums = forums.Finish();
			batch.forum_tags = forum_tags.Finish();
			batch.forum_members = forum_members.Finish();
			batch.posts = posts.Finish();
			batch.post_tags = post_tags.Finish();
			batch.comments = comments.Finish();
			batch.comment_tags = comment_tags.Finish();
			batch.post_likes = post_likes.Finish();
			batch.comment_likes = comment_likes.Finish();
			batch.forum_collection = forums.TakeCollection();
			batch.forum_tag_collection = forum_tags.TakeCollection();
			batch.forum_member_collection = forum_members.TakeCollection();
			batch.post_collection = posts.TakeCollection();
			batch.post_tag_collection = post_tags.TakeCollection();
			batch.comment_collection = comments.TakeCollection();
			batch.comment_tag_collection = comment_tags.TakeCollection();
			batch.post_like_collection = post_likes.TakeCollection();
			batch.comment_like_collection = comment_likes.TakeCollection();
			batch.insert_forums = insert_forums.Finish();
			batch.insert_forum_tags = insert_forum_tags.Finish();
			batch.insert_forum_members = insert_forum_members.Finish();
			batch.insert_posts = insert_posts.Finish();
			batch.insert_post_tags = insert_post_tags.Finish();
			batch.insert_comments = insert_comments.Finish();
			batch.insert_comment_tags = insert_comment_tags.Finish();
			batch.insert_post_likes = insert_post_likes.Finish();
			batch.insert_comment_likes = insert_comment_likes.Finish();
			batch.delete_forums = delete_forums.Finish();
			batch.delete_forum_members = delete_forum_members.Finish();
			batch.delete_posts = delete_posts.Finish();
			batch.delete_comments = delete_comments.Finish();
			batch.delete_post_likes = delete_post_likes.Finish();
			batch.delete_comment_likes = delete_comment_likes.Finish();
			batch.counts = counts;
			return batch;
		}

		PersonOwnedRowCounts counts;
	};

	bool UsePrematerializedForumChunks() const {
		return chunk_message_appenders &&
		       (std::getenv("LDBCGEN_FORUM_PREMATERIALIZE") != nullptr || bind_data.direct_forum_parquet);
	}

	bool UseForumBlockPrematerialization() const {
		return UsePrematerializedForumChunks() && bind_data.threads > 1;
	}

	bool UseDirectForumParquet() const {
		return bind_data.direct_forum_parquet;
	}

	bool UseDirectPersonParquet() const {
		return bind_data.direct_person_parquet;
	}

	bool UseDirectForumUpdateParquet() const {
		return bind_data.direct_forum_update_parquet;
	}

	bool UseForumUpdateCopyTableFunction() const {
		return bind_data.forum_update_copy_table_function;
	}

	static bool IsForumGeneratedRelation(const string &relation_name) {
		return relation_name == "Forum" || relation_name == "Forum_hasTag_Tag" ||
		       relation_name == "Forum_hasMember_Person" || relation_name == "Post" ||
		       relation_name == "Post_hasTag_Tag" || relation_name == "Comment" ||
		       relation_name == "Comment_hasTag_Tag" || relation_name == "Person_likes_Post" ||
		       relation_name == "Person_likes_Comment";
	}

	static bool IsPersonSnapshotDirectRelation(const string &relation_name) {
		return LdbcDirectPersonParquetRelation(relation_name);
	}

	static vector<LogicalType> ForumInsertTypes(const string &relation_name) {
		if (relation_name == "Forum") {
			return {LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::VARCHAR,
			        LogicalType::BIGINT};
		}
		if (relation_name == "Post") {
			return {LogicalType::DATE,    LogicalType::TIMESTAMP_MS, LogicalType::BIGINT,  LogicalType::VARCHAR,
			        LogicalType::VARCHAR, LogicalType::VARCHAR,      LogicalType::VARCHAR, LogicalType::VARCHAR,
			        LogicalType::INTEGER, LogicalType::BIGINT,       LogicalType::BIGINT,  LogicalType::BIGINT};
		}
		if (relation_name == "Comment") {
			return {LogicalType::DATE,    LogicalType::TIMESTAMP_MS, LogicalType::BIGINT,  LogicalType::VARCHAR,
			        LogicalType::VARCHAR, LogicalType::VARCHAR,      LogicalType::INTEGER, LogicalType::BIGINT,
			        LogicalType::INTEGER, LogicalType::BIGINT,       LogicalType::BIGINT};
		}
		if (relation_name == "Forum_hasTag_Tag" || relation_name == "Post_hasTag_Tag" ||
		    relation_name == "Comment_hasTag_Tag") {
			return {LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::INTEGER};
		}
		return {LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::BIGINT};
	}

	static vector<LogicalType> ForumDeleteTypes(const string &relation_name) {
		if (relation_name == "Forum" || relation_name == "Post" || relation_name == "Comment") {
			return {LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT};
		}
		return {LogicalType::DATE, LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::BIGINT};
	}

	void AppendForumRange(const vector<LdbcForum> &forums, idx_t start, idx_t end, ForumChunkBuilders &builders) const {
		for (idx_t forum_idx = start; forum_idx < end; forum_idx++) {
			auto &forum = forums[forum_idx];
			if (IsSnapshotRow(forum.creation_date)) {
				AppendForumRow(builders.forums, forum);
				builders.counts.forums++;
				for (auto tag_id : forum.tags) {
					AppendTimestampMsInt64Int32Row(builders.forum_tags, forum.creation_date, forum.id, tag_id);
					builders.counts.forum_tags++;
				}
			} else if (IsInsertRow(forum.creation_date)) {
				AppendForumInsertRow(builders.insert_forums, forum);
				for (auto tag_id : forum.tags) {
					AppendTimestampMsInt64Int32InsertRow(builders.insert_forum_tags, forum.creation_date, forum.id,
					                                     tag_id);
				}
			}
			if (IsDeleteRow(forum.deletion_date, forum.explicitly_deleted)) {
				AppendNodeDeleteRow(builders.delete_forums, forum.deletion_date, forum.id);
			}

			for (auto &membership : forum.memberships) {
				if (IsSnapshotRow(membership.creation_date)) {
					AppendTimestampMsInt64Int64Row(builders.forum_members, membership.creation_date,
					                               membership.forum_id, membership.person_id);
					builders.counts.forum_members++;
				} else if (IsInsertRow(membership.creation_date)) {
					AppendTimestampMsInt64Int64InsertRow(builders.insert_forum_members, membership.creation_date,
					                                     membership.forum_id, membership.person_id);
				}
				if (IsDeleteRow(membership.deletion_date, membership.explicitly_deleted)) {
					AppendEdgeDeleteRow(builders.delete_forum_members, membership.deletion_date, membership.forum_id,
					                    membership.person_id);
				}
			}

			for (auto &post : forum.posts) {
				if (IsSnapshotRow(post.creation_date)) {
					AppendPostRow(builders.posts, post);
					builders.counts.posts++;
					for (auto tag_id : post.tags) {
						AppendTimestampMsInt64Int32Row(builders.post_tags, post.creation_date, post.id, tag_id);
						builders.counts.post_tags++;
					}
				} else if (IsInsertRow(post.creation_date)) {
					AppendPostInsertRow(builders.insert_posts, post);
					for (auto tag_id : post.tags) {
						AppendTimestampMsInt64Int32InsertRow(builders.insert_post_tags, post.creation_date, post.id,
						                                     tag_id);
					}
				}
				if (IsDeleteRow(post.deletion_date, post.explicitly_deleted)) {
					AppendNodeDeleteRow(builders.delete_posts, post.deletion_date, post.id);
				}
			}

			for (auto &comment : forum.comments) {
				if (IsSnapshotRow(comment.creation_date)) {
					AppendCommentRow(builders.comments, comment);
					builders.counts.comments++;
					for (auto tag_id : comment.tags) {
						AppendTimestampMsInt64Int32Row(builders.comment_tags, comment.creation_date, comment.id,
						                               tag_id);
						builders.counts.comment_tags++;
					}
				} else if (IsInsertRow(comment.creation_date)) {
					AppendCommentInsertRow(builders.insert_comments, comment);
					for (auto tag_id : comment.tags) {
						AppendTimestampMsInt64Int32InsertRow(builders.insert_comment_tags, comment.creation_date,
						                                     comment.id, tag_id);
					}
				}
				if (IsDeleteRow(comment.deletion_date, comment.explicitly_deleted)) {
					AppendNodeDeleteRow(builders.delete_comments, comment.deletion_date, comment.id);
				}
			}

			for (auto &like : forum.post_likes) {
				if (IsSnapshotRow(like.creation_date)) {
					AppendTimestampMsInt64Int64Row(builders.post_likes, like.creation_date, like.person_id,
					                               like.message_id);
					builders.counts.post_likes++;
				} else if (IsInsertRow(like.creation_date)) {
					AppendTimestampMsInt64Int64InsertRow(builders.insert_post_likes, like.creation_date, like.person_id,
					                                     like.message_id);
				}
				if (IsDeleteRow(like.deletion_date, like.explicitly_deleted)) {
					AppendEdgeDeleteRow(builders.delete_post_likes, like.deletion_date, like.person_id,
					                    like.message_id);
				}
			}

			for (auto &like : forum.comment_likes) {
				if (IsSnapshotRow(like.creation_date)) {
					AppendTimestampMsInt64Int64Row(builders.comment_likes, like.creation_date, like.person_id,
					                               like.message_id);
					builders.counts.comment_likes++;
				} else if (IsInsertRow(like.creation_date)) {
					AppendTimestampMsInt64Int64InsertRow(builders.insert_comment_likes, like.creation_date,
					                                     like.person_id, like.message_id);
				}
				if (IsDeleteRow(like.deletion_date, like.explicitly_deleted)) {
					AppendEdgeDeleteRow(builders.delete_comment_likes, like.deletion_date, like.person_id,
					                    like.message_id);
				}
			}
		}
	}

	void MaterializeForumRange(const vector<LdbcForum> &forums, idx_t start, idx_t end, ForumChunkBatch &target) const {
		ForumChunkBuilders builders(UseDirectForumParquet() && LdbcDirectSnapshotCollectionsEnabled() ? &context
		                                                                                              : nullptr);
		AppendForumRange(forums, start, end, builders);
		target = builders.Finish();
	}

	vector<ForumChunkBatch> MaterializeForumChunks(vector<LdbcForum> &forums) {
		if (forums.empty()) {
			return {};
		}
		auto worker_count = MinValue<idx_t>(bind_data.threads, forums.size());
		vector<ForumChunkBatch> batches(worker_count);
		if (worker_count <= 1) {
			MaterializeForumRange(forums, 0, forums.size(), batches[0]);
			return batches;
		}

		class LdbcForumMaterializeTask : public BaseExecutorTask {
		public:
			LdbcForumMaterializeTask(TaskExecutor &executor, LdbcLoadGenerator &loader, const vector<LdbcForum> &forums,
			                         vector<ForumChunkBatch> &batches, std::atomic<idx_t> &next_range,
			                         idx_t range_count)
			    : BaseExecutorTask(executor), loader(loader), forums(forums), batches(batches), next_range(next_range),
			      range_count(range_count) {
			}

			void ExecuteTask() override {
				while (true) {
					auto range_idx = next_range.fetch_add(1);
					if (range_idx >= range_count) {
						break;
					}
					auto start = (forums.size() * range_idx) / range_count;
					auto end = (forums.size() * (range_idx + 1)) / range_count;
					loader.MaterializeForumRange(forums, start, end, batches[range_idx]);
				}
			}

			string TaskType() const override {
				return "LdbcForumMaterializeTask";
			}

		private:
			LdbcLoadGenerator &loader;
			const vector<LdbcForum> &forums;
			vector<ForumChunkBatch> &batches;
			std::atomic<idx_t> &next_range;
			idx_t range_count;
		};

		std::atomic<idx_t> next_range(0);
		TaskExecutor executor(context);
		for (idx_t worker_idx = 0; worker_idx < worker_count; worker_idx++) {
			executor.ScheduleTask(
			    make_uniq<LdbcForumMaterializeTask>(executor, *this, forums, batches, next_range, worker_count));
		}
		executor.WorkOnTasks();
		return batches;
	}

	void MaterializeForumBlock(idx_t block_id, idx_t block_start, idx_t block_end, vector<LdbcForum> &forums) {
		(void)block_start;
		(void)block_end;
		ForumChunkBatch batch;
		auto materialize_start = LdbcPhaseProfileEnabled() ? LdbcProfileSampleNow() : LdbcProfileSample();
		MaterializeForumRange(forums, 0, forums.size(), batch);
		forums.clear();
		forums.shrink_to_fit();
		auto materialize_delta = LdbcPhaseProfileEnabled()
		                             ? LdbcProfileSampleDelta(LdbcProfileSampleNow(), materialize_start)
		                             : LdbcProfileSample();
		if (UseDirectForumParquet()) {
			WriteForumSnapshotParquetBlock(block_id, batch);
		}
		QueueMaterializedForumBatch(block_id, std::move(batch), materialize_delta);
	}

	void MaterializeForumSlice(idx_t slice_id, idx_t slice_start, idx_t slice_end, vector<LdbcForum> &forums,
	                           bool finished) {
		(void)slice_start;
		(void)slice_end;
		ForumChunkBuilders *builders;
		{
			std::lock_guard<std::mutex> lock(active_forum_chunk_builders_lock);
			auto entry = active_forum_chunk_builders.find(slice_id);
			if (entry == active_forum_chunk_builders.end()) {
				auto snapshot_context =
				    UseDirectForumParquet() && LdbcDirectSnapshotCollectionsEnabled() ? &context : nullptr;
				auto result = active_forum_chunk_builders.emplace(
				    slice_id, make_uniq<ForumChunkBuilders>(snapshot_context));
				entry = result.first;
			}
			builders = entry->second.get();
		}
		auto materialize_start = LdbcPhaseProfileEnabled() ? LdbcProfileSampleNow() : LdbcProfileSample();
		AppendForumRange(forums, 0, forums.size(), *builders);
		forums.clear();
		auto materialize_delta = LdbcPhaseProfileEnabled()
		                             ? LdbcProfileSampleDelta(LdbcProfileSampleNow(), materialize_start)
		                             : LdbcProfileSample();
		if (!finished) {
			RecordForumMaterializeProfile(materialize_delta);
			return;
		}
		unique_ptr<ForumChunkBuilders> completed_builders;
		{
			std::lock_guard<std::mutex> lock(active_forum_chunk_builders_lock);
			auto entry = active_forum_chunk_builders.find(slice_id);
			if (entry == active_forum_chunk_builders.end()) {
				throw InternalException("Missing forum slice builders for slice %llu",
				                        static_cast<unsigned long long>(slice_id));
			}
			completed_builders = std::move(entry->second);
			active_forum_chunk_builders.erase(entry);
		}
		auto batch = completed_builders->Finish();
		if (UseDirectForumParquet()) {
			WriteForumSnapshotParquetBlock(slice_id, batch);
		}
		QueueMaterializedForumBatch(slice_id, std::move(batch), materialize_delta);
	}

	void RecordForumMaterializeProfile(const LdbcProfileSample &materialize_delta) {
		if (!LdbcPhaseProfileEnabled()) {
			return;
		}
		std::lock_guard<std::mutex> lock(pending_forum_chunk_batches_lock);
		forum_materialize_profile.wall_ms += materialize_delta.wall_ms;
		forum_materialize_profile.user_ms += materialize_delta.user_ms;
		forum_materialize_profile.system_ms += materialize_delta.system_ms;
		forum_materialize_profile_calls++;
	}

	void QueueMaterializedForumBatch(idx_t block_id, ForumChunkBatch batch,
	                                 const LdbcProfileSample &materialize_delta) {
		std::lock_guard<std::mutex> lock(pending_forum_chunk_batches_lock);
		if (LdbcPhaseProfileEnabled()) {
			forum_materialize_profile.wall_ms += materialize_delta.wall_ms;
			forum_materialize_profile.user_ms += materialize_delta.user_ms;
			forum_materialize_profile.system_ms += materialize_delta.system_ms;
			forum_materialize_profile_calls++;
		}
		auto result = pending_forum_chunk_batches.emplace(block_id, std::move(batch));
		if (!result.second) {
			throw InternalException("Duplicate materialized forum block %llu",
			                        static_cast<unsigned long long>(block_id));
		}
	}

	void WriteSnapshotParquetBlock(idx_t block_id, const string &relation_name, vector<unique_ptr<DataChunk>> &chunks) {
		if (chunks.empty()) {
			return;
		}
		auto relation_index = LdbcRelationIndexByName(relation_name);
		auto &relation = LdbcRelationAt(relation_index);
		auto path = LdbcSparkRelationBlockPartPath(context, bind_data, "initial_snapshot", relation, block_id);
		WriteLdbcChunksToParquet(context, path, LdbcRelationTypes(relation_name),
		                         LdbcRelationColumnNames(relation_name), chunks);
		chunks.clear();
		chunks.shrink_to_fit();
	}

	void WriteSnapshotParquetBlock(idx_t block_id, const string &relation_name,
	                               unique_ptr<ColumnDataCollection> &collection) {
		if (!collection || collection->Count() == 0) {
			return;
		}
		auto relation_index = LdbcRelationIndexByName(relation_name);
		auto &relation = LdbcRelationAt(relation_index);
		auto path = LdbcSparkRelationBlockPartPath(context, bind_data, "initial_snapshot", relation, block_id);
		WriteLdbcCollectionToParquet(context, path, LdbcRelationTypes(relation_name),
		                             LdbcRelationColumnNames(relation_name), *collection);
		collection.reset();
	}

	void WriteForumSnapshotParquetBlock(idx_t block_id, ForumChunkBatch &batch) {
		WriteSnapshotParquetBlock(block_id, "Forum", batch.forum_collection);
		WriteSnapshotParquetBlock(block_id, "Forum_hasTag_Tag", batch.forum_tag_collection);
		WriteSnapshotParquetBlock(block_id, "Forum_hasMember_Person", batch.forum_member_collection);
		WriteSnapshotParquetBlock(block_id, "Post", batch.post_collection);
		WriteSnapshotParquetBlock(block_id, "Post_hasTag_Tag", batch.post_tag_collection);
		WriteSnapshotParquetBlock(block_id, "Comment", batch.comment_collection);
		WriteSnapshotParquetBlock(block_id, "Comment_hasTag_Tag", batch.comment_tag_collection);
		WriteSnapshotParquetBlock(block_id, "Person_likes_Post", batch.post_like_collection);
		WriteSnapshotParquetBlock(block_id, "Person_likes_Comment", batch.comment_like_collection);
		WriteSnapshotParquetBlock(block_id, "Forum", batch.forums);
		WriteSnapshotParquetBlock(block_id, "Forum_hasTag_Tag", batch.forum_tags);
		WriteSnapshotParquetBlock(block_id, "Forum_hasMember_Person", batch.forum_members);
		WriteSnapshotParquetBlock(block_id, "Post", batch.posts);
		WriteSnapshotParquetBlock(block_id, "Post_hasTag_Tag", batch.post_tags);
		WriteSnapshotParquetBlock(block_id, "Comment", batch.comments);
		WriteSnapshotParquetBlock(block_id, "Comment_hasTag_Tag", batch.comment_tags);
		WriteSnapshotParquetBlock(block_id, "Person_likes_Post", batch.post_likes);
		WriteSnapshotParquetBlock(block_id, "Person_likes_Comment", batch.comment_likes);
	}

	void EnsurePersonSnapshotBuilders() {
		if (!UseDirectPersonParquet() || person_snapshot_builders_initialized) {
			return;
		}
		auto collection_context = LdbcDirectSnapshotCollectionsEnabled() ? &context : nullptr;
		person_builder = make_uniq<LdbcChunkBuilder>(LdbcRelationTypes("Person"), collection_context);
		interest_builder = make_uniq<LdbcChunkBuilder>(LdbcRelationTypes("Person_hasInterest_Tag"), collection_context);
		study_builder = make_uniq<LdbcChunkBuilder>(LdbcRelationTypes("Person_studyAt_University"), collection_context);
		work_builder = make_uniq<LdbcChunkBuilder>(LdbcRelationTypes("Person_workAt_Company"), collection_context);
		knows_builder = make_uniq<LdbcChunkBuilder>(LdbcRelationTypes("Person_knows_Person"), collection_context);
		person_snapshot_builders_initialized = true;
	}

	void FlushPersonRowsSnapshotParquet() {
		if (!UseDirectPersonParquet() || person_rows_snapshot_parquet_flushed) {
			return;
		}
		PersonSnapshotChunkBatch batch;
		if (person_builder) {
			batch.persons = person_builder->Finish();
		}
		if (interest_builder) {
			batch.interests = interest_builder->Finish();
		}
		if (study_builder) {
			batch.study_at = study_builder->Finish();
		}
		if (work_builder) {
			batch.work_at = work_builder->Finish();
		}
		batch.persons_collection = person_builder ? person_builder->TakeCollection() : nullptr;
		batch.interests_collection = interest_builder ? interest_builder->TakeCollection() : nullptr;
		batch.study_at_collection = study_builder ? study_builder->TakeCollection() : nullptr;
		batch.work_at_collection = work_builder ? work_builder->TakeCollection() : nullptr;
		WriteSnapshotParquetBlock(0, "Person", batch.persons_collection);
		WriteSnapshotParquetBlock(0, "Person_hasInterest_Tag", batch.interests_collection);
		WriteSnapshotParquetBlock(0, "Person_studyAt_University", batch.study_at_collection);
		WriteSnapshotParquetBlock(0, "Person_workAt_Company", batch.work_at_collection);
		WriteSnapshotParquetBlock(0, "Person", batch.persons);
		WriteSnapshotParquetBlock(0, "Person_hasInterest_Tag", batch.interests);
		WriteSnapshotParquetBlock(0, "Person_studyAt_University", batch.study_at);
		WriteSnapshotParquetBlock(0, "Person_workAt_Company", batch.work_at);
		person_builder.reset();
		interest_builder.reset();
		study_builder.reset();
		work_builder.reset();
		person_rows_snapshot_parquet_flushed = true;
	}

	void FlushKnowsSnapshotParquet() {
		if (!UseDirectPersonParquet() || knows_snapshot_parquet_flushed) {
			return;
		}
		PersonSnapshotChunkBatch batch;
		if (knows_builder) {
			batch.knows = knows_builder->Finish();
		}
		batch.knows_collection = knows_builder ? knows_builder->TakeCollection() : nullptr;
		WriteSnapshotParquetBlock(0, "Person_knows_Person", batch.knows_collection);
		WriteSnapshotParquetBlock(0, "Person_knows_Person", batch.knows);
		knows_builder.reset();
		knows_snapshot_parquet_flushed = true;
	}

	static vector<LogicalType> DropPartitionColumn(vector<LogicalType> types) {
		D_ASSERT(!types.empty());
		types.erase(types.begin());
		return types;
	}

	static vector<string> DropPartitionColumn(vector<string> names) {
		D_ASSERT(!names.empty());
		names.erase(names.begin());
		return names;
	}

	static unique_ptr<DataChunk> SliceUpdateChunkWithoutBatchId(DataChunk &source, const vector<LogicalType> &types,
	                                                            const SelectionVector &selection, idx_t count) {
		auto result = make_uniq<DataChunk>();
		result->Initialize(Allocator::DefaultAllocator(), types);
		for (idx_t column_idx = 0; column_idx < types.size(); column_idx++) {
			result->data[column_idx].Slice(source.data[column_idx + 1], selection, count);
		}
		result->SetCardinality(count);
		return result;
	}

	void WritePartitionedUpdateParquetBlock(idx_t block_id, const string &operation, const string &relation_name,
	                                        vector<unique_ptr<DataChunk>> &chunks, vector<LogicalType> table_types,
	                                        vector<string> table_names) {
		if (chunks.empty()) {
			return;
		}
		auto relation_index = LdbcRelationIndexByName(relation_name);
		auto &relation = LdbcRelationAt(relation_index);
		auto file_types = DropPartitionColumn(std::move(table_types));
		auto file_names = DropPartitionColumn(std::move(table_names));
		std::map<int32_t, vector<unique_ptr<DataChunk>>> partition_chunks;
		for (auto &chunk : chunks) {
			chunk->Flatten();
			auto dates = FlatVector::GetData<date_t>(chunk->data[0]);
			std::map<int32_t, vector<sel_t>> rows_by_partition;
			for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
				rows_by_partition[dates[row_idx].days].push_back(static_cast<sel_t>(row_idx));
			}
			for (auto &entry : rows_by_partition) {
				SelectionVector selection(entry.second.size());
				for (idx_t selection_idx = 0; selection_idx < entry.second.size(); selection_idx++) {
					selection.set_index(selection_idx, entry.second[selection_idx]);
				}
				partition_chunks[entry.first].push_back(
				    SliceUpdateChunkWithoutBatchId(*chunk, file_types, selection, entry.second.size()));
			}
		}
		for (auto &entry : partition_chunks) {
			auto partition_value = Date::ToString(date_t(entry.first));
			auto path = LdbcSparkRelationPartitionBlockPartPath(context, bind_data, operation, relation,
			                                                    partition_value, block_id);
			WriteLdbcChunksToParquet(context, path, file_types, file_names, entry.second);
		}
		chunks.clear();
		chunks.shrink_to_fit();
	}

	void WriteInsertParquetBlock(idx_t block_id, const string &relation_name, vector<unique_ptr<DataChunk>> &chunks) {
		WritePartitionedUpdateParquetBlock(block_id, "inserts", relation_name, chunks, ForumInsertTypes(relation_name),
		                                   LdbcInsertColumnNames(relation_name));
	}

	void WriteDeleteParquetBlock(idx_t block_id, const string &relation_name, vector<unique_ptr<DataChunk>> &chunks) {
		WritePartitionedUpdateParquetBlock(block_id, "deletes", relation_name, chunks, ForumDeleteTypes(relation_name),
		                                   LdbcDeleteColumnNames(relation_name));
	}

	void WriteUpdateParquetRelation(idx_t block_id, const string &operation, const string &relation_name,
	                                vector<unique_ptr<DataChunk>> &chunks) {
		if (operation == "inserts") {
			WriteInsertParquetBlock(block_id, relation_name, chunks);
		} else if (operation == "deletes") {
			WriteDeleteParquetBlock(block_id, relation_name, chunks);
		} else {
			throw InternalException("Unexpected LDBC update operation: %s", operation);
		}
	}

	void WriteForumUpdateParquetBlock(idx_t block_id, ForumChunkBatch &batch) {
		if (!bind_data.emit_updates) {
			return;
		}
		WriteInsertParquetBlock(block_id, "Forum", batch.insert_forums);
		WriteInsertParquetBlock(block_id, "Forum_hasTag_Tag", batch.insert_forum_tags);
		WriteInsertParquetBlock(block_id, "Forum_hasMember_Person", batch.insert_forum_members);
		WriteInsertParquetBlock(block_id, "Post", batch.insert_posts);
		WriteInsertParquetBlock(block_id, "Post_hasTag_Tag", batch.insert_post_tags);
		WriteInsertParquetBlock(block_id, "Comment", batch.insert_comments);
		WriteInsertParquetBlock(block_id, "Comment_hasTag_Tag", batch.insert_comment_tags);
		WriteInsertParquetBlock(block_id, "Person_likes_Post", batch.insert_post_likes);
		WriteInsertParquetBlock(block_id, "Person_likes_Comment", batch.insert_comment_likes);
		WriteDeleteParquetBlock(block_id, "Forum", batch.delete_forums);
		WriteDeleteParquetBlock(block_id, "Forum_hasMember_Person", batch.delete_forum_members);
		WriteDeleteParquetBlock(block_id, "Post", batch.delete_posts);
		WriteDeleteParquetBlock(block_id, "Comment", batch.delete_comments);
		WriteDeleteParquetBlock(block_id, "Person_likes_Post", batch.delete_post_likes);
		WriteDeleteParquetBlock(block_id, "Person_likes_Comment", batch.delete_comment_likes);
	}

	static void MoveChunks(vector<unique_ptr<DataChunk>> &target, vector<unique_ptr<DataChunk>> &source) {
		if (source.empty()) {
			return;
		}
		target.reserve(target.size() + source.size());
		for (auto &chunk : source) {
			target.push_back(std::move(chunk));
		}
		source.clear();
	}

	void CollectForumUpdateChunks(ForumChunkBatch &batch) {
		MoveChunks(pending_forum_update_batch.insert_forums, batch.insert_forums);
		MoveChunks(pending_forum_update_batch.insert_forum_tags, batch.insert_forum_tags);
		MoveChunks(pending_forum_update_batch.insert_forum_members, batch.insert_forum_members);
		MoveChunks(pending_forum_update_batch.insert_posts, batch.insert_posts);
		MoveChunks(pending_forum_update_batch.insert_post_tags, batch.insert_post_tags);
		MoveChunks(pending_forum_update_batch.insert_comments, batch.insert_comments);
		MoveChunks(pending_forum_update_batch.insert_comment_tags, batch.insert_comment_tags);
		MoveChunks(pending_forum_update_batch.insert_post_likes, batch.insert_post_likes);
		MoveChunks(pending_forum_update_batch.insert_comment_likes, batch.insert_comment_likes);
		MoveChunks(pending_forum_update_batch.delete_forums, batch.delete_forums);
		MoveChunks(pending_forum_update_batch.delete_forum_members, batch.delete_forum_members);
		MoveChunks(pending_forum_update_batch.delete_posts, batch.delete_posts);
		MoveChunks(pending_forum_update_batch.delete_comments, batch.delete_comments);
		MoveChunks(pending_forum_update_batch.delete_post_likes, batch.delete_post_likes);
		MoveChunks(pending_forum_update_batch.delete_comment_likes, batch.delete_comment_likes);
	}

	class LdbcForumUpdateParquetTask : public BaseExecutorTask {
	public:
		LdbcForumUpdateParquetTask(TaskExecutor &executor, LdbcLoadGenerator &loader, idx_t block_id, string operation,
		                           string relation_name, vector<unique_ptr<DataChunk>> &chunks)
		    : BaseExecutorTask(executor), loader(loader), block_id(block_id), operation(std::move(operation)),
		      relation_name(std::move(relation_name)), chunks(chunks) {
		}

		void ExecuteTask() override {
			loader.WriteUpdateParquetRelation(block_id, operation, relation_name, chunks);
		}

		string TaskType() const override {
			return "LdbcForumUpdateParquetTask";
		}

	private:
		LdbcLoadGenerator &loader;
		idx_t block_id;
		string operation;
		string relation_name;
		vector<unique_ptr<DataChunk>> &chunks;
	};

	void ScheduleUpdateParquetTask(TaskExecutor &executor, idx_t block_id, const string &operation,
	                               const string &relation_name, vector<unique_ptr<DataChunk>> &chunks) {
		if (chunks.empty()) {
			return;
		}
		executor.ScheduleTask(
		    make_uniq<LdbcForumUpdateParquetTask>(executor, *this, block_id, operation, relation_name, chunks));
	}

	void WriteForumUpdateParquetBlockParallel(idx_t block_id, ForumChunkBatch &batch) {
		if (!bind_data.emit_updates) {
			return;
		}
		TaskExecutor executor(context);
		ScheduleUpdateParquetTask(executor, block_id, "inserts", "Forum", batch.insert_forums);
		ScheduleUpdateParquetTask(executor, block_id, "inserts", "Forum_hasTag_Tag", batch.insert_forum_tags);
		ScheduleUpdateParquetTask(executor, block_id, "inserts", "Forum_hasMember_Person", batch.insert_forum_members);
		ScheduleUpdateParquetTask(executor, block_id, "inserts", "Post", batch.insert_posts);
		ScheduleUpdateParquetTask(executor, block_id, "inserts", "Post_hasTag_Tag", batch.insert_post_tags);
		ScheduleUpdateParquetTask(executor, block_id, "inserts", "Comment", batch.insert_comments);
		ScheduleUpdateParquetTask(executor, block_id, "inserts", "Comment_hasTag_Tag", batch.insert_comment_tags);
		ScheduleUpdateParquetTask(executor, block_id, "inserts", "Person_likes_Post", batch.insert_post_likes);
		ScheduleUpdateParquetTask(executor, block_id, "inserts", "Person_likes_Comment", batch.insert_comment_likes);
		ScheduleUpdateParquetTask(executor, block_id, "deletes", "Forum", batch.delete_forums);
		ScheduleUpdateParquetTask(executor, block_id, "deletes", "Forum_hasMember_Person", batch.delete_forum_members);
		ScheduleUpdateParquetTask(executor, block_id, "deletes", "Post", batch.delete_posts);
		ScheduleUpdateParquetTask(executor, block_id, "deletes", "Comment", batch.delete_comments);
		ScheduleUpdateParquetTask(executor, block_id, "deletes", "Person_likes_Post", batch.delete_post_likes);
		ScheduleUpdateParquetTask(executor, block_id, "deletes", "Person_likes_Comment", batch.delete_comment_likes);
		executor.WorkOnTasks();
	}

	void FlushForumUpdateParquet() {
		if (!UseDirectForumUpdateParquet() || forum_update_parquet_flushed) {
			return;
		}
		auto append_start = LdbcPhaseProfileEnabled() ? LdbcProfileSampleNow() : LdbcProfileSample();
		WriteForumUpdateParquetBlockParallel(0, pending_forum_update_batch);
		forum_update_parquet_flushed = true;
		if (LdbcPhaseProfileEnabled()) {
			auto delta = LdbcProfileSampleDelta(LdbcProfileSampleNow(), append_start);
			if (delta.wall_ms > 0) {
				forum_append_chunks_profile.wall_ms += delta.wall_ms;
				forum_append_chunks_profile.user_ms += delta.user_ms;
				forum_append_chunks_profile.system_ms += delta.system_ms;
				forum_append_chunks_profile_calls++;
			}
		}
	}

	static void RegisterUpdateChunks(LdbcForumUpdateChunkRegistryEntry &entry, const string &operation,
	                                 const string &relation_name, vector<unique_ptr<DataChunk>> &chunks) {
		if (chunks.empty()) {
			return;
		}
		entry.chunks[operation + ":" + relation_name] = std::move(chunks);
	}

	void RegisterForumUpdateChunks() {
		if (!UseForumUpdateCopyTableFunction() || forum_update_chunks_registered) {
			return;
		}
		if (bind_data.forum_update_chunk_token.empty()) {
			throw InternalException("Missing LDBC forum update chunk token");
		}
		auto entry = make_shared_ptr<LdbcForumUpdateChunkRegistryEntry>();
		RegisterUpdateChunks(*entry, "inserts", "Forum", pending_forum_update_batch.insert_forums);
		RegisterUpdateChunks(*entry, "inserts", "Forum_hasTag_Tag", pending_forum_update_batch.insert_forum_tags);
		RegisterUpdateChunks(*entry, "inserts", "Forum_hasMember_Person",
		                     pending_forum_update_batch.insert_forum_members);
		RegisterUpdateChunks(*entry, "inserts", "Post", pending_forum_update_batch.insert_posts);
		RegisterUpdateChunks(*entry, "inserts", "Post_hasTag_Tag", pending_forum_update_batch.insert_post_tags);
		RegisterUpdateChunks(*entry, "inserts", "Comment", pending_forum_update_batch.insert_comments);
		RegisterUpdateChunks(*entry, "inserts", "Comment_hasTag_Tag", pending_forum_update_batch.insert_comment_tags);
		RegisterUpdateChunks(*entry, "inserts", "Person_likes_Post", pending_forum_update_batch.insert_post_likes);
		RegisterUpdateChunks(*entry, "inserts", "Person_likes_Comment",
		                     pending_forum_update_batch.insert_comment_likes);
		RegisterUpdateChunks(*entry, "deletes", "Forum", pending_forum_update_batch.delete_forums);
		RegisterUpdateChunks(*entry, "deletes", "Forum_hasMember_Person",
		                     pending_forum_update_batch.delete_forum_members);
		RegisterUpdateChunks(*entry, "deletes", "Post", pending_forum_update_batch.delete_posts);
		RegisterUpdateChunks(*entry, "deletes", "Comment", pending_forum_update_batch.delete_comments);
		RegisterUpdateChunks(*entry, "deletes", "Person_likes_Post", pending_forum_update_batch.delete_post_likes);
		RegisterUpdateChunks(*entry, "deletes", "Person_likes_Comment",
		                     pending_forum_update_batch.delete_comment_likes);
		std::lock_guard<std::mutex> lock(ldbc_forum_update_chunk_registry_lock);
		ldbc_forum_update_chunk_registry[bind_data.forum_update_chunk_token] = std::move(entry);
		forum_update_chunks_registered = true;
	}

	void AppendChunks(LdbcChunkAppender &appender, vector<unique_ptr<DataChunk>> &chunks) {
		for (auto &chunk : chunks) {
			appender.AppendChunk(*chunk);
		}
	}

	void AppendInsertChunks(const string &relation_name, vector<unique_ptr<DataChunk>> &chunks) {
		if (chunks.empty()) {
			return;
		}
		auto entry = insert_chunk_appenders.find(relation_name);
		if (entry == insert_chunk_appenders.end()) {
			throw InternalException("Missing LDBC insert chunk appender for relation %s", relation_name);
		}
		AppendChunks(*entry->second, chunks);
	}

	void AppendDeleteChunks(const string &relation_name, vector<unique_ptr<DataChunk>> &chunks) {
		if (chunks.empty()) {
			return;
		}
		auto entry = delete_chunk_appenders.find(relation_name);
		if (entry == delete_chunk_appenders.end()) {
			throw InternalException("Missing LDBC delete chunk appender for relation %s", relation_name);
		}
		AppendChunks(*entry->second, chunks);
	}

	void AppendForumChunkBatch(ForumChunkBatch &batch) {
		AppendChunks(*forum_appender, batch.forums);
		AppendChunks(*forum_tag_appender, batch.forum_tags);
		AppendChunks(*forum_member_appender, batch.forum_members);
		AppendChunks(*post_chunk_appender, batch.posts);
		AppendChunks(*post_tag_appender, batch.post_tags);
		AppendChunks(*comment_chunk_appender, batch.comments);
		AppendChunks(*comment_tag_appender, batch.comment_tags);
		AppendChunks(*post_like_appender, batch.post_likes);
		AppendChunks(*comment_like_appender, batch.comment_likes);
		if (UseDirectForumUpdateParquet() || UseForumUpdateCopyTableFunction()) {
			CollectForumUpdateChunks(batch);
		} else if (bind_data.emit_updates) {
			AppendInsertChunks("Forum", batch.insert_forums);
			AppendInsertChunks("Forum_hasTag_Tag", batch.insert_forum_tags);
			AppendInsertChunks("Forum_hasMember_Person", batch.insert_forum_members);
			AppendInsertChunks("Post", batch.insert_posts);
			AppendInsertChunks("Post_hasTag_Tag", batch.insert_post_tags);
			AppendInsertChunks("Comment", batch.insert_comments);
			AppendInsertChunks("Comment_hasTag_Tag", batch.insert_comment_tags);
			AppendInsertChunks("Person_likes_Post", batch.insert_post_likes);
			AppendInsertChunks("Person_likes_Comment", batch.insert_comment_likes);
			AppendDeleteChunks("Forum", batch.delete_forums);
			AppendDeleteChunks("Forum_hasMember_Person", batch.delete_forum_members);
			AppendDeleteChunks("Post", batch.delete_posts);
			AppendDeleteChunks("Comment", batch.delete_comments);
			AppendDeleteChunks("Person_likes_Post", batch.delete_post_likes);
			AppendDeleteChunks("Person_likes_Comment", batch.delete_comment_likes);
		}
		person_counts.forums += batch.counts.forums;
		person_counts.forum_tags += batch.counts.forum_tags;
		person_counts.forum_members += batch.counts.forum_members;
		person_counts.posts += batch.counts.posts;
		person_counts.post_tags += batch.counts.post_tags;
		person_counts.comments += batch.counts.comments;
		person_counts.comment_tags += batch.counts.comment_tags;
		person_counts.post_likes += batch.counts.post_likes;
		person_counts.comment_likes += batch.counts.comment_likes;
	}

	void DrainForumChunkBatches() {
		auto append_start = LdbcPhaseProfileEnabled() ? LdbcProfileSampleNow() : LdbcProfileSample();
		while (true) {
			ForumChunkBatch batch;
			{
				std::lock_guard<std::mutex> lock(pending_forum_chunk_batches_lock);
				auto entry = pending_forum_chunk_batches.find(next_forum_chunk_block);
				if (entry == pending_forum_chunk_batches.end()) {
					break;
				}
				batch = std::move(entry->second);
				pending_forum_chunk_batches.erase(entry);
				next_forum_chunk_block++;
			}
			AppendForumChunkBatch(batch);
		}
		if (LdbcPhaseProfileEnabled()) {
			auto delta = LdbcProfileSampleDelta(LdbcProfileSampleNow(), append_start);
			if (delta.wall_ms > 0) {
				forum_append_chunks_profile.wall_ms += delta.wall_ms;
				forum_append_chunks_profile.user_ms += delta.user_ms;
				forum_append_chunks_profile.system_ms += delta.system_ms;
				forum_append_chunks_profile_calls++;
			}
		}
	}

	void EnsureDynamicState() {
		if (config) {
			return;
		}
		auto resource_dir = ResourceDirFromDictionaryDir(bind_data.dictionary_dir);
		config = make_uniq<LdbcDatagenConfig>(LdbcDatagenConfig::Load(bind_data.scale_factor, resource_dir));
		dates = make_uniq<LdbcDateGenerator>(*config);
		bulkload_threshold =
		    dates->SimulationEnd() -
		    static_cast<int64_t>(static_cast<double>(dates->SimulationEnd() - dates->SimulationStart()) *
		                         (1.0 - config->bulkload_portion));
		chunk_message_appenders = std::getenv("LDBCGEN_ROW_MESSAGES") == nullptr;

		person_appender = MakeStaticAppender(context, bind_data, "Person");
		interest_appender = make_uniq<LdbcChunkAppender>(
		    MakeStaticAppender(context, bind_data, "Person_hasInterest_Tag"),
		    vector<LogicalType> {LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::INTEGER});
		study_appender = MakeStaticAppender(context, bind_data, "Person_studyAt_University");
		work_appender = MakeStaticAppender(context, bind_data, "Person_workAt_Company");
		knows_appender = make_uniq<LdbcChunkAppender>(
		    MakeStaticAppender(context, bind_data, "Person_knows_Person"),
		    vector<LogicalType> {LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::BIGINT});
		forum_appender = MakeForumAppender(context, bind_data);
		forum_member_appender = make_uniq<LdbcChunkAppender>(
		    MakeStaticAppender(context, bind_data, "Forum_hasMember_Person"),
		    vector<LogicalType> {LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::BIGINT});
		forum_tag_appender = make_uniq<LdbcChunkAppender>(
		    MakeStaticAppender(context, bind_data, "Forum_hasTag_Tag"),
		    vector<LogicalType> {LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::INTEGER});
		if (chunk_message_appenders) {
			post_chunk_appender = MakePostAppender(context, bind_data);
		} else {
			post_appender = MakeStaticAppender(context, bind_data, "Post");
		}
		post_tag_appender = make_uniq<LdbcChunkAppender>(
		    MakeStaticAppender(context, bind_data, "Post_hasTag_Tag"),
		    vector<LogicalType> {LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::INTEGER});
		if (chunk_message_appenders) {
			comment_chunk_appender = MakeCommentAppender(context, bind_data);
		} else {
			comment_appender = MakeStaticAppender(context, bind_data, "Comment");
		}
		comment_tag_appender = make_uniq<LdbcChunkAppender>(
		    MakeStaticAppender(context, bind_data, "Comment_hasTag_Tag"),
		    vector<LogicalType> {LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::INTEGER});
		post_like_appender = make_uniq<LdbcChunkAppender>(
		    MakeStaticAppender(context, bind_data, "Person_likes_Post"),
		    vector<LogicalType> {LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::BIGINT});
		comment_like_appender = make_uniq<LdbcChunkAppender>(
		    MakeStaticAppender(context, bind_data, "Person_likes_Comment"),
		    vector<LogicalType> {LogicalType::TIMESTAMP_MS, LogicalType::BIGINT, LogicalType::BIGINT});
		if (bind_data.emit_updates) {
			idx_t relation_start = 0;
			for (idx_t row_idx = 1; row_idx <= LdbcSchemaSize(); row_idx++) {
				if (row_idx != LdbcSchemaSize() && string(LDBC_BI_STATIC_SCHEMA[row_idx].relation_name) ==
				                                       LDBC_BI_STATIC_SCHEMA[relation_start].relation_name) {
					continue;
				}
				auto &relation = LDBC_BI_STATIC_SCHEMA[relation_start];
				relation_start = row_idx;
				if (string(relation.kind) == "static_node") {
					continue;
				}
				auto relation_name = string(relation.relation_name);
				if (UsePrematerializedForumChunks() && IsForumGeneratedRelation(relation_name)) {
					insert_chunk_appenders[relation_name] = make_uniq<LdbcChunkAppender>(
					    MakeStaticAppender(context, bind_data, LdbcInsertTableName(bind_data, relation_name)),
					    ForumInsertTypes(relation_name));
				} else {
					insert_appenders[relation_name] =
					    MakeStaticAppender(context, bind_data, LdbcInsertTableName(bind_data, relation_name));
				}
				if (LdbcRelationHasDeletes(relation_name)) {
					if (UsePrematerializedForumChunks() && IsForumGeneratedRelation(relation_name)) {
						delete_chunk_appenders[relation_name] = make_uniq<LdbcChunkAppender>(
						    MakeStaticAppender(context, bind_data, LdbcDeleteTableName(bind_data, relation_name)),
						    ForumDeleteTypes(relation_name));
					} else {
						delete_appenders[relation_name] =
						    MakeStaticAppender(context, bind_data, LdbcDeleteTableName(bind_data, relation_name));
					}
				}
			}
		}
	}

	void LoadStatic() {
		LdbcProfileTimer timer("populate.static.total");
		StaticDictionaryData data;
		{
			LdbcProfileTimer phase_timer("load.static_dictionaries");
			data = LoadStaticDictionaryData(bind_data);
		}
		SetLdbcGenProgress(&progress_state, 4.0);
		{
			LdbcProfileTimer phase_timer("append.static.Place");
			row_counts["Place"] = AppendPlaces(context, bind_data, data);
		}
		SetLdbcGenProgress(&progress_state, 8.0);
		{
			LdbcProfileTimer phase_timer("append.static.TagClass");
			row_counts["TagClass"] = AppendTagClasses(context, bind_data, data);
		}
		SetLdbcGenProgress(&progress_state, 12.0);
		{
			LdbcProfileTimer phase_timer("append.static.Tag");
			row_counts["Tag"] = AppendTags(context, bind_data, data);
		}
		SetLdbcGenProgress(&progress_state, 16.0);
		{
			LdbcProfileTimer phase_timer("append.static.Organisation");
			row_counts["Organisation"] = AppendOrganisations(context, bind_data, data);
		}
		SetLdbcGenProgress(&progress_state, 20.0);
	}

	bool GeneratePersons() {
		EnsureDynamicState();
		if (!persons_initialized) {
			persons.resize(NumericCast<idx_t>(config->num_persons));
			persons_initialized = true;
		}
		LdbcProfileTimer timer("generate.persons");
		auto block_size = NumericCast<idx_t>(config->block_size);
		auto total_blocks = (persons.size() + block_size - 1) / block_size;
		auto worker_count = MinValue<idx_t>(bind_data.threads, MaxValue<idx_t>(total_blocks - person_next_block, 1));
		auto blocks_this_round = MinValue<idx_t>(total_blocks - person_next_block, worker_count);
		if (blocks_this_round == 0) {
			SetLdbcGenProgress(&progress_state, 25.0);
			return true;
		}
		auto block_start = person_next_block;
		auto block_end = block_start + blocks_this_round;

		if (worker_count <= 1) {
			GeneratePersonBlocks(block_start, block_end);
		} else {
			class LdbcPersonBlockTask : public BaseExecutorTask {
			public:
				LdbcPersonBlockTask(TaskExecutor &executor, LdbcLoadGenerator &loader, std::atomic<idx_t> &next_block,
				                    idx_t block_end)
				    : BaseExecutorTask(executor), loader(loader), next_block(next_block), block_end(block_end) {
				}

				void ExecuteTask() override {
					LdbcPersonGenerator generator(*loader.config);
					while (true) {
						auto block_id = next_block.fetch_add(1);
						if (block_id >= block_end) {
							break;
						}
						loader.GeneratePersonBlock(generator, block_id);
					}
				}

				string TaskType() const override {
					return "LdbcPersonBlockTask";
				}

			private:
				LdbcLoadGenerator &loader;
				std::atomic<idx_t> &next_block;
				idx_t block_end;
			};

			std::atomic<idx_t> next_block(block_start);
			TaskExecutor executor(context);
			for (idx_t worker_idx = 0; worker_idx < worker_count; worker_idx++) {
				executor.ScheduleTask(make_uniq<LdbcPersonBlockTask>(executor, *this, next_block, block_end));
			}
			executor.WorkOnTasks();
		}

		person_next_block = block_end;
		SetLdbcGenProgress(&progress_state, LdbcGenProgressRange(20.0, 25.0, person_next_block, total_blocks));
		if (person_next_block >= total_blocks) {
			SetLdbcGenProgress(&progress_state, 25.0);
			return true;
		}
		return false;
	}

	void GeneratePersonBlocks(idx_t block_start, idx_t block_end) {
		LdbcPersonGenerator generator(*config);
		for (idx_t block_id = block_start; block_id < block_end; block_id++) {
			GeneratePersonBlock(generator, block_id);
		}
	}

	void GeneratePersonBlock(LdbcPersonGenerator &generator, idx_t block_id) {
		auto block_size = NumericCast<idx_t>(config->block_size);
		auto start = block_id * block_size;
		auto end = std::min<idx_t>(persons.size(), start + block_size);
		for (idx_t sequential_id = start; sequential_id < end; sequential_id++) {
			persons[sequential_id] = generator.GenerateCore(NumericCast<int64_t>(sequential_id));
		}
	}

	bool IsSnapshotRow(int64_t creation_date) const {
		return creation_date < bulkload_threshold;
	}

	bool IsInsertRow(int64_t creation_date) const {
		return bind_data.emit_updates && creation_date >= bulkload_threshold && creation_date < dates->SimulationEnd();
	}

	bool IsDeleteRow(int64_t deletion_date, bool explicitly_deleted) const {
		return bind_data.emit_updates && explicitly_deleted && deletion_date >= bulkload_threshold &&
		       deletion_date < dates->SimulationEnd();
	}

	Value BatchId(int64_t epoch_ms) const {
		return Value::DATE(LdbcDateFromEpochMs(epoch_ms));
	}

	InternalAppender &InsertAppender(const string &relation_name) {
		auto entry = insert_appenders.find(relation_name);
		if (entry == insert_appenders.end()) {
			throw InternalException("Missing LDBC insert appender for relation %s", relation_name);
		}
		return *entry->second;
	}

	InternalAppender &DeleteAppender(const string &relation_name) {
		auto entry = delete_appenders.find(relation_name);
		if (entry == delete_appenders.end()) {
			throw InternalException("Missing LDBC delete appender for relation %s", relation_name);
		}
		return *entry->second;
	}

	void AppendPersonInsert(const LdbcPersonCore &person) {
		auto &appender = InsertAppender("Person");
		appender.BeginRow();
		appender.Append(BatchId(person.creation_date));
		appender.Append(Value::TIMESTAMP(LdbcTimestampMs(person.creation_date)));
		appender.Append<int64_t>(person.account_id);
		appender.Append(Value(person.first_name));
		appender.Append(Value(person.last_name));
		appender.Append(Value(person.gender == 0 ? string("female") : string("male")));
		appender.Append(Value::DATE(LdbcDateFromEpochMs(person.birthday)));
		appender.Append(Value(person.ip_address));
		appender.Append(Value(person.browser_name));
		appender.Append<int32_t>(person.city_id);
		appender.Append(Value(person.languages));
		appender.Append(Value(person.emails));
		appender.EndRow();
	}

	void AppendTimestampInt64Int32Insert(const string &relation_name, int64_t creation_date, int64_t id1, int32_t id2) {
		auto &appender = InsertAppender(relation_name);
		appender.BeginRow();
		appender.Append(BatchId(creation_date));
		appender.Append(Value::TIMESTAMP(LdbcTimestampMs(creation_date)));
		appender.Append<int64_t>(id1);
		appender.Append<int32_t>(id2);
		appender.EndRow();
	}

	void AppendTimestampInt64Int64Insert(const string &relation_name, int64_t creation_date, int64_t id1, int64_t id2) {
		auto &appender = InsertAppender(relation_name);
		appender.BeginRow();
		appender.Append(BatchId(creation_date));
		appender.Append(Value::TIMESTAMP(LdbcTimestampMs(creation_date)));
		appender.Append<int64_t>(id1);
		appender.Append<int64_t>(id2);
		appender.EndRow();
	}

	void AppendStudyInsert(const LdbcPersonCore &person) {
		auto &appender = InsertAppender("Person_studyAt_University");
		appender.BeginRow();
		appender.Append(BatchId(person.creation_date));
		appender.Append(Value::TIMESTAMP(LdbcTimestampMs(person.creation_date)));
		appender.Append<int64_t>(person.account_id);
		appender.Append<int64_t>(person.university_id);
		appender.Append<int32_t>(Date::ExtractYear(LdbcDateFromEpochMs(person.class_year)));
		appender.EndRow();
	}

	void AppendWorkInsert(const LdbcPersonCore &person, const std::pair<const int64_t, int64_t> &company) {
		auto &appender = InsertAppender("Person_workAt_Company");
		appender.BeginRow();
		appender.Append(BatchId(person.creation_date));
		appender.Append(Value::TIMESTAMP(LdbcTimestampMs(person.creation_date)));
		appender.Append<int64_t>(person.account_id);
		appender.Append<int64_t>(company.first);
		appender.Append<int32_t>(Date::ExtractYear(LdbcDateFromEpochMs(company.second)));
		appender.EndRow();
	}

	void AppendForumInsert(const LdbcForum &forum) {
		auto &appender = InsertAppender("Forum");
		appender.BeginRow();
		appender.Append(BatchId(forum.creation_date));
		appender.Append(Value::TIMESTAMP(LdbcTimestampMs(forum.creation_date)));
		appender.Append<int64_t>(forum.id);
		appender.Append(Value(forum.title));
		appender.Append<int64_t>(forum.moderator_person_id);
		appender.EndRow();
	}

	void AppendPostInsert(const LdbcPost &post) {
		auto &appender = InsertAppender("Post");
		appender.BeginRow();
		appender.Append(BatchId(post.creation_date));
		appender.Append(Value::TIMESTAMP(LdbcTimestampMs(post.creation_date)));
		appender.Append<int64_t>(post.id);
		appender.Append(post.image_file.empty() ? Value(LogicalType::VARCHAR) : Value(post.image_file));
		appender.Append(Value(post.location_ip));
		appender.Append(Value(post.browser_used));
		appender.Append(post.language.empty() ? Value(LogicalType::VARCHAR) : Value(post.language));
		appender.Append(post.image_file.empty() ? Value(post.content) : Value(LogicalType::VARCHAR));
		appender.Append<int32_t>(post.length);
		appender.Append<int64_t>(post.creator_person_id);
		appender.Append<int64_t>(post.forum_id);
		appender.Append<int64_t>(post.location_country_id);
		appender.EndRow();
	}

	void AppendCommentInsert(const LdbcComment &comment) {
		auto &appender = InsertAppender("Comment");
		appender.BeginRow();
		appender.Append(BatchId(comment.creation_date));
		appender.Append(Value::TIMESTAMP(LdbcTimestampMs(comment.creation_date)));
		appender.Append<int64_t>(comment.id);
		appender.Append(Value(comment.location_ip));
		appender.Append(Value(comment.browser_used));
		appender.Append(Value(comment.content));
		appender.Append<int32_t>(comment.length);
		appender.Append<int64_t>(comment.creator_person_id);
		appender.Append<int64_t>(comment.location_country_id);
		appender.Append(comment.parent_post_id == -1 ? Value(LogicalType::BIGINT)
		                                             : Value::BIGINT(comment.parent_post_id));
		appender.Append(comment.parent_comment_id == -1 ? Value(LogicalType::BIGINT)
		                                                : Value::BIGINT(comment.parent_comment_id));
		appender.EndRow();
	}

	void AppendNodeDelete(const string &relation_name, int64_t deletion_date, int64_t id) {
		auto &appender = DeleteAppender(relation_name);
		appender.BeginRow();
		appender.Append(BatchId(deletion_date));
		appender.Append(Value::TIMESTAMP(LdbcTimestampMs(deletion_date)));
		appender.Append<int64_t>(id);
		appender.EndRow();
	}

	void AppendEdgeDelete(const string &relation_name, int64_t deletion_date, int64_t id1, int64_t id2) {
		auto &appender = DeleteAppender(relation_name);
		appender.BeginRow();
		appender.Append(BatchId(deletion_date));
		appender.Append(Value::TIMESTAMP(LdbcTimestampMs(deletion_date)));
		appender.Append<int64_t>(id1);
		appender.Append<int64_t>(id2);
		appender.EndRow();
	}

	bool AppendPersons() {
		EnsurePersonSnapshotBuilders();
		static constexpr idx_t PERSON_BATCH_SIZE = 256;
		LdbcProfileTimer timer("append.person_owned.batch");
		idx_t end = std::min<idx_t>(persons.size(), person_idx + PERSON_BATCH_SIZE);
		for (; person_idx < end; person_idx++) {
			auto &person = persons[person_idx];
			if (IsSnapshotRow(person.creation_date)) {
				if (UseDirectPersonParquet()) {
					AppendPersonRow(*person_builder, person);
				} else {
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
				}
				person_counts.persons++;
			} else if (IsInsertRow(person.creation_date)) {
				AppendPersonInsert(person);
			}
			if (IsDeleteRow(person.deletion_date, person.explicitly_deleted)) {
				AppendNodeDelete("Person", person.deletion_date, person.account_id);
			}

			for (auto tag_id : person.interests) {
				if (IsSnapshotRow(person.creation_date)) {
					if (UseDirectPersonParquet()) {
						AppendTimestampMsInt64Int32Row(*interest_builder, person.creation_date, person.account_id,
						                               tag_id);
					} else {
						AppendTimestampMsInt64Int32Row(*interest_appender, person.creation_date, person.account_id,
						                               tag_id);
					}
					person_counts.interests++;
				} else if (IsInsertRow(person.creation_date)) {
					AppendTimestampInt64Int32Insert("Person_hasInterest_Tag", person.creation_date, person.account_id,
					                                tag_id);
				}
			}
			if (person.university_id != -1 && person.class_year != -1 && IsSnapshotRow(person.creation_date)) {
				if (UseDirectPersonParquet()) {
					AppendStudyRow(*study_builder, person);
				} else {
					study_appender->BeginRow();
					study_appender->Append(Value::TIMESTAMP(LdbcTimestampMs(person.creation_date)));
					study_appender->Append<int64_t>(person.account_id);
					study_appender->Append<int64_t>(person.university_id);
					study_appender->Append<int32_t>(Date::ExtractYear(LdbcDateFromEpochMs(person.class_year)));
					study_appender->EndRow();
				}
				person_counts.study_at++;
			} else if (person.university_id != -1 && person.class_year != -1 && IsInsertRow(person.creation_date)) {
				AppendStudyInsert(person);
			}
			for (auto &company : person.companies) {
				if (IsSnapshotRow(person.creation_date)) {
					if (UseDirectPersonParquet()) {
						AppendWorkRow(*work_builder, person, company);
					} else {
						work_appender->BeginRow();
						work_appender->Append(Value::TIMESTAMP(LdbcTimestampMs(person.creation_date)));
						work_appender->Append<int64_t>(person.account_id);
						work_appender->Append<int64_t>(company.first);
						work_appender->Append<int32_t>(Date::ExtractYear(LdbcDateFromEpochMs(company.second)));
						work_appender->EndRow();
					}
					person_counts.work_at++;
				} else if (IsInsertRow(person.creation_date)) {
					AppendWorkInsert(person, company);
				}
			}
		}
		SetLdbcGenProgress(&progress_state, LdbcGenProgressRange(25.0, 45.0, person_idx, persons.size()));
		return person_idx >= persons.size();
	}

	bool GenerateKnows() {
		if (!knows_generator) {
			knows_generator = make_uniq<LdbcKnowsGenerator>(*config, persons, bind_data.threads, &context);
		}
		LdbcProfileTimer timer("generate.knows");
		auto done = knows_generator->GenerateNext(MaxValue<idx_t>(4, bind_data.threads));
		SetLdbcGenProgress(&progress_state,
		                   LdbcGenProgressRange(45.0, 55.0, static_cast<idx_t>(knows_generator->Progress()), 100));
		if (done) {
			knows_edges = knows_generator->ReleaseEdges();
			knows_generator.reset();
			SetLdbcGenProgress(&progress_state, 55.0);
			return true;
		}
		return false;
	}

	bool AppendKnows() {
		EnsurePersonSnapshotBuilders();
		static constexpr idx_t KNOWS_BATCH_SIZE = 8192;
		LdbcProfileTimer timer("append.knows.batch");
		idx_t end = std::min<idx_t>(knows_edges.size(), knows_idx + KNOWS_BATCH_SIZE);
		for (; knows_idx < end; knows_idx++) {
			auto &edge = knows_edges[knows_idx];
			if (IsSnapshotRow(edge.creation_date)) {
				if (UseDirectPersonParquet()) {
					AppendTimestampMsInt64Int64Row(*knows_builder, edge.creation_date, edge.person1_id,
					                               edge.person2_id);
				} else {
					AppendTimestampMsInt64Int64Row(*knows_appender, edge.creation_date, edge.person1_id,
					                               edge.person2_id);
				}
				person_counts.knows++;
			} else if (IsInsertRow(edge.creation_date)) {
				AppendTimestampInt64Int64Insert("Person_knows_Person", edge.creation_date, edge.person1_id,
				                                edge.person2_id);
			}
			if (IsDeleteRow(edge.deletion_date, edge.explicitly_deleted)) {
				AppendEdgeDelete("Person_knows_Person", edge.deletion_date, edge.person1_id, edge.person2_id);
			}
		}
		SetLdbcGenProgress(&progress_state, LdbcGenProgressRange(55.0, 65.0, knows_idx, knows_edges.size()));
		return knows_idx >= knows_edges.size();
	}

	bool GenerateForums() {
		if (!forum_generator) {
			LdbcProfileTimer timer("generate.forums.init");
			std::function<void(LdbcForum && forum)> append_forum;
			auto use_prematerialized_chunks = UsePrematerializedForumChunks();
			auto use_block_prematerialization = UseForumBlockPrematerialization();
			LdbcForumGenerator::BlockCallback block_callback;
			LdbcForumGenerator::SliceCallback slice_callback;
			if (LdbcPhaseProfileEnabled()) {
				std::cerr << "[ldbcgen] forum.prematerialize_chunks: "
				          << (use_prematerialized_chunks ? "true" : "false")
				          << " block_materialize=" << (use_block_prematerialization ? "true" : "false")
				          << " direct_forum_parquet=" << (UseDirectForumParquet() ? "true" : "false")
				          << " direct_forum_update_parquet=" << (UseDirectForumUpdateParquet() ? "true" : "false")
				          << " forum_update_copy_table_function="
				          << (UseForumUpdateCopyTableFunction() ? "true" : "false")
				          << " emit_updates=" << (bind_data.emit_updates ? "true" : "false")
				          << " chunk_message_appenders=" << (chunk_message_appenders ? "true" : "false") << "\n";
			}
			if (!use_prematerialized_chunks) {
				append_forum = [&](LdbcForum &&forum) {
					AppendForum(forum);
				};
			}
			if (use_block_prematerialization) {
				block_callback = [&](idx_t block_id, idx_t block_start, idx_t block_end, vector<LdbcForum> &forums) {
					MaterializeForumBlock(block_id, block_start, block_end, forums);
				};
				slice_callback = [&](idx_t slice_id, idx_t slice_start, idx_t slice_end, vector<LdbcForum> &forums,
				                     bool finished) {
					MaterializeForumSlice(slice_id, slice_start, slice_end, forums, finished);
				};
			}
			forum_generator = make_uniq<LdbcForumGenerator>(
			    *config, persons, knows_edges, append_forum,
			    [&](idx_t done, idx_t total) {
				    SetLdbcGenProgress(&progress_state, LdbcGenProgressRange(65.0, 98.0, done, total));
			    },
			    bind_data.threads, &context, block_callback, slice_callback);
		}
		LdbcProfileTimer timer("generate.forums.batch");
		auto done = forum_generator->GenerateNext(8);
		if (UseForumBlockPrematerialization()) {
			DrainForumChunkBatches();
		} else if (UsePrematerializedForumChunks()) {
			auto forums = forum_generator->ReleaseForums();
			auto materialize_start = LdbcPhaseProfileEnabled() ? LdbcProfileSampleNow() : LdbcProfileSample();
			auto batches = MaterializeForumChunks(forums);
			if (LdbcPhaseProfileEnabled()) {
				auto delta = LdbcProfileSampleDelta(LdbcProfileSampleNow(), materialize_start);
				forum_materialize_profile.wall_ms += delta.wall_ms;
				forum_materialize_profile.user_ms += delta.user_ms;
				forum_materialize_profile.system_ms += delta.system_ms;
				forum_materialize_profile_calls++;
			}
			auto append_start = LdbcPhaseProfileEnabled() ? LdbcProfileSampleNow() : LdbcProfileSample();
			for (auto &batch : batches) {
				AppendForumChunkBatch(batch);
			}
			if (LdbcPhaseProfileEnabled()) {
				auto delta = LdbcProfileSampleDelta(LdbcProfileSampleNow(), append_start);
				forum_append_chunks_profile.wall_ms += delta.wall_ms;
				forum_append_chunks_profile.user_ms += delta.user_ms;
				forum_append_chunks_profile.system_ms += delta.system_ms;
				forum_append_chunks_profile_calls++;
			}
		}
		if (done) {
			if (UseForumBlockPrematerialization()) {
				DrainForumChunkBatches();
			}
			FlushForumUpdateParquet();
			RegisterForumUpdateChunks();
			SetLdbcGenProgress(&progress_state, 98.0);
		}
		return done;
	}

	void AppendForum(const LdbcForum &forum) {
		LdbcScopedForumAppendProfile profile(*this);
		{
			LdbcScopedForumAppendPartProfile part_profile(*this, ForumAppendPart::FORUM);
			if (IsSnapshotRow(forum.creation_date)) {
				AppendForumRow(*forum_appender, forum);
				person_counts.forums++;
				for (auto tag_id : forum.tags) {
					AppendTimestampMsInt64Int32Row(*forum_tag_appender, forum.creation_date, forum.id, tag_id);
					person_counts.forum_tags++;
				}
			} else if (IsInsertRow(forum.creation_date)) {
				AppendForumInsert(forum);
				for (auto tag_id : forum.tags) {
					AppendTimestampInt64Int32Insert("Forum_hasTag_Tag", forum.creation_date, forum.id, tag_id);
				}
			}
			if (IsDeleteRow(forum.deletion_date, forum.explicitly_deleted)) {
				AppendNodeDelete("Forum", forum.deletion_date, forum.id);
			}
		}
		{
			LdbcScopedForumAppendPartProfile part_profile(*this, ForumAppendPart::MEMBERSHIPS);
			for (auto &membership : forum.memberships) {
				if (IsSnapshotRow(membership.creation_date)) {
					AppendTimestampMsInt64Int64Row(*forum_member_appender, membership.creation_date,
					                               membership.forum_id, membership.person_id);
					person_counts.forum_members++;
				} else if (IsInsertRow(membership.creation_date)) {
					AppendTimestampInt64Int64Insert("Forum_hasMember_Person", membership.creation_date,
					                                membership.forum_id, membership.person_id);
				}
				if (IsDeleteRow(membership.deletion_date, membership.explicitly_deleted)) {
					AppendEdgeDelete("Forum_hasMember_Person", membership.deletion_date, membership.forum_id,
					                 membership.person_id);
				}
			}
		}
		{
			LdbcScopedForumAppendPartProfile part_profile(*this, ForumAppendPart::POSTS);
			for (auto &post : forum.posts) {
				if (IsSnapshotRow(post.creation_date)) {
					if (chunk_message_appenders) {
						AppendPostRow(*post_chunk_appender, post);
					} else {
						post_appender->BeginRow();
						post_appender->Append(Value::TIMESTAMP(LdbcTimestampMs(post.creation_date)));
						post_appender->Append<int64_t>(post.id);
						post_appender->Append(post.image_file.empty() ? Value(LogicalType::VARCHAR)
						                                              : Value(post.image_file));
						post_appender->Append(Value(post.location_ip));
						post_appender->Append(Value(post.browser_used));
						post_appender->Append(post.language.empty() ? Value(LogicalType::VARCHAR)
						                                            : Value(post.language));
						post_appender->Append(post.image_file.empty() ? Value(post.content)
						                                              : Value(LogicalType::VARCHAR));
						post_appender->Append<int32_t>(post.length);
						post_appender->Append<int64_t>(post.creator_person_id);
						post_appender->Append<int64_t>(post.forum_id);
						post_appender->Append<int64_t>(post.location_country_id);
						post_appender->EndRow();
					}
					person_counts.posts++;
					for (auto tag_id : post.tags) {
						AppendTimestampMsInt64Int32Row(*post_tag_appender, post.creation_date, post.id, tag_id);
						person_counts.post_tags++;
					}
				} else if (IsInsertRow(post.creation_date)) {
					AppendPostInsert(post);
					for (auto tag_id : post.tags) {
						AppendTimestampInt64Int32Insert("Post_hasTag_Tag", post.creation_date, post.id, tag_id);
					}
				}
				if (IsDeleteRow(post.deletion_date, post.explicitly_deleted)) {
					AppendNodeDelete("Post", post.deletion_date, post.id);
				}
			}
		}
		{
			LdbcScopedForumAppendPartProfile part_profile(*this, ForumAppendPart::COMMENTS);
			for (auto &comment : forum.comments) {
				if (IsSnapshotRow(comment.creation_date)) {
					if (chunk_message_appenders) {
						AppendCommentRow(*comment_chunk_appender, comment);
					} else {
						comment_appender->BeginRow();
						comment_appender->Append(Value::TIMESTAMP(LdbcTimestampMs(comment.creation_date)));
						comment_appender->Append<int64_t>(comment.id);
						comment_appender->Append(Value(comment.location_ip));
						comment_appender->Append(Value(comment.browser_used));
						comment_appender->Append(Value(comment.content));
						comment_appender->Append<int32_t>(comment.length);
						comment_appender->Append<int64_t>(comment.creator_person_id);
						comment_appender->Append<int64_t>(comment.location_country_id);
						comment_appender->Append(comment.parent_post_id == -1 ? Value(LogicalType::BIGINT)
						                                                      : Value::BIGINT(comment.parent_post_id));
						comment_appender->Append(comment.parent_comment_id == -1
						                             ? Value(LogicalType::BIGINT)
						                             : Value::BIGINT(comment.parent_comment_id));
						comment_appender->EndRow();
					}
					person_counts.comments++;
					for (auto tag_id : comment.tags) {
						AppendTimestampMsInt64Int32Row(*comment_tag_appender, comment.creation_date, comment.id,
						                               tag_id);
						person_counts.comment_tags++;
					}
				} else if (IsInsertRow(comment.creation_date)) {
					AppendCommentInsert(comment);
					for (auto tag_id : comment.tags) {
						AppendTimestampInt64Int32Insert("Comment_hasTag_Tag", comment.creation_date, comment.id,
						                                tag_id);
					}
				}
				if (IsDeleteRow(comment.deletion_date, comment.explicitly_deleted)) {
					AppendNodeDelete("Comment", comment.deletion_date, comment.id);
				}
			}
		}
		{
			LdbcScopedForumAppendPartProfile part_profile(*this, ForumAppendPart::POST_LIKES);
			for (auto &like : forum.post_likes) {
				if (IsSnapshotRow(like.creation_date)) {
					AppendTimestampMsInt64Int64Row(*post_like_appender, like.creation_date, like.person_id,
					                               like.message_id);
					person_counts.post_likes++;
				} else if (IsInsertRow(like.creation_date)) {
					AppendTimestampInt64Int64Insert("Person_likes_Post", like.creation_date, like.person_id,
					                                like.message_id);
				}
				if (IsDeleteRow(like.deletion_date, like.explicitly_deleted)) {
					AppendEdgeDelete("Person_likes_Post", like.deletion_date, like.person_id, like.message_id);
				}
			}
		}
		{
			LdbcScopedForumAppendPartProfile part_profile(*this, ForumAppendPart::COMMENT_LIKES);
			for (auto &like : forum.comment_likes) {
				if (IsSnapshotRow(like.creation_date)) {
					AppendTimestampMsInt64Int64Row(*comment_like_appender, like.creation_date, like.person_id,
					                               like.message_id);
					person_counts.comment_likes++;
				} else if (IsInsertRow(like.creation_date)) {
					AppendTimestampInt64Int64Insert("Person_likes_Comment", like.creation_date, like.person_id,
					                                like.message_id);
				}
				if (IsDeleteRow(like.deletion_date, like.explicitly_deleted)) {
					AppendEdgeDelete("Person_likes_Comment", like.deletion_date, like.person_id, like.message_id);
				}
			}
		}
	}

	void Close() {
		person_appender->Close();
		interest_appender->Close();
		study_appender->Close();
		work_appender->Close();
		knows_appender->Close();
		forum_appender->Close();
		forum_member_appender->Close();
		forum_tag_appender->Close();
		if (chunk_message_appenders) {
			post_chunk_appender->Close();
		} else {
			post_appender->Close();
		}
		post_tag_appender->Close();
		if (chunk_message_appenders) {
			comment_chunk_appender->Close();
		} else {
			comment_appender->Close();
		}
		comment_tag_appender->Close();
		post_like_appender->Close();
		comment_like_appender->Close();
		for (auto &entry : insert_appenders) {
			entry.second->Close();
		}
		for (auto &entry : delete_appenders) {
			entry.second->Close();
		}
		for (auto &entry : insert_chunk_appenders) {
			entry.second->Close();
		}
		for (auto &entry : delete_chunk_appenders) {
			entry.second->Close();
		}

		row_counts["Person"] = person_counts.persons;
		row_counts["Person_hasInterest_Tag"] = person_counts.interests;
		row_counts["Person_knows_Person"] = person_counts.knows;
		row_counts["Person_studyAt_University"] = person_counts.study_at;
		row_counts["Person_workAt_Company"] = person_counts.work_at;
		row_counts["Forum"] = person_counts.forums;
		row_counts["Forum_hasMember_Person"] = person_counts.forum_members;
		row_counts["Forum_hasTag_Tag"] = person_counts.forum_tags;
		row_counts["Post"] = person_counts.posts;
		row_counts["Post_hasTag_Tag"] = person_counts.post_tags;
		row_counts["Comment"] = person_counts.comments;
		row_counts["Comment_hasTag_Tag"] = person_counts.comment_tags;
		row_counts["Person_likes_Post"] = person_counts.post_likes;
		row_counts["Person_likes_Comment"] = person_counts.comment_likes;
	}

	ClientContext &context;
	LdbcGenBindData bind_data;
	LdbcGenGlobalState &progress_state;
	Phase phase = Phase::LOAD_STATIC;
	std::array<LdbcProfileSample, PHASE_COUNT> phase_profiles {};
	std::array<idx_t, PHASE_COUNT> phase_profile_calls {};
	LdbcProfileSample forum_append_profile;
	idx_t forum_append_profile_calls = 0;
	LdbcProfileSample forum_materialize_profile;
	idx_t forum_materialize_profile_calls = 0;
	LdbcProfileSample forum_append_chunks_profile;
	idx_t forum_append_chunks_profile_calls = 0;
	std::array<double, FORUM_APPEND_PART_COUNT> forum_append_part_ms {};
	std::array<idx_t, FORUM_APPEND_PART_COUNT> forum_append_part_calls {};
	unordered_map<string, idx_t> row_counts;
	PersonOwnedRowCounts person_counts;
	unique_ptr<LdbcDatagenConfig> config;
	unique_ptr<LdbcDateGenerator> dates;
	int64_t bulkload_threshold = 0;
	vector<LdbcPersonCore> persons;
	vector<LdbcKnowsEdge> knows_edges;
	bool persons_initialized = false;
	idx_t person_next_block = 0;
	idx_t person_idx = 0;
	idx_t knows_idx = 0;
	unique_ptr<LdbcKnowsGenerator> knows_generator;
	unique_ptr<LdbcForumGenerator> forum_generator;
	std::mutex active_forum_chunk_builders_lock;
	unordered_map<idx_t, unique_ptr<ForumChunkBuilders>> active_forum_chunk_builders;
	std::mutex pending_forum_chunk_batches_lock;
	unordered_map<idx_t, ForumChunkBatch> pending_forum_chunk_batches;
	idx_t next_forum_chunk_block = 0;
	ForumChunkBatch pending_forum_update_batch;
	bool forum_update_parquet_flushed = false;
	bool forum_update_chunks_registered = false;
	unique_ptr<LdbcChunkBuilder> person_builder;
	unique_ptr<LdbcChunkBuilder> interest_builder;
	unique_ptr<LdbcChunkBuilder> study_builder;
	unique_ptr<LdbcChunkBuilder> work_builder;
	unique_ptr<LdbcChunkBuilder> knows_builder;
	bool person_snapshot_builders_initialized = false;
	bool person_rows_snapshot_parquet_flushed = false;
	bool knows_snapshot_parquet_flushed = false;

	unique_ptr<InternalAppender> person_appender;
	unique_ptr<LdbcChunkAppender> interest_appender;
	unique_ptr<InternalAppender> study_appender;
	unique_ptr<InternalAppender> work_appender;
	unique_ptr<LdbcChunkAppender> knows_appender;
	unique_ptr<LdbcChunkAppender> forum_appender;
	unique_ptr<LdbcChunkAppender> forum_member_appender;
	unique_ptr<LdbcChunkAppender> forum_tag_appender;
	bool chunk_message_appenders = false;
	unique_ptr<InternalAppender> post_appender;
	unique_ptr<LdbcChunkAppender> post_chunk_appender;
	unique_ptr<LdbcChunkAppender> post_tag_appender;
	unique_ptr<InternalAppender> comment_appender;
	unique_ptr<LdbcChunkAppender> comment_chunk_appender;
	unique_ptr<LdbcChunkAppender> comment_tag_appender;
	unique_ptr<LdbcChunkAppender> post_like_appender;
	unique_ptr<LdbcChunkAppender> comment_like_appender;
	unordered_map<string, unique_ptr<InternalAppender>> insert_appenders;
	unordered_map<string, unique_ptr<InternalAppender>> delete_appenders;
	unordered_map<string, unique_ptr<LdbcChunkAppender>> insert_chunk_appenders;
	unordered_map<string, unique_ptr<LdbcChunkAppender>> delete_chunk_appenders;
};

static unordered_map<string, idx_t> RunLdbcLoadGenerator(ClientContext &context, const LdbcGenBindData &bind_data,
                                                         LdbcGenGlobalState *progress_state = nullptr) {
	LdbcGenGlobalState local_state;
	auto &state = progress_state ? *progress_state : local_state;
	LdbcLoadGenerator generator(context, bind_data, state);
	while (!generator.GenerateNext()) {
	}
	return generator.ReleaseRowCounts();
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

static string LdbcRelationOutputPath(ClientContext &context, const LdbcGenBindData &bind_data, idx_t relation_index) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto &relation = LdbcRelationAt(relation_index);
	auto extension = bind_data.format == "parquet" ? "parquet" : "csv";
	return fs.JoinPath(bind_data.output_dir, string(relation.entity_path) + "." + extension);
}

static string LdbcSparkBiRoot(ClientContext &context, const LdbcGenBindData &bind_data) {
	auto &fs = FileSystem::GetFileSystem(context);
	return fs.JoinPath(bind_data.output_dir, "graphs/" + bind_data.format + "/bi/composite-merged-fk");
}

static string LdbcSparkRelationDir(ClientContext &context, const LdbcGenBindData &bind_data, const string &operation,
                                   const LdbcSchemaColumn &relation) {
	auto &fs = FileSystem::GetFileSystem(context);
	return fs.JoinPath(LdbcSparkBiRoot(context, bind_data), operation + "/" + string(relation.entity_path));
}

static string LdbcSparkRelationPartPath(ClientContext &context, const LdbcGenBindData &bind_data,
                                        const string &operation, const LdbcSchemaColumn &relation) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto extension = bind_data.format == "parquet" ? "parquet" : "csv";
	return fs.JoinPath(LdbcSparkRelationDir(context, bind_data, operation, relation),
	                   string("part-00000.") + extension);
}

static string LdbcSparkRelationBlockPartPath(ClientContext &context, const LdbcGenBindData &bind_data,
                                             const string &operation, const LdbcSchemaColumn &relation,
                                             idx_t block_id) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto extension = bind_data.format == "parquet" ? "parquet" : "csv";
	std::ostringstream part_name;
	part_name << "part-" << std::setw(5) << std::setfill('0') << block_id << "." << extension;
	return fs.JoinPath(LdbcSparkRelationDir(context, bind_data, operation, relation), part_name.str());
}

static string LdbcSparkRelationPartitionBlockPartPath(ClientContext &context, const LdbcGenBindData &bind_data,
                                                      const string &operation, const LdbcSchemaColumn &relation,
                                                      const string &partition_value, idx_t block_id) {
	auto &fs = FileSystem::GetFileSystem(context);
	auto extension = bind_data.format == "parquet" ? "parquet" : "csv";
	auto partition_dir =
	    fs.JoinPath(LdbcSparkRelationDir(context, bind_data, operation, relation), "batch_id=" + partition_value);
	fs.CreateDirectoriesRecursive(partition_dir);
	std::ostringstream part_name;
	part_name << "part-" << std::setw(5) << std::setfill('0') << block_id << "." << extension;
	return fs.JoinPath(partition_dir, part_name.str());
}

static idx_t LdbcRelationIndexByName(const string &relation_name) {
	for (idx_t relation_index = 0; relation_index < LdbcRelationCount(); relation_index++) {
		if (relation_name == LdbcRelationAt(relation_index).relation_name) {
			return relation_index;
		}
	}
	throw InternalException("Unknown LDBC relation %s", relation_name);
}

static vector<string> LdbcRelationColumnNames(const string &relation_name) {
	vector<string> names;
	for (idx_t column_idx = 0; column_idx < LdbcSchemaSize(); column_idx++) {
		auto &column = LDBC_BI_STATIC_SCHEMA[column_idx];
		if (relation_name == column.relation_name) {
			names.push_back(column.column_name);
		}
	}
	if (names.empty()) {
		throw InternalException("Unknown LDBC relation %s", relation_name);
	}
	return names;
}

static vector<LogicalType> LdbcRelationTypes(const string &relation_name) {
	vector<LogicalType> types;
	for (idx_t column_idx = 0; column_idx < LdbcSchemaSize(); column_idx++) {
		auto &column = LDBC_BI_STATIC_SCHEMA[column_idx];
		if (relation_name == column.relation_name) {
			types.push_back(LdbcLogicalType(column.logical_type));
		}
	}
	if (types.empty()) {
		throw InternalException("Unknown LDBC relation %s", relation_name);
	}
	return types;
}

static vector<LogicalType> LdbcInsertTypes(const string &relation_name) {
	auto types = LdbcRelationTypes(relation_name);
	types.insert(types.begin(), LogicalType::DATE);
	return types;
}

static vector<string> LdbcInsertColumnNames(const string &relation_name) {
	auto names = LdbcRelationColumnNames(relation_name);
	names.insert(names.begin(), "batch_id");
	return names;
}

static vector<LogicalType> LdbcDeleteTypes(const string &relation_name) {
	vector<LogicalType> types {LogicalType::DATE, LogicalType::TIMESTAMP_MS};
	auto relation_index = LdbcRelationIndexByName(relation_name);
	for (auto &key : SplitByDelimiter(LdbcRelationAt(relation_index).primary_key, ",")) {
		auto names = LdbcRelationColumnNames(relation_name);
		auto relation_types = LdbcRelationTypes(relation_name);
		for (idx_t column_idx = 0; column_idx < names.size(); column_idx++) {
			if (names[column_idx] == key) {
				types.push_back(relation_types[column_idx]);
				break;
			}
		}
	}
	return types;
}

static vector<string> LdbcDeleteColumnNames(const string &relation_name) {
	vector<string> names {"batch_id", "deletionDate"};
	auto relation_index = LdbcRelationIndexByName(relation_name);
	for (auto &key : SplitByDelimiter(LdbcRelationAt(relation_index).primary_key, ",")) {
		names.push_back(key);
	}
	return names;
}

static bool LdbcDirectForumParquetRelation(const string &relation_name) {
	return relation_name == "Forum" || relation_name == "Forum_hasTag_Tag" ||
	       relation_name == "Forum_hasMember_Person" || relation_name == "Post" || relation_name == "Post_hasTag_Tag" ||
	       relation_name == "Comment" || relation_name == "Comment_hasTag_Tag" ||
	       relation_name == "Person_likes_Post" || relation_name == "Person_likes_Comment";
}

static bool LdbcDirectPersonParquetRelation(const string &relation_name) {
	return relation_name == "Person" || relation_name == "Person_hasInterest_Tag" ||
	       relation_name == "Person_studyAt_University" || relation_name == "Person_workAt_Company" ||
	       relation_name == "Person_knows_Person";
}

static void WriteLdbcChunksToParquet(ClientContext &context, const string &path, const vector<LogicalType> &types,
                                     const vector<string> &names, vector<unique_ptr<DataChunk>> &chunks) {
	if (chunks.empty()) {
		return;
	}
	ColumnDataCollection collection(context, types);
	ColumnDataAppendState append_state;
	collection.InitializeAppend(append_state);
	for (auto &chunk : chunks) {
		collection.Append(append_state, *chunk);
	}
	WriteLdbcCollectionToParquet(context, path, types, names, collection);
}

static void WriteLdbcCollectionToParquet(ClientContext &context, const string &path, const vector<LogicalType> &types,
                                         const vector<string> &names, ColumnDataCollection &collection) {
	if (collection.Count() == 0) {
		return;
	}
	auto &fs = FileSystem::GetFileSystem(context);
	ParquetWriterOptions options;
	options.file_name = path;
	options.sql_types = types;
	options.column_names = names;
	options.codec = duckdb_parquet::CompressionCodec::SNAPPY;
	options.string_dictionary_page_size_limit = PrimitiveColumnWriter::MAX_UNCOMPRESSED_DICT_PAGE_SIZE;
	options.enable_bloom_filters = true;
	options.bloom_filter_false_positive_ratio = 0.01;
	options.compression_level = ZStdFileSystem::DefaultCompressionLevel();
	options.parquet_version = ParquetVersion::V1;
	options.geoparquet_version = GeoParquetVersion::V1;
	options.write_timestamp_as_int96 = false;
	options.timestamp_is_adjusted_to_utc = TimeStampIsAdjustedToUTC::AUTO;
	options.not_null_columns.assign(types.size(), false);

	vector<pair<string, string>> metadata;
	ParquetWriter writer(context, fs, std::move(options), metadata);
	unique_ptr<ParquetWriteTransformData> transform_data;
	writer.Flush(collection, transform_data);
	writer.Finalize();
}

static void EnsureLdbcOutputDirectories(ClientContext &context, const LdbcGenBindData &bind_data) {
	auto &fs = FileSystem::GetFileSystem(context);
	fs.CreateDirectoriesRecursive(bind_data.output_dir);
	for (idx_t relation_index = 0; relation_index < LdbcRelationCount(); relation_index++) {
		auto entity_path = string(LdbcRelationAt(relation_index).entity_path);
		auto slash = entity_path.find_last_of('/');
		if (slash == string::npos) {
			continue;
		}
		fs.CreateDirectoriesRecursive(fs.JoinPath(bind_data.output_dir, entity_path.substr(0, slash)));
		fs.CreateDirectoriesRecursive(
		    LdbcSparkRelationDir(context, bind_data, "initial_snapshot", LdbcRelationAt(relation_index)));
		if (string(LdbcRelationAt(relation_index).kind) != "static_node") {
			fs.CreateDirectoriesRecursive(
			    LdbcSparkRelationDir(context, bind_data, "inserts", LdbcRelationAt(relation_index)));
			if (LdbcRelationHasDeletes(LdbcRelationAt(relation_index).relation_name)) {
				fs.CreateDirectoriesRecursive(
				    LdbcSparkRelationDir(context, bind_data, "deletes", LdbcRelationAt(relation_index)));
			}
		}
	}
}

static string LdbcQualifiedTableName(const LdbcGenBindData &bind_data, const string &relation_name) {
	return SQLQuotedIdentifier::ToString(bind_data.catalog) + "." + SQLQuotedIdentifier::ToString(bind_data.schema) +
	       "." + SQLQuotedIdentifier::ToString(relation_name);
}

static string LdbcCreateInsertTableSQL(const LdbcGenBindData &bind_data, const LdbcSchemaColumn *begin,
                                       const LdbcSchemaColumn *end) {
	auto table_name = LdbcInsertTableName(bind_data, begin->relation_name);
	string sql = "CREATE TABLE " + LdbcQualifiedTableName(bind_data, table_name) + " (";
	sql += SQLQuotedIdentifier::ToString("batch_id") + " DATE NOT NULL";
	for (auto column = begin; column != end; column++) {
		sql += ", ";
		sql += SQLQuotedIdentifier::ToString(column->column_name);
		sql += " ";
		sql += column->logical_type;
		if (!column->nullable) {
			sql += " NOT NULL";
		}
	}
	sql += ")";
	return sql;
}

static string LdbcCreateDeleteTableSQL(const LdbcGenBindData &bind_data, const LdbcSchemaColumn *begin) {
	auto table_name = LdbcDeleteTableName(bind_data, begin->relation_name);
	string sql = "CREATE TABLE " + LdbcQualifiedTableName(bind_data, table_name) + " (";
	sql += SQLQuotedIdentifier::ToString("batch_id") + " DATE NOT NULL";
	sql += ", " + SQLQuotedIdentifier::ToString("deletionDate") + " TIMESTAMP_MS NOT NULL";
	for (auto &key : SplitByDelimiter(begin->primary_key, ",")) {
		sql += ", ";
		sql += SQLQuotedIdentifier::ToString(key);
		sql += " ";
		bool found = false;
		const auto *column = begin;
		while (column < LDBC_BI_STATIC_SCHEMA + LdbcSchemaSize() &&
		       string(column->relation_name) == begin->relation_name) {
			if (key == column->column_name) {
				sql += column->logical_type;
				found = true;
				break;
			}
			column++;
		}
		if (!found) {
			throw InternalException("Could not resolve primary key column %s for relation %s", key,
			                        begin->relation_name);
		}
		sql += " NOT NULL";
	}
	sql += ")";
	return sql;
}

static void CreateLdbcStagingTablesWithSQL(ClientContext &context, const LdbcGenBindData &bind_data) {
	ExecuteLdbcSQL(context, "CREATE SCHEMA " + SQLQuotedIdentifier::ToString(bind_data.catalog) + "." +
	                            SQLQuotedIdentifier::ToString(bind_data.schema));
	idx_t relation_start = 0;
	for (idx_t row_idx = 1; row_idx <= LdbcSchemaSize(); row_idx++) {
		if (row_idx != LdbcSchemaSize() && string(LDBC_BI_STATIC_SCHEMA[row_idx].relation_name) ==
		                                       LDBC_BI_STATIC_SCHEMA[relation_start].relation_name) {
			continue;
		}
		auto relation_name = string(LDBC_BI_STATIC_SCHEMA[relation_start].relation_name);
		string sql = "CREATE TABLE " + LdbcQualifiedTableName(bind_data, relation_name) + " (";
		for (idx_t column_idx = relation_start; column_idx < row_idx; column_idx++) {
			if (column_idx > relation_start) {
				sql += ", ";
			}
			auto &column = LDBC_BI_STATIC_SCHEMA[column_idx];
			sql += SQLQuotedIdentifier::ToString(column.column_name);
			sql += " ";
			sql += column.logical_type;
			if (!column.nullable) {
				sql += " NOT NULL";
			}
		}
		sql += ")";
		ExecuteLdbcSQL(context, sql);
		if (bind_data.emit_updates && string(LDBC_BI_STATIC_SCHEMA[relation_start].kind) != "static_node") {
			ExecuteLdbcSQL(context, LdbcCreateInsertTableSQL(bind_data, LDBC_BI_STATIC_SCHEMA + relation_start,
			                                                 LDBC_BI_STATIC_SCHEMA + row_idx));
			auto relation_name = string(LDBC_BI_STATIC_SCHEMA[relation_start].relation_name);
			if (LdbcRelationHasDeletes(relation_name)) {
				ExecuteLdbcSQL(context, LdbcCreateDeleteTableSQL(bind_data, LDBC_BI_STATIC_SCHEMA + relation_start));
			}
		}
		relation_start = row_idx;
	}
}

static void CreateLdbcTables(ClientContext &context, const LdbcGenBindData &bind_data) {
	CreateSchemaIfNeeded(context, bind_data.catalog, bind_data.schema);

	idx_t relation_start = 0;
	for (idx_t row_idx = 1; row_idx <= LdbcSchemaSize(); row_idx++) {
		if (row_idx == LdbcSchemaSize() || string(LDBC_BI_STATIC_SCHEMA[row_idx].relation_name) !=
		                                       LDBC_BI_STATIC_SCHEMA[relation_start].relation_name) {
			CreateLdbcTable(context, LDBC_BI_STATIC_SCHEMA + relation_start, LDBC_BI_STATIC_SCHEMA + row_idx,
			                bind_data.catalog, bind_data.schema, bind_data.overwrite, bind_data.primary_keys);
			if (bind_data.emit_updates && string(LDBC_BI_STATIC_SCHEMA[relation_start].kind) != "static_node") {
				auto relation_name = string(LDBC_BI_STATIC_SCHEMA[relation_start].relation_name);
				CreateLdbcInsertTable(context, bind_data, LDBC_BI_STATIC_SCHEMA + relation_start,
				                      LDBC_BI_STATIC_SCHEMA + row_idx);
				if (LdbcRelationHasDeletes(relation_name)) {
					CreateLdbcDeleteTable(context, bind_data, LDBC_BI_STATIC_SCHEMA + relation_start);
				}
			}
			relation_start = row_idx;
		}
	}
}

static unordered_map<string, string> CopyLdbcTablesToFiles(ClientContext &context, const LdbcGenBindData &bind_data,
                                                           LdbcGenGlobalState *progress_state = nullptr) {
	EnsureLdbcOutputDirectories(context, bind_data);
	unordered_map<string, string> output_paths;
	string copy_options;
	if (bind_data.format == "csv") {
		copy_options = "FORMAT CSV, HEADER TRUE";
	} else if (bind_data.format == "parquet") {
		copy_options = "FORMAT PARQUET";
	} else {
		throw InternalException("Unexpected LDBC file format: %s", bind_data.format);
	}
	auto overwrite_options = copy_options;
	if (bind_data.overwrite) {
		overwrite_options += ", OVERWRITE TRUE";
	}
	for (idx_t relation_index = 0; relation_index < LdbcRelationCount(); relation_index++) {
		auto &relation = LdbcRelationAt(relation_index);
		auto relation_name = string(relation.relation_name);
		auto direct_snapshot_relation =
		    (bind_data.direct_forum_parquet && LdbcDirectForumParquetRelation(relation_name)) ||
		    (bind_data.direct_person_parquet && LdbcDirectPersonParquetRelation(relation_name));
		auto direct_forum_updates =
		    bind_data.direct_forum_update_parquet && LdbcDirectForumParquetRelation(relation_name);
		auto copy_forum_update_chunks =
		    bind_data.forum_update_copy_table_function && LdbcDirectForumParquetRelation(relation_name);
		auto output_path = direct_snapshot_relation
		                       ? LdbcSparkRelationDir(context, bind_data, "initial_snapshot", relation)
		                       : LdbcRelationOutputPath(context, bind_data, relation_index);
		string sql;
		if (!direct_snapshot_relation) {
			sql = "COPY " + LdbcQualifiedTableName(bind_data, relation_name) + " TO " +
			      SQLString::ToString(output_path) + " (" + overwrite_options + ")";
			ExecuteLdbcSQL(context, sql);
			auto spark_snapshot_path = LdbcSparkRelationPartPath(context, bind_data, "initial_snapshot", relation);
			sql = "COPY " + LdbcQualifiedTableName(bind_data, relation_name) + " TO " +
			      SQLString::ToString(spark_snapshot_path) + " (" + overwrite_options + ")";
			ExecuteLdbcSQL(context, sql);
		}
		if (bind_data.emit_updates && string(relation.kind) != "static_node" && !direct_forum_updates) {
			auto insert_path = LdbcSparkRelationDir(context, bind_data, "inserts", relation);
			string insert_options = copy_options + ", PARTITION_BY (batch_id)";
			if (bind_data.overwrite) {
				insert_options += ", OVERWRITE TRUE";
			}
			if (copy_forum_update_chunks) {
				if (HasLdbcForumUpdateChunks(bind_data.forum_update_chunk_token, "inserts", relation_name)) {
					sql = "COPY (SELECT * FROM ldbcgen_forum_update_chunks(token := " +
					      SQLString::ToString(bind_data.forum_update_chunk_token) +
					      ", operation := 'inserts', relation := " + SQLString::ToString(relation_name) + ")) TO " +
					      SQLString::ToString(insert_path) + " (" + insert_options + ")";
					ExecuteLdbcProfiledForumUpdateCopy(context, sql, "inserts", relation_name);
				}
			} else {
				sql = "COPY " + LdbcQualifiedTableName(bind_data, LdbcInsertTableName(bind_data, relation_name)) +
				      " TO " + SQLString::ToString(insert_path) + " (" + insert_options + ")";
				ExecuteLdbcSQL(context, sql);
			}
			if (LdbcRelationHasDeletes(relation_name)) {
				auto delete_path = LdbcSparkRelationDir(context, bind_data, "deletes", relation);
				string delete_options = copy_options + ", PARTITION_BY (batch_id)";
				if (bind_data.overwrite) {
					delete_options += ", OVERWRITE TRUE";
				}
				if (copy_forum_update_chunks) {
					if (HasLdbcForumUpdateChunks(bind_data.forum_update_chunk_token, "deletes", relation_name)) {
						sql = "COPY (SELECT * FROM ldbcgen_forum_update_chunks(token := " +
						      SQLString::ToString(bind_data.forum_update_chunk_token) +
						      ", operation := 'deletes', relation := " + SQLString::ToString(relation_name) + ")) TO " +
						      SQLString::ToString(delete_path) + " (" + delete_options + ")";
						ExecuteLdbcProfiledForumUpdateCopy(context, sql, "deletes", relation_name);
					}
				} else {
					sql = "COPY " + LdbcQualifiedTableName(bind_data, LdbcDeleteTableName(bind_data, relation_name)) +
					      " TO " + SQLString::ToString(delete_path) + " (" + delete_options + ")";
					ExecuteLdbcSQL(context, sql);
				}
			}
		}
		output_paths[relation_name] = output_path;
		SetLdbcGenProgress(progress_state, LdbcGenProgressRange(98.0, 100.0, relation_index + 1, LdbcRelationCount()));
	}
	if (!bind_data.forum_update_chunk_token.empty()) {
		std::lock_guard<std::mutex> lock(ldbc_forum_update_chunk_registry_lock);
		ldbc_forum_update_chunk_registry.erase(bind_data.forum_update_chunk_token);
	}
	return output_paths;
}

static unordered_map<string, idx_t> MaterializeLdbcTables(ClientContext &context, const LdbcGenBindData &bind_data) {
	CreateLdbcTables(context, bind_data);
	return RunLdbcLoadGenerator(context, bind_data);
}

namespace ldbc {

void LDBCGenWrapper::CreateLDBCSchema(ClientContext &context, string catalog, string schema, bool overwrite,
                                      bool primary_keys) {
	LdbcProfileTimer timer("create_schema");
	LdbcGenBindData bind_data;
	bind_data.catalog = std::move(catalog);
	bind_data.schema = std::move(schema);
	bind_data.overwrite = overwrite;
	bind_data.primary_keys = primary_keys;
	CreateLdbcTables(context, bind_data);
}

unordered_map<string, idx_t> LDBCGenWrapper::LoadLDBCData(ClientContext &context, string catalog, string schema,
                                                          string dictionary_dir, double scale_factor) {
	LdbcGenBindData bind_data;
	bind_data.catalog = std::move(catalog);
	bind_data.schema = std::move(schema);
	bind_data.dictionary_dir = std::move(dictionary_dir);
	bind_data.scale_factor = scale_factor;
	return RunLdbcLoadGenerator(context, bind_data);
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
	result->catalog = DatabaseManager::GetDefaultDatabase(context).GetIdentifierName();
	result->schema = ClientData::Get(context).catalog_search_path->GetDefault().GetSchema().GetIdentifierName();
	result->threads = MaxValue<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads(), 1);
	result->scale_factor = GetDoubleParameter(input, "sf", 1.0);
	result->catalog = GetStringParameter(input, "catalog", result->catalog);
	result->output_dir = GetStringParameter(input, "output_dir", "");
	result->target = StringUtil::Lower(GetStringParameter(input, "target", "tables"));
	result->schema = GetStringParameter(input, "schema", result->schema);
	result->format = StringUtil::Lower(GetStringParameter(input, "format", "parquet"));
	result->dictionary_dir = GetStringParameter(input, "dictionary_dir", result->dictionary_dir);
	auto threads = GetBigIntParameter(input, "threads", NumericCast<int64_t>(result->threads));
	result->overwrite = GetBooleanParameter(input, "overwrite", false);
	result->primary_keys = GetBooleanParameter(input, "primary_keys", false);

	if (result->scale_factor <= 0) {
		throw BinderException("ldbcgen parameter sf must be greater than zero");
	}
	if (threads <= 0) {
		throw BinderException("ldbcgen parameter threads must be greater than zero");
	}
	result->threads = UnsafeNumericCast<idx_t>(threads);
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
	if (result->target == "tables") {
		result->emit_updates = true;
		result->visible_updates = true;
	}
	if (result->target == "files" && result->format == "parquet" &&
	    std::getenv("LDBCGEN_DIRECT_FORUM_PARQUET") != nullptr) {
		result->direct_forum_parquet = true;
		if (std::getenv("LDBCGEN_FORUM_UPDATE_COPY_TABLE_FUNCTION") != nullptr) {
			result->forum_update_copy_table_function = true;
		} else if (std::getenv("LDBCGEN_DIRECT_FORUM_UPDATE_PARQUET") != nullptr) {
			result->direct_forum_update_parquet = true;
		}
	}
	if (result->target == "files" && result->format == "parquet" &&
	    std::getenv("LDBCGEN_DIRECT_PERSON_PARQUET") != nullptr) {
		result->direct_person_parquet = true;
	}
	if (input.binder && result->target == "tables") {
		auto &catalog = Catalog::GetCatalog(context, Identifier(result->catalog));
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

static double LdbcGenProgress(ClientContext &context, const FunctionData *bind_data,
                              const GlobalTableFunctionState *global_state) {
	if (!global_state) {
		return 0.0;
	}
	auto &state = global_state->Cast<LdbcGenGlobalState>();
	return state.progress.load();
}

static void LdbcGenFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<LdbcGenBindData>();
	auto &state = data_p.global_state->Cast<LdbcGenGlobalState>();

	if (state.finished.load()) {
		data_p.async_result = AsyncResultType::FINISHED;
		return;
	}

	if (bind_data.target == "tables" && !state.schema_created) {
		LdbcProfileTimer timer("ldbcgen.materialize");
		SetLdbcGenProgress(&state, 1.0);
		CreateLdbcTables(context, bind_data);
		SetLdbcGenProgress(&state, 2.0);
		state.schema_created = true;
		if (data_p.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR) {
			data_p.async_result = LdbcGenYield();
			return;
		}
	}

	if (bind_data.target == "tables" && !state.materialized.load()) {
		if (!state.load_started.exchange(true)) {
			state.load_generator = make_uniq<LdbcLoadGenerator>(context, bind_data, state);
		}
		if (!state.load_generator) {
			throw InternalException("LDBC load generator was not initialized");
		}
		idx_t iterations =
		    data_p.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR ? 1 : DConstants::INVALID_INDEX;
		for (idx_t iteration = 0; iteration < iterations; iteration++) {
			if (state.load_generator->GenerateNext()) {
				state.row_counts = state.load_generator->ReleaseRowCounts();
				state.load_generator.reset();
				SetLdbcGenProgress(&state, 98.0);
				state.materialized.store(true);
				break;
			}
		}
		if (!state.materialized.load()) {
			if (data_p.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR) {
				data_p.async_result = LdbcGenYield();
				return;
			}
		}
	}

	if (bind_data.target == "files" && !state.materialized.load()) {
		if (!state.load_started.exchange(true)) {
			state.file_database = make_uniq<DuckDB>(nullptr);
			state.file_connection = make_uniq<Connection>(*state.file_database);
			state.file_bind_data = make_uniq<LdbcGenBindData>(bind_data);
			state.file_bind_data->target = "tables";
			state.file_bind_data->catalog =
			    DatabaseManager::GetDefaultDatabase(*state.file_connection->context).GetIdentifierName();
			state.file_bind_data->schema = "__ldbcgen_files_" + std::to_string(reinterpret_cast<uintptr_t>(&state));
			state.file_bind_data->overwrite = true;
			state.file_bind_data->primary_keys = false;
			state.file_bind_data->emit_updates = true;
			if (state.file_bind_data->forum_update_copy_table_function) {
				state.file_bind_data->forum_update_chunk_token =
				    "__ldbcgen_forum_updates_" + std::to_string(reinterpret_cast<uintptr_t>(&state));
			}
			auto &file_context = *state.file_connection->context;
			if (state.file_bind_data->direct_forum_parquet || state.file_bind_data->direct_person_parquet) {
				EnsureLdbcOutputDirectories(file_context, *state.file_bind_data);
			}
			CreateLdbcStagingTablesWithSQL(file_context, *state.file_bind_data);
			ExecuteLdbcSQL(file_context, "BEGIN TRANSACTION");
			MarkLdbcTransactionReadWrite(file_context, state.file_bind_data->catalog);
			state.load_generator = make_uniq<LdbcLoadGenerator>(file_context, *state.file_bind_data, state);
		}
		if (!state.load_generator || !state.file_bind_data || !state.file_connection) {
			throw InternalException("LDBC file load generator was not initialized");
		}
		auto &file_context = *state.file_connection->context;
		idx_t iterations =
		    data_p.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR ? 1 : DConstants::INVALID_INDEX;
		for (idx_t iteration = 0; iteration < iterations; iteration++) {
			if (state.load_generator->GenerateNext()) {
				state.row_counts = state.load_generator->ReleaseRowCounts();
				state.load_generator.reset();
				ExecuteLdbcSQL(file_context, "COMMIT");
				state.output_paths = CopyLdbcTablesToFiles(file_context, *state.file_bind_data, &state);
				ExecuteLdbcSQL(file_context,
				               "DROP SCHEMA " + SQLQuotedIdentifier::ToString(state.file_bind_data->catalog) + "." +
				                   SQLQuotedIdentifier::ToString(state.file_bind_data->schema) + " CASCADE");
				state.file_bind_data.reset();
				state.file_connection.reset();
				state.file_database.reset();
				state.materialized.store(true);
				break;
			}
		}
		if (!state.materialized.load()) {
			if (data_p.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR) {
				data_p.async_result = LdbcGenYield();
				return;
			}
		}
	}

	if (state.offset >= ldbc::LDBCGenWrapper::RelationCount()) {
		SetLdbcGenProgress(&state, 100.0);
		state.finished.store(true);
		data_p.async_result = AsyncResultType::FINISHED;
		return;
	}

	idx_t count = 0;
	if (bind_data.target == "files") {
		while (state.offset < ldbc::LDBCGenWrapper::RelationCount() && count < STANDARD_VECTOR_SIZE) {
			auto relation_name = ldbc::LDBCGenWrapper::RelationName(state.offset);
			output.data[0].SetValue(count, relation_name);
			auto output_path = state.output_paths.find(relation_name);
			output.data[1].SetValue(count, output_path == state.output_paths.end() ? Value(LogicalType::VARCHAR)
			                                                                       : Value(output_path->second));
			auto row_count = state.row_counts.find(relation_name);
			output.data[2].SetValue(count, Value::BIGINT(row_count == state.row_counts.end() ? 0 : row_count->second));
			output.data[3].SetValue(count, Value(LogicalType::VARCHAR));
			output.data[4].SetValue(count, bind_data.format);
			output.data[5].SetValue(count, bind_data.overwrite ? "recreated" : "created");
			count++;
			state.offset++;
		}
		SetLdbcGenProgress(&state,
		                   LdbcGenProgressRange(98.0, 100.0, state.offset, ldbc::LDBCGenWrapper::RelationCount()));
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
		SetLdbcGenProgress(&state,
		                   LdbcGenProgressRange(98.0, 100.0, state.offset, ldbc::LDBCGenWrapper::RelationCount()));
	}
	output.SetCardinality(count);
}

static unique_ptr<FunctionData> LdbcForumUpdateChunkBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	if (!input.inputs.empty()) {
		throw BinderException("ldbcgen_forum_update_chunks only accepts named parameters");
	}
	auto result = make_uniq<LdbcForumUpdateChunkBindData>();
	result->token = GetStringParameter(input, "token", "");
	result->operation = StringUtil::Lower(GetStringParameter(input, "operation", ""));
	result->relation_name = GetStringParameter(input, "relation", "");
	if (result->token.empty()) {
		throw BinderException("ldbcgen_forum_update_chunks parameter token must be set");
	}
	if (result->operation != "inserts" && result->operation != "deletes") {
		throw BinderException("ldbcgen_forum_update_chunks parameter operation must be 'inserts' or 'deletes'");
	}
	if (!LdbcDirectForumParquetRelation(result->relation_name)) {
		throw BinderException("ldbcgen_forum_update_chunks relation must be a forum-generated relation");
	}
	if (result->operation == "deletes" && !LdbcRelationHasDeletes(result->relation_name)) {
		throw BinderException("ldbcgen_forum_update_chunks relation does not have deletes");
	}
	if (result->operation == "inserts") {
		names = LdbcInsertColumnNames(result->relation_name);
		return_types = LdbcInsertTypes(result->relation_name);
	} else {
		names = LdbcDeleteColumnNames(result->relation_name);
		return_types = LdbcDeleteTypes(result->relation_name);
	}
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> LdbcForumUpdateChunkInit(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<LdbcForumUpdateChunkBindData>();
	auto result = make_uniq<LdbcForumUpdateChunkGlobalState>();
	auto chunk_key = bind_data.operation + ":" + bind_data.relation_name;
	std::lock_guard<std::mutex> lock(ldbc_forum_update_chunk_registry_lock);
	auto token_entry = ldbc_forum_update_chunk_registry.find(bind_data.token);
	if (token_entry == ldbc_forum_update_chunk_registry.end()) {
		throw InvalidInputException("Unknown LDBC forum update chunk token: %s", bind_data.token);
	}
	auto chunk_entry = token_entry->second->chunks.find(chunk_key);
	if (chunk_entry != token_entry->second->chunks.end()) {
		result->chunks = std::move(chunk_entry->second);
		token_entry->second->chunks.erase(chunk_entry);
	}
	return result;
}

static void LdbcForumUpdateChunkFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<LdbcForumUpdateChunkGlobalState>();
	auto offset = state.offset.fetch_add(1);
	if (offset >= state.chunks.size()) {
		return;
	}
	output.Reference(*state.chunks[offset]);
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
	names.emplace_back("operation");
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

struct LdbcSchemaMetadataRow {
	string relation_name;
	string entity_path;
	string kind;
	string operation;
	string path;
	idx_t column_index;
	string column_name;
	string logical_type;
	bool nullable;
	string primary_key;
};

static void AddLdbcSchemaMetadataRow(vector<LdbcSchemaMetadataRow> &rows, const LdbcSchemaColumn &relation,
                                     const string &operation, const string &path, idx_t column_index,
                                     const string &column_name, const string &logical_type, bool nullable) {
	rows.push_back({relation.relation_name, relation.entity_path, relation.kind, operation, path, column_index,
	                column_name, logical_type, nullable, relation.primary_key});
}

static vector<LdbcSchemaMetadataRow> BuildLdbcSchemaMetadataRows(const string &format) {
	vector<LdbcSchemaMetadataRow> rows;
	for (idx_t relation_index = 0; relation_index < LdbcRelationCount(); relation_index++) {
		auto &relation = LdbcRelationAt(relation_index);
		auto relation_name = string(relation.relation_name);
		auto entity_path = string(relation.entity_path);
		auto snapshot_path = "graphs/" + format + "/bi/composite-merged-fk/initial_snapshot/" + entity_path;
		idx_t row_idx = 0;
		while (row_idx < LdbcSchemaSize() && string(LDBC_BI_STATIC_SCHEMA[row_idx].relation_name) != relation_name) {
			row_idx++;
		}
		idx_t column_index = 0;
		while (row_idx < LdbcSchemaSize() && string(LDBC_BI_STATIC_SCHEMA[row_idx].relation_name) == relation_name) {
			auto &column = LDBC_BI_STATIC_SCHEMA[row_idx];
			AddLdbcSchemaMetadataRow(rows, relation, "initial_snapshot", snapshot_path, column_index++,
			                         column.column_name, column.logical_type, column.nullable);
			row_idx++;
		}
		if (string(relation.kind) == "static_node") {
			continue;
		}

		auto insert_path = "graphs/" + format + "/bi/composite-merged-fk/inserts/" + entity_path;
		AddLdbcSchemaMetadataRow(rows, relation, "inserts", insert_path, 0, "batch_id", "DATE", false);
		row_idx = 0;
		while (row_idx < LdbcSchemaSize() && string(LDBC_BI_STATIC_SCHEMA[row_idx].relation_name) != relation_name) {
			row_idx++;
		}
		column_index = 1;
		while (row_idx < LdbcSchemaSize() && string(LDBC_BI_STATIC_SCHEMA[row_idx].relation_name) == relation_name) {
			auto &column = LDBC_BI_STATIC_SCHEMA[row_idx];
			AddLdbcSchemaMetadataRow(rows, relation, "inserts", insert_path, column_index++, column.column_name,
			                         column.logical_type, column.nullable);
			row_idx++;
		}

		if (!LdbcRelationHasDeletes(relation_name)) {
			continue;
		}
		auto delete_path = "graphs/" + format + "/bi/composite-merged-fk/deletes/" + entity_path;
		AddLdbcSchemaMetadataRow(rows, relation, "deletes", delete_path, 0, "batch_id", "DATE", false);
		AddLdbcSchemaMetadataRow(rows, relation, "deletes", delete_path, 1, "deletionDate", "TIMESTAMP_MS", false);
		column_index = 2;
		for (auto &key : SplitByDelimiter(relation.primary_key, ",")) {
			row_idx = 0;
			while (row_idx < LdbcSchemaSize() &&
			       string(LDBC_BI_STATIC_SCHEMA[row_idx].relation_name) != relation_name) {
				row_idx++;
			}
			bool found = false;
			while (row_idx < LdbcSchemaSize() &&
			       string(LDBC_BI_STATIC_SCHEMA[row_idx].relation_name) == relation_name) {
				auto &column = LDBC_BI_STATIC_SCHEMA[row_idx];
				if (key == column.column_name) {
					AddLdbcSchemaMetadataRow(rows, relation, "deletes", delete_path, column_index++, column.column_name,
					                         column.logical_type, false);
					found = true;
					break;
				}
				row_idx++;
			}
			if (!found) {
				throw InternalException("Could not resolve primary key column %s for relation %s", key, relation_name);
			}
		}
	}
	return rows;
}

static unique_ptr<GlobalTableFunctionState> LdbcGenSchemaInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<LdbcGenSchemaGlobalState>();
}

static void LdbcGenSchemaFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<LdbcGenSchemaBindData>();
	auto &state = data_p.global_state->Cast<LdbcGenSchemaGlobalState>();
	idx_t count = 0;
	auto rows = BuildLdbcSchemaMetadataRows(bind_data.format);
	for (idx_t row_idx = state.offset; row_idx < rows.size() && count < STANDARD_VECTOR_SIZE; row_idx++) {
		auto &row = rows[row_idx];
		output.data[0].SetValue(count, row.relation_name);
		output.data[1].SetValue(count, row.entity_path);
		output.data[2].SetValue(count, row.kind);
		output.data[3].SetValue(count, row.operation);
		output.data[4].SetValue(count, row.path);
		output.data[5].SetValue(count, Value::INTEGER(NumericCast<int32_t>(row.column_index)));
		output.data[6].SetValue(count, row.column_name);
		output.data[7].SetValue(count, row.logical_type);
		output.data[8].SetValue(count, Value::BOOLEAN(row.nullable));
		output.data[9].SetValue(count, row.primary_key);
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
	AddConfigEntry(entries, "missing_ratio", DoubleToConfigString(config.missing_ratio), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "prob_another_browser", DoubleToConfigString(config.prob_another_browser), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "prob_uncorrelated_company", DoubleToConfigString(config.prob_uncorrelated_company),
	               "DOUBLE", "params_default.ini");
	AddConfigEntry(entries, "prob_uncorrelated_organisation",
	               DoubleToConfigString(config.prob_uncorrelated_organisation), "DOUBLE", "params_default.ini");
	AddConfigEntry(entries, "prob_top_univ", DoubleToConfigString(config.prob_top_univ), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "tag_country_corr_prob", DoubleToConfigString(config.tag_country_corr_prob), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "max_num_post_per_month", std::to_string(config.max_num_post_per_month), "INTEGER",
	               "params_default.ini");
	AddConfigEntry(entries, "max_num_comments", std::to_string(config.max_num_comments), "INTEGER",
	               "params_default.ini");
	AddConfigEntry(entries, "max_num_flashmob_post_per_month", std::to_string(config.max_num_flashmob_post_per_month),
	               "INTEGER", "params_default.ini");
	AddConfigEntry(entries, "max_num_group_created_per_person", std::to_string(config.max_num_group_created_per_person),
	               "INTEGER", "params_default.ini");
	AddConfigEntry(entries, "max_num_group_flashmob_post_per_month",
	               std::to_string(config.max_num_group_flashmob_post_per_month), "INTEGER", "params_default.ini");
	AddConfigEntry(entries, "max_num_group_post_per_month", std::to_string(config.max_num_group_post_per_month),
	               "INTEGER", "params_default.ini");
	AddConfigEntry(entries, "max_num_like", std::to_string(config.max_num_like), "INTEGER", "params_default.ini");
	AddConfigEntry(entries, "max_group_size", std::to_string(config.max_group_size), "INTEGER", "params_default.ini");
	AddConfigEntry(entries, "max_num_photo_albums_per_month", std::to_string(config.max_num_photo_albums_per_month),
	               "INTEGER", "params_default.ini");
	AddConfigEntry(entries, "max_num_photo_per_albums", std::to_string(config.max_num_photo_per_albums), "INTEGER",
	               "params_default.ini");
	AddConfigEntry(entries, "max_num_popular_places", std::to_string(config.max_num_popular_places), "INTEGER",
	               "params_default.ini");
	AddConfigEntry(entries, "max_num_tag_per_flashmob_post", std::to_string(config.max_num_tag_per_flashmob_post),
	               "INTEGER", "params_default.ini");
	AddConfigEntry(entries, "flashmob_tags_per_month", std::to_string(config.flashmob_tags_per_month), "INTEGER",
	               "params_default.ini");
	AddConfigEntry(entries, "min_text_size", std::to_string(config.min_text_size), "INTEGER", "params_default.ini");
	AddConfigEntry(entries, "max_text_size", std::to_string(config.max_text_size), "INTEGER", "params_default.ini");
	AddConfigEntry(entries, "min_comment_size", std::to_string(config.min_comment_size), "INTEGER",
	               "params_default.ini");
	AddConfigEntry(entries, "max_comment_size", std::to_string(config.max_comment_size), "INTEGER",
	               "params_default.ini");
	AddConfigEntry(entries, "min_large_post_size", std::to_string(config.min_large_post_size), "INTEGER",
	               "params_default.ini");
	AddConfigEntry(entries, "max_large_post_size", std::to_string(config.max_large_post_size), "INTEGER",
	               "params_default.ini");
	AddConfigEntry(entries, "min_large_comment_size", std::to_string(config.min_large_comment_size), "INTEGER",
	               "params_default.ini");
	AddConfigEntry(entries, "max_large_comment_size", std::to_string(config.max_large_comment_size), "INTEGER",
	               "params_default.ini");
	AddConfigEntry(entries, "group_moderator_prob", DoubleToConfigString(config.group_moderator_prob), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "prob_another_browser", DoubleToConfigString(config.prob_another_browser), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "prob_diff_ip_travel_season", DoubleToConfigString(config.prob_diff_ip_travel_season),
	               "DOUBLE", "params_default.ini");
	AddConfigEntry(entries, "prob_diff_ip_not_travel_season",
	               DoubleToConfigString(config.prob_diff_ip_not_travel_season), "DOUBLE", "params_default.ini");
	AddConfigEntry(entries, "ratio_reduce_text", DoubleToConfigString(config.ratio_reduce_text), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "ratio_large_post", DoubleToConfigString(config.ratio_large_post), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "ratio_large_comment", DoubleToConfigString(config.ratio_large_comment), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "prob_interest_flashmob_tag", DoubleToConfigString(config.prob_interest_flashmob_tag),
	               "DOUBLE", "params_default.ini");
	AddConfigEntry(entries, "prob_random_per_level", DoubleToConfigString(config.prob_random_per_level), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "flashmob_tag_min_level", DoubleToConfigString(config.flashmob_tag_min_level), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "flashmob_tag_max_level", DoubleToConfigString(config.flashmob_tag_max_level), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "flashmob_tag_dist_exp", DoubleToConfigString(config.flashmob_tag_dist_exp), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "prob_forum_deleted", DoubleToConfigString(config.prob_forum_deleted), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "prob_memb_deleted", DoubleToConfigString(config.prob_memb_deleted), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "prob_photo_deleted", DoubleToConfigString(config.prob_photo_deleted), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "prob_comment_deleted", DoubleToConfigString(config.prob_comment_deleted), "DOUBLE",
	               "params_default.ini");
	AddConfigEntry(entries, "prob_like_deleted", DoubleToConfigString(config.prob_like_deleted), "DOUBLE",
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

static unique_ptr<GlobalTableFunctionState> LdbcGenPersonCoreInit(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
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
	RegisterLdbcBiQueries(loader);

	TableFunction ldbcgen("ldbcgen", {}, LdbcGenFunction, LdbcGenBind, LdbcGenInit);
	ldbcgen.named_parameters["sf"] = LogicalType::DOUBLE;
	ldbcgen.named_parameters["catalog"] = LogicalType::VARCHAR;
	ldbcgen.named_parameters["output_dir"] = LogicalType::VARCHAR;
	ldbcgen.named_parameters["target"] = LogicalType::VARCHAR;
	ldbcgen.named_parameters["schema"] = LogicalType::VARCHAR;
	ldbcgen.named_parameters["format"] = LogicalType::VARCHAR;
	ldbcgen.named_parameters["dictionary_dir"] = LogicalType::VARCHAR;
	ldbcgen.named_parameters["threads"] = LogicalType::BIGINT;
	ldbcgen.named_parameters["overwrite"] = LogicalType::BOOLEAN;
	ldbcgen.named_parameters["primary_keys"] = LogicalType::BOOLEAN;
	ldbcgen.table_scan_progress = LdbcGenProgress;
	loader.RegisterFunction(ldbcgen);

	TableFunction ldbcgen_schema("ldbcgen_schema", {}, LdbcGenSchemaFunction, LdbcGenSchemaBind, LdbcGenSchemaInit);
	ldbcgen_schema.named_parameters["format"] = LogicalType::VARCHAR;
	loader.RegisterFunction(ldbcgen_schema);

	TableFunction ldbcgen_forum_update_chunks("ldbcgen_forum_update_chunks", {}, LdbcForumUpdateChunkFunction,
	                                          LdbcForumUpdateChunkBind, LdbcForumUpdateChunkInit);
	ldbcgen_forum_update_chunks.named_parameters["token"] = LogicalType::VARCHAR;
	ldbcgen_forum_update_chunks.named_parameters["operation"] = LogicalType::VARCHAR;
	ldbcgen_forum_update_chunks.named_parameters["relation"] = LogicalType::VARCHAR;
	loader.RegisterFunction(ldbcgen_forum_update_chunks);

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
