#pragma once

#include "duckdb/common/common.hpp"

#include <unordered_map>

namespace duckdb {
class ClientContext;

namespace ldbc {

class LDBCGenWrapper {
public:
	static void CreateLDBCSchema(ClientContext &context, string catalog, string schema, bool overwrite,
	                             bool primary_keys = false);
	static unordered_map<string, idx_t> LoadLDBCData(ClientContext &context, string catalog, string schema,
	                                                 string dictionary_dir, double scale_factor);
	static idx_t RelationCount();
	static string RelationName(idx_t relation_index);
};

} // namespace ldbc
} // namespace duckdb
