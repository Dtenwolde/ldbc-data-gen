#pragma once

#include "duckdb/common/common.hpp"

#include <istream>
#include <memory>

namespace duckdb {

struct LdbcResource {
	string path;
	std::unique_ptr<std::istream> stream;
};

bool LdbcUsesEmbeddedResources(const string &resource_dir);
string LdbcResourcePath(const string &base, const string &path);
LdbcResource LdbcOpenResourcePath(const string &path);
LdbcResource LdbcOpenResource(const string &resource_dir, const string &relative_path);
LdbcResource LdbcOpenResource(const string &base, const string &path, const string &file_name);
string LdbcReadResourceText(const string &resource_dir, const string &relative_path);

} // namespace duckdb
