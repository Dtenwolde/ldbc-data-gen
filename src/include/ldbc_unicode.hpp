#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

string LdbcJavaNormalizeNfdStripDiacritics(const string &value);
string LdbcEmailBaseFromFirstName(const string &first_name);
int32_t LdbcJavaStringLength(const string &value);
string LdbcJavaSubstring(const string &value, int32_t offset, int32_t length);

} // namespace duckdb
