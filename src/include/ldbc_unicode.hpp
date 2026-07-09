#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

string LdbcJavaNormalizeNfdStripDiacritics(const string &value);
string LdbcEmailBaseFromFirstName(const string &first_name);

} // namespace duckdb
