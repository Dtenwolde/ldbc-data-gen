#define DUCKDB_EXTENSION_MAIN

#include "ldbc_data_gen_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void LdbcDataGenScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "...........🦆 " + name.GetString());
	});
}

inline void LdbcDataGenOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "LdbcDataGen " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto ldbc_data_gen_scalar_function =
	    ScalarFunction("ldbc_data_gen", {LogicalType::VARCHAR}, LogicalType::VARCHAR, LdbcDataGenScalarFun);

	loader.RegisterFunction(ldbc_data_gen_scalar_function);

	// Register another scalar function
	auto ldbc_data_gen_openssl_version_scalar_function = ScalarFunction("ldbc_data_gen_openssl_version", {LogicalType::VARCHAR},
	                                                             LogicalType::VARCHAR, LdbcDataGenOpenSSLVersionScalarFun);
	loader.RegisterFunction(ldbc_data_gen_openssl_version_scalar_function);
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
