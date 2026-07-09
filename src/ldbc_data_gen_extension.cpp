#define DUCKDB_EXTENSION_MAIN

#include "ldbc_data_gen_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct LdbcGenBindData : public TableFunctionData {
	double scale_factor = 1.0;
	string output_dir;
	string format = "parquet";
	bool overwrite = false;
};

struct LdbcGenGlobalState : public GlobalTableFunctionState {
	bool emitted = false;
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

static unique_ptr<FunctionData> LdbcGenBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	if (!input.inputs.empty()) {
		throw BinderException("ldbcgen only accepts named parameters");
	}

	auto result = make_uniq<LdbcGenBindData>();
	result->scale_factor = GetDoubleParameter(input, "sf", 1.0);
	result->output_dir = GetStringParameter(input, "output_dir", "");
	result->format = StringUtil::Lower(GetStringParameter(input, "format", "parquet"));
	result->overwrite = GetBooleanParameter(input, "overwrite", false);

	if (result->scale_factor <= 0) {
		throw BinderException("ldbcgen parameter sf must be greater than zero");
	}
	if (result->format != "parquet" && result->format != "csv") {
		throw BinderException("ldbcgen parameter format must be either 'parquet' or 'csv'");
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
	if (state.emitted) {
		return;
	}

	auto path = bind_data.output_dir.empty() ? string() : bind_data.output_dir;
	output.data[0].SetValue(0, "ldbc_snb_bi_static");
	output.data[1].SetValue(0, path);
	output.data[2].SetValue(0, Value(LogicalType::BIGINT));
	output.data[3].SetValue(0, Value(LogicalType::VARCHAR));
	output.data[4].SetValue(0, bind_data.format);
	output.data[5].SetValue(0, "planned");
	output.SetCardinality(1);
	state.emitted = true;
}

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction ldbcgen("ldbcgen", {}, LdbcGenFunction, LdbcGenBind, LdbcGenInit);
	ldbcgen.named_parameters["sf"] = LogicalType::DOUBLE;
	ldbcgen.named_parameters["output_dir"] = LogicalType::VARCHAR;
	ldbcgen.named_parameters["format"] = LogicalType::VARCHAR;
	ldbcgen.named_parameters["overwrite"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(ldbcgen);
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
