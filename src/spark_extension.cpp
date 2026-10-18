#include "duckdb/common/helper.hpp"
#include "duckdb/main/config.hpp"
#include "spark_execute_command.hpp"
#include "spark_execute_query.hpp"
#include "spark_scan_table.hpp"
#include "spark_storage.hpp"
#define DUCKDB_EXTENSION_MAIN

#include "spark_extension.hpp"

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>

#define SPARK_EXTENSION_VERSION "0.0.1"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	spark::SparkScanTableFunction spark_scan_table_function;
	loader.RegisterFunction(spark_scan_table_function);

	spark::SparkExecuteQueryFunction spark_query_function;
	loader.RegisterFunction(spark_query_function);

	spark::SparkExecuteCommandFunction spark_command_function;
	loader.RegisterFunction(spark_command_function);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.storage_extensions["spark"] = make_uniq<spark::SparkStorageExtension>();
}

void SparkExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string SparkExtension::Name() {
	return "spark";
}

std::string SparkExtension::Version() const {
	return SPARK_EXTENSION_VERSION;
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(spark, loader) {
	duckdb::LoadInternal(loader);
}
}
