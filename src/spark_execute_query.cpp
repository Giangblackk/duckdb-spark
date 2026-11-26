#include "spark_execute_query.hpp"

#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parser.hpp"
#include "spark_catalog.hpp"
#include "spark_client.hpp"

#include <arrow/type_fwd.h>

namespace duckdb {
namespace spark {

void SparkExecuteQueryFunction::SparkExecuteQueryFunc(ClientContext &context, TableFunctionInput &data_p,
                                                      DataChunk &output) {
	if (!data_p.local_state) {
		return;
	}

	auto &bind_data = data_p.bind_data->CastNoConst<SparkExecuteQueryBindData>();
	auto &global_state = data_p.global_state->Cast<SparkExecuteQueryGlobalState>();
	auto &local_state = data_p.local_state->Cast<SparkExecuteQueryLocalState>();

	local_state.all_columns.Reset();
	if (local_state.chunk_offset >= static_cast<idx_t>(local_state.chunk->arrow_array.length)) {
		if (!ArrowTableFunction::ArrowScanParallelStateNext(context, data_p.bind_data.get(), local_state,
		                                                    global_state)) {
			return;
		}
	}

	auto output_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, NumericCast<idx_t>(local_state.chunk->arrow_array.length) -
	                                                             local_state.chunk_offset);
	output.SetCardinality(output_size);
	if (output_size > 0) {
		bool arrow_scan_is_projected =
		    !local_state.column_ids.empty() && local_state.column_ids.size() < bind_data.names.size();
		ArrowTableFunction::ArrowToDuckDB(local_state, bind_data.arrow_table.GetColumns(), output, 0,
		                                  arrow_scan_is_projected, COLUMN_IDENTIFIER_ROW_ID);
	}
	output.Verify();
	local_state.chunk_offset += output.size();
	lock_guard<mutex> glock(global_state.lock);
	global_state.position += output.size();
}

unique_ptr<LocalTableFunctionState>
SparkExecuteQueryFunction::SparkExecuteQueryInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                      GlobalTableFunctionState *global_state_p) {
	auto &client_context = context.client;
	auto &bind_data = input.bind_data->CastNoConst<SparkExecuteQueryBindData>();
	auto &gstate = global_state_p->Cast<SparkExecuteQueryGlobalState>();

	auto current_chunk = make_uniq<ArrowArrayWrapper>();
	auto lstate = make_uniq<SparkExecuteQueryLocalState>(std::move(current_chunk), client_context);

	auto sorted_column_ids = input.column_ids;
	std::sort(sorted_column_ids.begin(), sorted_column_ids.end());

	lstate->column_ids = sorted_column_ids;
	lstate->filters = input.filters.get();
	if (!bind_data.projection_pushdown_enabled) {
		lstate->column_ids.clear();
	} else if (!input.projection_ids.empty() || !gstate.projection_ids.empty()) {
		auto &asgs = global_state_p->Cast<ArrowScanGlobalState>();
		lstate->all_columns.Initialize(client_context, asgs.scanned_types);
	}
	if (!ArrowTableFunction::ArrowScanParallelStateNext(client_context, input.bind_data.get(), *lstate, gstate)) {
		return nullptr;
	}

	return std::move(lstate);
}

unique_ptr<ArrowArrayStreamWrapper> SparkExecuteQueryProduceArrowScan(const ArrowScanFunctionData &function,
                                                                      const vector<column_t> &column_ids,
                                                                      TableFilterSet *filters) {
	ArrowStreamParameters parameters;
	return function.scanner_producer(function.stream_factory_ptr, parameters);
}

unique_ptr<GlobalTableFunctionState>
SparkExecuteQueryFunction::SparkExecuteQueryInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->CastNoConst<SparkExecuteQueryBindData>();
	auto spark_client = bind_data.spark_client;

	auto gstate = make_uniq<SparkExecuteQueryGlobalState>();
	auto factory = make_shared_ptr<SparkStreamFactory>(bind_data.spark_client, bind_data.execute_query_plan);
	auto factory_dependency = bind_data.GetFactoryDependency();
	if (!factory_dependency) {
		throw InternalException("Factory dependency not initialized");
	}

	factory_dependency->factory = factory;
	bind_data.stream_factory_ptr = reinterpret_cast<uintptr_t>(factory.get());

	gstate->stream = SparkExecuteQueryProduceArrowScan(bind_data, input.column_ids, input.filters.get());

	return std::move(gstate);
}

unique_ptr<FunctionData> SparkExecuteQueryFunction::SparkExecuteQueryBind(ClientContext &context,
                                                                          TableFunctionBindInput &input,
                                                                          vector<LogicalType> &return_types,
                                                                          vector<string> &names) {
	// Get parameters
	auto catalog_name = input.inputs[0].GetValue<string>();
	auto sql_query = input.inputs[1].GetValue<string>();

	auto &database_manager = DatabaseManager::Get(context);
	auto database = database_manager.GetDatabase(context, catalog_name);

	if (database) {
		auto &catalog = database->GetCatalog();
		if (catalog.GetCatalogType() != "spark") {
			throw BinderException("Database " + catalog_name + " is not a Spark database/catalog");
		}
		auto &spark_catalog = catalog.Cast<SparkCatalog>();
		auto &sparkClient = spark_catalog.spark_client;
		// Add Spark gRPC client to table function data
		unique_ptr<SparkExecuteQueryBindData> bind_data = make_uniq<SparkExecuteQueryBindData>(sparkClient, sql_query);

		// parsing sql_query to validate it's valid query for execution
		Parser parser;
		parser.ParseQuery(sql_query);
		if (parser.statements.size() == 0) {
			throw BinderException("There is no statement in the input SQL query");
		}
		if (parser.statements.size() != 1) {
			throw BinderException("SQL query of multiple statements are currently not supported");
		}
		auto &stmt = parser.statements[0];
		switch (stmt->type) { // StatementType enum
		case StatementType::SELECT_STATEMENT:
			break;
		default:
			throw BinderException("This function does not support SQL statements of type `%s`. Please switch to use "
			                      "`spark_command` for more versatile SQL commands.",
			                      StatementTypeToString(stmt->type));
			break;
		}

		// get arrow schema from analyzing plan, export and populate shema to attributes
		auto arrow_shema = bind_data->spark_client->AnalyzePlanToArrowSchema(bind_data->execute_query_plan);

		// avoid edge case of empty output schema
		if (arrow_shema->num_fields() == 0) {
			throw BinderException(
			    "Empty output schema. If you are executing Spark SQL commands which return empty output. "
			    "Please switch to use `spark_command` for more versatile SQL commands.");
		}

		//  export schema from C++ to C
		auto status = arrow::ExportSchema(*std::move(arrow_shema), &bind_data->schema_root.arrow_schema);
		if (!status.ok()) {
			throw BinderException("Arrow schema export failed: " + status.ToString());
		}
		ArrowTableFunction::PopulateArrowTableSchema(duckdb::DBConfig::GetConfig(context), bind_data->arrow_table,
		                                             bind_data->schema_root.arrow_schema);

		// Initialize the names and return types from analyze plan
		names = bind_data->arrow_table.GetNames();
		return_types = bind_data->arrow_table.GetTypes();

		bind_data->names = names;
		bind_data->all_types = return_types;

		return std::move(bind_data);
	} else {
		throw BinderException("Database " + catalog_name + " is not found");
	}
}

SparkExecuteQueryFunction::SparkExecuteQueryFunction()
    : TableFunction("spark_query", {LogicalType::VARCHAR, LogicalType::VARCHAR},
                    SparkExecuteQueryFunction::SparkExecuteQueryFunc, SparkExecuteQueryFunction::SparkExecuteQueryBind,
                    SparkExecuteQueryFunction::SparkExecuteQueryInitGlobal,
                    SparkExecuteQueryFunction::SparkExecuteQueryInitLocal) {};
} // namespace spark
} // namespace duckdb
