#include "spark_execute_command.hpp"

#include "duckdb/main/attached_database.hpp"
#include "spark_catalog.hpp"
#include "spark_client.hpp"

#include <arrow/array/array_binary.h>
namespace duckdb {
namespace spark {

// GlobalFunctionState to store arrow Reader or record batches
struct SparkExecuteCommandGlobalFunctionState : public GlobalTableFunctionState {
	explicit SparkExecuteCommandGlobalFunctionState() {
	}
	bool done = false;
	mutex lock;
};

// Table Function Data
struct SparkExecuteCommandBindData : public TableFunctionData {
	explicit SparkExecuteCommandBindData(shared_ptr<SparkGRPCClient> &spark_client, string &sql_command)
	    : spark_client(spark_client) {
		// build Spark gRPC Plan
		execute_command_plan = spark_client->PlanExecuteSQLCommand(sql_command);
	}
	shared_ptr<SparkGRPCClient> spark_client;
	::spark::connect::Plan execute_command_plan;
};

static void SparkExecuteCommandFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<SparkExecuteCommandBindData>();
	auto &global_state = data_p.global_state->Cast<SparkExecuteCommandGlobalFunctionState>();
	auto spark_client = bind_data.spark_client;

	if (global_state.done) {
		return;
	}

	// lock for exclusivity in get next from stream/batches
	std::lock_guard<mutex> guard(global_state.lock);

	auto [status, sql_cmd_relation] = spark_client->GetSQLCommandResultRelation(bind_data.execute_command_plan);

	output.SetCardinality(1);

	if (!status.ok()) {
		// status = FAILED
		FlatVector::GetData<string_t>(output.data[0])[0] = StringVector::AddString(output.data[0], "FAILED");
		// message = error message
		FlatVector::GetData<string_t>(output.data[1])[0] =
		    StringVector::AddString(output.data[1], status.error_message());
	} else {
		// status = SUCCEED
		FlatVector::GetData<string_t>(output.data[0])[0] = StringVector::AddString(output.data[0], "SUCCEED");
		// collect relation output of sql command
		if (!sql_cmd_relation) {
			// message = NULL
			FlatVector::SetNull(output.data[1], 0, true);
		}
		auto sql_cmd_string_relation = spark_client->ShowString(*sql_cmd_relation);
		auto sql_cmd_string_plan = spark_client->PlanFromRelation(sql_cmd_string_relation);
		auto batches = spark_client->GetRecordBatches(sql_cmd_string_plan);
		auto first_batch = batches[0];
		auto first_col = first_batch->column(0);
		auto first_array = std::static_pointer_cast<arrow::StringArray>(first_col);
		if (first_array->IsNull(0)) {
			// message = NULL
			FlatVector::SetNull(output.data[1], 0, true);
		} else {
			auto value = first_array->GetString(0);
			// message = relation.show()
			FlatVector::GetData<string_t>(output.data[1])[0] = StringVector::AddString(output.data[1], value);
		}
	}
	global_state.done = true;
}

static unique_ptr<GlobalTableFunctionState> SparkExecuteCommandInitGlobalState(ClientContext &context,
                                                                               TableFunctionInitInput &input) {
	auto state = make_uniq<SparkExecuteCommandGlobalFunctionState>();
	auto &bind_data = input.bind_data->CastNoConst<SparkExecuteCommandBindData>();
	auto spark_client = bind_data.spark_client;

	return std::move(state);
}

static unique_ptr<FunctionData> SparkExecuteCommandBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	// Get parameters
	auto catalog_name = input.inputs[0].GetValue<string>();
	auto sql_command = input.inputs[1].GetValue<string>();

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
		unique_ptr<SparkExecuteCommandBindData> bind_data =
		    make_uniq<SparkExecuteCommandBindData>(sparkClient, sql_command);
		names.emplace_back("status");
		return_types.emplace_back(duckdb::LogicalTypeId::VARCHAR);
		names.emplace_back("message");
		return_types.emplace_back(duckdb::LogicalTypeId::VARCHAR);
		return std::move(bind_data);
	} else {
		throw BinderException("Database " + catalog_name + " is not found");
	}
}

SparkExecuteCommandFunction::SparkExecuteCommandFunction()
    : TableFunction("spark_command", {LogicalType::VARCHAR, LogicalType::VARCHAR}, SparkExecuteCommandFunc,
                    SparkExecuteCommandBind, SparkExecuteCommandInitGlobalState) {};
} // namespace spark
} // namespace duckdb
