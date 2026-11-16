#pragma once

#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "spark_arrow_reader.hpp"
#include "spark_client.hpp"
namespace duckdb {
namespace spark {
class SparkExecuteQueryFunction : public TableFunction {
public:
	SparkExecuteQueryFunction();

private:
	static unique_ptr<FunctionData> SparkExecuteQueryBind(ClientContext &context, TableFunctionBindInput &input,
	                                                      vector<LogicalType> &return_types, vector<string> &names);
	static unique_ptr<GlobalTableFunctionState> SparkExecuteQueryInitGlobal(ClientContext &context,
	                                                                        TableFunctionInitInput &input);
	static unique_ptr<LocalTableFunctionState> SparkExecuteQueryInitLocal(ExecutionContext &context,
	                                                                      TableFunctionInitInput &input,
	                                                                      GlobalTableFunctionState *global_state_p);
	static void SparkExecuteQueryFunc(ClientContext &ctx, TableFunctionInput &data_p, DataChunk &output);
};

struct SparkExecuteQueryBindData : public ArrowScanFunctionData {
	// default constructor
	explicit SparkExecuteQueryBindData(shared_ptr<SparkGRPCClient> &spark_client, const string &sql_query)
	    : ArrowScanFunctionData(&SparkStreamFactory::Produce, 0, make_shared_ptr<FactoryDependency>(nullptr)),
	      spark_client(spark_client), sql_query(sql_query) {
		execute_query_plan = spark_client->PlanExecuteSQLQuery(sql_query);
	}

	shared_ptr<FactoryDependency> GetFactoryDependency() {
		return dependency ? shared_ptr_cast<DependencyItem, FactoryDependency>(dependency) : nullptr;
	}

	SparkExecuteQueryBindData(const SparkExecuteQueryBindData &) = delete;
	SparkExecuteQueryBindData &operator=(const SparkExecuteQueryBindData &) = delete;

	shared_ptr<SparkGRPCClient> spark_client;
	::spark::connect::Plan execute_query_plan;
	vector<string> names;
	string sql_query;
};

struct SparkExecuteQueryGlobalState : public ArrowScanGlobalState {
	mutex lock;
	atomic<idx_t> position = 0;
};

struct SparkExecuteQueryLocalState : public ArrowScanLocalState {
	explicit SparkExecuteQueryLocalState(unique_ptr<ArrowArrayWrapper> current_chunk, ClientContext &context)
	    : ArrowScanLocalState(std::move(current_chunk), context) {
	}
};

} // namespace spark
} // namespace duckdb
