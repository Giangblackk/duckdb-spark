#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"
namespace duckdb {
namespace spark {
class SparkOptimizerExtension : public OptimizerExtension {
public:
	SparkOptimizerExtension();
};
} // namespace spark
// This runs BEFORE the DuckDB built-in optimizers have run
void PreOptimizeSparkPlan(duckdb::OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
// This runs AFTER the DuckDB built-in optimizers have run
void OptimizeSparkPlan(duckdb::OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

} // namespace duckdb
