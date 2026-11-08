#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"
namespace duckdb {
namespace spark {
class SparkOptimizerExtension : public OptimizerExtension {
public:
	SparkOptimizerExtension();
};
} // namespace spark
void OptimizeSparkPlan(duckdb::OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
} // namespace duckdb
