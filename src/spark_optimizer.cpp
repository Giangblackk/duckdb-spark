#include "spark_optimizer.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "spark_scan_table.hpp"
#include <cstdint>

namespace duckdb {

// Walk the plan tree, looking for LIMIT operator directly above an spark_scan_table (with optional intermediate
// PROJECTION nodes). When found, the constant limit and offset values are stored in the bind data and the LIMIT
// operator is removed from the plan so that DuckDB does not duplicate limit enforcement.
static void OptimizeLimitPushdown(unique_ptr<LogicalOperator> &op) {
	if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
		auto &limit_op = op->Cast<LogicalLimit>();

		// Walk through projections to find the underlying GET operator.
		// The pattern we're looking for is LIMIT -> PROJECTION(s) -> GET.
		reference<LogicalOperator> child_ref = *op->children[0];
		while (child_ref.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
			if (child_ref.get().children.empty()) {
				break;
			}
			child_ref = *child_ref.get().children[0];
		}
		if (child_ref.get().type == LogicalOperatorType::LOGICAL_GET) {
			auto &get_op = child_ref.get().Cast<LogicalGet>();

			if (get_op.function.name == "spark_scan_table" && get_op.bind_data) {
				// Check if we can extract constant limit and offset values.
				int32_t limit_value = -1;
				int32_t offset_value = 0;
				bool can_pushdown = true;

				// Extract limit value if it's a constant.
				if (limit_op.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
					limit_value = static_cast<int32_t>(limit_op.limit_val.GetConstantValue());
				} else {
					// Non-constant limit (expression or percentage), UNSUPPORTED => cannot pushdown.
					can_pushdown = false;
				}

				// Extract offset value if it's a constant
				if (limit_op.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
					offset_value = static_cast<int32_t>(limit_op.offset_val.GetConstantValue());
				} else {
					// Non-constant offset (expression or percentage), UNSUPPORTED => cannot pushdown.
					can_pushdown = false;
				}

				if (can_pushdown && (limit_value > 0 || offset_value > 0)) {
					// Store limit and offset value in the SparkScanTableBindData
					auto &spark_bind_data = get_op.bind_data->Cast<spark::SparkScanTableBindData>();
					spark_bind_data.limit = limit_value;
					spark_bind_data.offset = offset_value;

					// Remove LIMIT operator from the plan
					op = std::move(op->children[0]);

					// Continue optimizing the new root (which was the child)
					OptimizeLimitPushdown(op);
					return;
				}
			}
		}

		// Could not pushdown, recurse into children
		for (auto &child : op->children) {
			OptimizeLimitPushdown(child);
		}
		return;
	}

	// For all other operator types, recurse into children
	for (auto &child : op->children) {
		OptimizeLimitPushdown(child);
	}
}

void OptimizeSparkPlan(duckdb::OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	OptimizeLimitPushdown(plan);
}

namespace spark {
spark::SparkOptimizerExtension::SparkOptimizerExtension() {
	optimize_function = OptimizeSparkPlan;
}

} // namespace spark
} // namespace duckdb
