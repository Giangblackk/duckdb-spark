#pragma once

#include "duckdb/function/table_function.hpp"
namespace duckdb {
namespace spark {
class SparkExecuteCommandFunction : public TableFunction {
public:
	SparkExecuteCommandFunction();
};
} // namespace spark
} // namespace duckdb
