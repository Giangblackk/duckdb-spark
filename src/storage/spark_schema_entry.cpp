#include "spark_schema_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/enums/on_create_conflict.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "spark_table_set.hpp"

namespace duckdb {
namespace spark {
SparkSchemaEntry::SparkSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info), tables(*this) {
}
SparkSchemaEntry::~SparkSchemaEntry() {
}

bool IsSupportedCatalogType(CatalogType type) {
	// currently support only table entries
	// TODO support other types of entries
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		return true;
	default:
		return false;
	}
}

void SparkSchemaEntry::Scan(ClientContext &context, CatalogType type,
                            const std::function<void(CatalogEntry &)> &callback) {
	if (!IsSupportedCatalogType(type)) {
		return;
	}
	GetCatalogSet(type).Scan(context, callback);
};
void SparkSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan without context is not implemented");
};
optional_ptr<CatalogEntry> SparkSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                         TableCatalogEntry &table) {
	return nullptr;
};
optional_ptr<CatalogEntry> SparkSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	return nullptr;
};
optional_ptr<CatalogEntry> SparkSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &base_info = info.base->Cast<CreateTableInfo>();
	if (base_info.on_conflict == OnCreateConflict::ALTER_ON_CONFLICT) {
		// short-circuit for not implemented case
		throw NotImplementedException("ALTER ON CONFLICT in CreateTable is not implemented for Spark");
	}
	// lookup for table info
	EntryLookupInfo lookup_info(CatalogType::TABLE_ENTRY, base_info.table);
	auto existing_entry = tables.GetEntry(transaction.GetContext(), lookup_info);

	if (existing_entry) {
		if (base_info.on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			// CREATE TABLE: if the table already exists, throw error
			throw CatalogException("table `%s.%s.%s` already existed.", base_info.catalog, base_info.schema,
			                       base_info.table);
		} else if (base_info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			// CREATE TABLE IF NOT EXISTS: if the table already exists, simply return the existing entry
			return existing_entry;
		} else if (base_info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
			// CREATE OR REPLACE TABLE: if the table already exists, drop table first
			DropInfo drop_info;
			drop_info.catalog = base_info.catalog;
			drop_info.schema = base_info.schema;
			drop_info.name = base_info.table;
			drop_info.type = CatalogType::TABLE_ENTRY;
			drop_info.cascade = false;
			drop_info.if_not_found = OnEntryNotFound::RETURN_NULL;
			DropEntry(transaction.GetContext(), drop_info);
		}
	}
	// after all, create table
	return tables.CreateTable(transaction.GetContext(), info);
};
optional_ptr<CatalogEntry> SparkSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	return nullptr;
};
optional_ptr<CatalogEntry> SparkSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                 CreateTableFunctionInfo &info) {
	return nullptr;
};
optional_ptr<CatalogEntry> SparkSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	return nullptr;
};
optional_ptr<CatalogEntry> SparkSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                CreateCopyFunctionInfo &info) {
	return nullptr;
};
optional_ptr<CatalogEntry> SparkSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                  CreatePragmaFunctionInfo &info) {
	return nullptr;
};
optional_ptr<CatalogEntry> SparkSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                             CreateCollationInfo &info) {
	return nullptr;
};
optional_ptr<CatalogEntry> SparkSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	return nullptr;
};
void SparkSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	info.schema = name;
	GetCatalogSet(info.type).DropEntry(context, info);
};

optional_ptr<CatalogEntry> SparkSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                         const EntryLookupInfo &lookup_info) {
	if (!IsSupportedCatalogType(lookup_info.GetCatalogType())) {
		return nullptr;
	}
	return GetCatalogSet(lookup_info.GetCatalogType()).GetEntry(transaction.GetContext(), lookup_info);
};
void SparkSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {};

SparkCatalogSet &SparkSchemaEntry::GetCatalogSet(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		return tables;
	default:
		string error_message = "Spark: Type not supported for GetCatalogSet: " + CatalogTypeToString(type);
		throw NotImplementedException(error_message);
	}
}

} // namespace spark
} // namespace duckdb
