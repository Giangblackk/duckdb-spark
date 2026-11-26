# DuckDB Spark Extension

## Introduction

`duckdb-spark` is a DuckDB extension that enables DuckDB to connect to Apache Spark via the [Spark Connect API](https://spark.apache.org/docs/latest/spark-connect-overview.html), allowing DuckDB to leverage the broader Spark ecosystem.

The vision of this project is to enable hybrid data processing systems that combine the strengths of distributed computing (scalability) with local computing (performance).

## Overall Design

### Overview

The `duckdb-spark` extension acts as a bridge between DuckDB and the Spark Connect Driver, enabling DuckDB to utilize the resources of scalable, distributed Spark clusters. It connects to a broad range of data systems (Cloudera, Databricks, etc.) and open lakehouses (Apache Iceberg, Delta Lake, Apache Hudi, Apache Paimon, etc.), allowing users to explore and analyze data without needing to manage the underlying storage or table format details.

This project is made possible by two key foundations:

1. **Spark Connect API** — built on two language-agnostic technologies: [gRPC](https://grpc.io/) and [Apache Arrow](https://arrow.apache.org/) in-memory columnar data format.
2. **DuckDB's Apache Arrow integration** — enables zero-copy data access and conversion between DuckDB's internal format and Arrow `RecordBatch`.

```mermaid
flowchart TD

spark_cluster(Spark Cluster)
spark_connect(Spark Connect Driver)
duckdb_ext(duckdb-spark extension)
duckdb(DuckDB)

duckdb --> duckdb_ext -- gRPC + Arrow --> spark_connect --> spark_cluster
```

### Read Path

```mermaid
flowchart TD

storage(Data Storage)
catalog(Data Catalog)
spark_connect(Spark Connect Driver & Cluster)
duckdb_ext(DuckDB Extension with grpc and arrow)
duckdb(DuckDB)

catalog -- get tables metadata --> spark_connect
storage -- read tables as DataFrame --> spark_connect -- send table data as Arrow batches via gRPC --> duckdb_ext -- read data as arrow table --> duckdb
```

### Write Path

```mermaid
flowchart TD

storage(Data Storage)
catalog(Data Catalog)
spark_connect(Spark Connect Driver)
duckdb_ext(DuckDB Extension with grpc and arrow)
duckdb(DuckDB)

duckdb -- write data as arrow table --> duckdb_ext -- write data as arrow batch via gRPC --> spark_connect -- write data from DataFrame to tables --> storage
spark_connect -- update table metadata --> catalog
```

## Documentation

- [Extension Docs](./docs/README.md)
