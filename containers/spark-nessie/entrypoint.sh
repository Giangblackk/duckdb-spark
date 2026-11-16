#!/bin/bash

echo "Starting Spark Connect Server"

bash /opt/spark/sbin/start-connect-server.sh \
    --packages org.apache.spark:spark-connect_2.12:3.5.2,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.96.0 \
    --jars /opt/spark/jars/iceberg-aws-bundle-1.6.1.jar \
    --driver-class-path /opt/spark/jars/iceberg-aws-bundle-1.6.1.jar \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions \
    --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.nessie.warehouse=s3://warehouse \
    --conf spark.sql.catalog.nessie.s3.endpoint=http://minio:9000 \
    --conf spark.sql.catalog.nessie.s3.path-style-access=true \
    --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
    --conf spark.sql.catalog.nessie.io-impl=org.apache.iceberg.io.ResolvingFileIO \
    --conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v1 \
    --conf spark.sql.catalog.nessie.ref=main \
    --conf spark.sql.catalog.nessie.cache-enabled=false \
    --conf spark.streaming.stopGracefullyOnShutdown=true \
    --conf spark.decommission.enabled=true

sleep infinity