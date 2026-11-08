#!/bin/bash

echo "Starting Spark Connect Server"

bash /opt/spark/sbin/start-connect-server.sh \
    --conf spark.sql.extensions=org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider \


sleep infinity