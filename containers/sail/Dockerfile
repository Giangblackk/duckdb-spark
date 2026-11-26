ARG PYTHON_VERSION=3.14

FROM python:${PYTHON_VERSION}-slim

ARG PYSAIL_VERSION
ARG PYSPARK_VERSION=4.2.0

RUN python3 -m pip install --no-cache-dir "pysail==${PYSAIL_VERSION}"
RUN python3 -m pip install --no-cache-dir "pyspark-client==${PYSPARK_VERSION}"

ENV SAIL_CATALOG__LIST='[{type="memory", name="sail", initial_database=["dev", "analytics"], initial_database_comment="Development analytics database"}]'

ENTRYPOINT ["/usr/local/bin/sail"]