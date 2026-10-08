FROM apache/sedona:1.9.1

# setup workdir
WORKDIR /opt/spark

USER root

# copy entrypoint script and chmod
COPY ./entrypoint.sh /opt/spark/

RUN chmod 755 /opt/spark/entrypoint.sh

ENTRYPOINT [ "/opt/spark/entrypoint.sh" ]