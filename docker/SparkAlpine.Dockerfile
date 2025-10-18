FROM python:3.13.0-alpine3.20 AS spark-base

ENV SPARK_VERSION=3.5.7
ENV SPARK_HOME="/opt/spark"
ENV SCALA_VERSION=2.13
ENV DELTA_SPARK_VERSION=3.3.2

ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_MASTER_PORT=7077
ENV SPARK_NO_DAEMONIZE=true
ENV SPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_${SCALA_VERSION}:${DELTA_SPARK_VERSION} --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH=$SPARK_HOME/python

RUN mkdir -p ${SPARK_HOME} && apk update --no-cache && apk upgrade --no-cache \
    && apk add --no-cache \
        bash \
        curl \
        openjdk11 \
        rsync

WORKDIR ${SPARK_HOME}


ADD https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3-scala${SCALA_VERSION}.tgz ./

RUN tar xvzf spark-${SPARK_VERSION}-bin-hadoop3-scala${SCALA_VERSION}.tgz --directory ${SPARK_HOME} --strip-components 1 \
    && rm -rf spark-${SPARK_VERSION}-bin-hadoop3-scala${SCALA_VERSION}.tgz \
    && chmod u+x ${SPARK_HOME}/sbin/* \
    && chmod u+x ${SPARK_HOME}/bin/* \
    && pip3 install --no-cache-dir pyspark==${SPARK_VERSION} delta-spark==${DELTA_SPARK_VERSION}

COPY --chmod=777 ./spark-defaults.conf ./conf/spark-defaults.conf
COPY --chmod=777 ./entrypoint.sh ./entrypoint.sh

ENTRYPOINT [ "./entrypoint.sh" ]