FROM python:3.13.0-alpine3.20 AS spark-base

ENV SPARK_VERSION=3.5.2
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${SPARK_HOME:-"/opt/hadoop"}

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}

RUN apk update --no-cache && apk upgrade --no-cache \
    && apk add --no-cache \
        curl \
        rsync \
        openjdk11 --repository=http://dl-cdn.alpinelinux.org/alpine/edge/community \
        bash

WORKDIR ${SPARK_HOME}

RUN curl https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
    && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

RUN pip3 install pyspark==3.5.2

ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_MASTER_PORT=7077
ENV PYSPARK_PYTHON=python3

COPY ./spark-defaults.conf "$SPARK_HOME/conf"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH

COPY --chmod=777 ./entrypoint.sh .
ENTRYPOINT [ "./entrypoint.sh" ]
