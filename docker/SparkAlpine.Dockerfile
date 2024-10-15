FROM python:3.13.0-alpine3.20 AS spark-base

ENV SPARK_VERSION=3.5.2
ENV SPARK_HOME="/opt/spark"
ENV HADOOP_HOME=${SPARK_HOME:-"/opt/hadoop"}

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}

RUN apk update --no-cache && apk upgrade --no-cache \
    && apk add --no-cache \
        rsync=3.3.0-r0 \
        openjdk11=11.0.24_p8-r0 \
        bash=5.2.26-r0

WORKDIR ${SPARK_HOME}

RUN wget -nv -O spark-${SPARK_VERSION}-bin-hadoop3.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
    && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

RUN pip3 install --no-cache-dir pyspark==3.5.2

ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST=spark-master
ENV SPARK_MASTER_PORT=7077
ENV SPARK_NO_DAEMONIZE=true
ENV PYSPARK_PYTHON=python3

ADD https://raw.githubusercontent.com/lkellermann/spark-lab/refs/heads/main/docker/spark-defaults.conf "$SPARK_HOME/conf/spark-defaults.conf"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python

ADD --chmod=777 https://raw.githubusercontent.com/lkellermann/spark-lab/refs/heads/main/docker/entrypoint.sh ./entrypoint.sh
ENTRYPOINT [ "./entrypoint.sh" ]
