FROM python:3.11-bullseye as spark-base

ENV SPARK_VERSION=3.5.2
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${SPARK_HOME:-"/opt/hadoop"}

RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}

RUN apt-get update && \
    apt-get install -y \
    sudo \
    curl \
    vim \
    unzip \
    rsync \
    openjdk-11-jdk\
    build-essential \
    software-properties-common \
    ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR ${SPARK_HOME}

RUN curl https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
    && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

COPY ./requirements.txt .
RUN pip3 install -r requirements.txt

ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
ENV SPARK_HOME="/opt/spark"
ENV SPARK_MASTER="spark://spark-master:7077"
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

COPY ./spark-defaults.conf "$SPARK_HOME/conf"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH

COPY --chmod=777 ./entrypoint.sh .
ENTRYPOINT [ "./entrypoint.sh" ]