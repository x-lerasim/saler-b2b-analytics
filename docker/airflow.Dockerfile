FROM apache/airflow:2.8.1

USER root


RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         gcc \
         libkrb5-dev \
         krb5-user \
         libffi-dev \
         default-jdk \
         procps \
         curl \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark


RUN curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar zxf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && chown -R airflow: ${SPARK_HOME}

ENV PATH=$PATH:$SPARK_HOME/bin
RUN mkdir -p /home/airflow/.ivy2 && chown -R airflow: /home/airflow/.ivy2

USER airflow


RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-amazon
