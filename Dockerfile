# builder step used to download and configure spark environment
FROM openjdk:11.0.11-jre-slim-buster as builder

USER root
# Add Dependencies for PySpark
RUN apt-get update && apt-get install -y curl sudo nano vim wget software-properties-common ssh net-tools ca-certificates zip

# Fix the value of PYTHONHASHSEED
# Note: this is needed when you use Python 3.3 or greater
ENV SPARK_VERSION=3.4.0 \
HADOOP_VERSION=3 \
SPARK_HOME=/opt/spark \
PYTHONHASHSEED=1

# Download and uncompress spark from the apache archive
RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
&& mkdir -p /opt/spark \
&& tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
&& rm apache-spark.tgz


# Install HUDI 3.4 - 0.14
# RUN wget -O hudi-spark3.4-bundle_2.12-0.14.1.jar https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.4-bundle_2.12/0.14.1/hudi-spark3.4-bundle_2.12-0.14.1.jar
# RUN mv hudi-spark3.4-bundle_2.12-0.14.1.jar /opt/spark/jars/

# install Delta lake 3.4
RUN wget -O delta-core_2.12-2.4.0.jar https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar
RUN mv delta-core_2.12-2.4.0.jar /opt/spark/jars/

# install Delta Storage 3.2
RUN wget -O delta-storage-3.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar
RUN mv delta-storage-3.2.0.jar /opt/spark/jars/

# Apache spark environment
FROM builder as apache-spark

WORKDIR /opt/spark

ENV SPARK_MASTER_PORT=7077 \
SPARK_MASTER_WEBUI_PORT=8888 \
SPARK_LOG_DIR=/opt/spark/logs \
SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
SPARK_WORKER_WEBUI_PORT=8085 \
SPARK_WORKER_PORT=7000 \
SPARK_MASTER="spark://spark-master:7077" \
SPARK_WORKLOAD="master" \
PYSPARK_PYTHON=python3.10

EXPOSE 8888 8085 8080 7077 6066 4040 22 443

RUN mkdir -p $SPARK_LOG_DIR && \
touch $SPARK_MASTER_LOG && \
touch $SPARK_WORKER_LOG && \
ln -sf /dev/stdout $SPARK_MASTER_LOG && \
ln -sf /dev/stdout $SPARK_WORKER_LOG

COPY start-spark.sh /

# Install python3.10
COPY install-python3.sh /

RUN /install-python3.sh

# Install depdencies
COPY apps/ apps/

WORKDIR /opt/spark/apps

# RUN poetry install
# RUN poetry build

# RUN poetry export -f requirements.txt --without-hashes -o requirements.txt
# RUN poetry run pip install . -r requirements.txt -t package_tmp
# RUN cd package_tmp
# RUN find . -name "*.pyc" -delete
# RUN zip -r ../package .


RUN python3.10 -m pip install -r requirements.txt

RUN python3.10 -m pip install . -r requirements.txt -t package_tmp
RUN cd package_tmp
RUN find . -name "*.pyc" -delete
RUN zip -r ../package .


RUN poetry build

RUN python3.10 -m pip install ./dist/spark_apps-0.1.0.tar.gz

CMD ["/bin/bash", "/start-spark.sh"]