# Use a custom Airflow as base image
FROM puckel/docker-airflow:latest

# Developer and mainteiner of the project
LABEL maintainer="Aleksandr Klein"

# Switch to ROOT user for installing mandatory packages
USER root

# Install mandatory packages
RUN apt-get update \
 && pip install --upgrade pip \
 && apt-get install -y libpq-dev gcc \
 && apt install wget -y \
 && apt-get install software-properties-common -y \
 && apt-add-repository 'deb http://security.debian.org/debian-security stretch/updates main' \
 && apt-get update \
 && apt-get install openjdk-8-jdk -y || true \
 && dpkg --configure -a || true \
 && mv /var/lib/dpkg/info/openjdk-8-jre-headless:amd64.* /tmp \
 && mv /var/lib/dpkg/info/openjdk-8-jre:amd64.* /tmp \
 && mv /var/lib/dpkg/info/openjdk-8-jdk:amd64.* /tmp \
 && dpkg --configure -a || true \
 && apt-get install openjdk-8-jdk -y \
 && apt-get install scala -y \
 && wget --no-check-certificate https://dlcdn.apache.org/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz \
 && tar -xzf spark-3.3.0-bin-hadoop3.tgz \
 && mv spark-3.3.0-bin-hadoop3/ /opt/spark \
 && rm spark-3.3.0-bin-hadoop3.tgz \
 && wget --no-check-certificate https://jdbc.postgresql.org/download/postgresql-42.5.0.jar \
 && mv postgresql-42.5.0.jar /opt/spark/jars \
 && wget --no-check-certificate https://downloads.apache.org/hadoop/common/hadoop-3.2.3/hadoop-3.2.3.tar.gz \
 && tar -xzf hadoop-3.2.3.tar.gz \
 && mv hadoop-3.2.3 /usr/local/hadoop \
 && rm hadoop-3.2.3.tar.gz \
 && mkdir /usr/local/hadoop/logs \
 && chmod -R 755 /usr/lib/jvm/java-8-openjdk-amd64/bin \
 && chmod -R 755 /opt/spark

# Switch to AIRFLOW user for running the application
USER airflow

# Set environment variables
ENV JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
ENV PATH="${PATH}:$JAVA_HOME/bin"
ENV _JAVA_OPTIONS="-Xmx512M"
ENV SPARK_HOME="/opt/spark"
ENV PATH="${PATH}:$SPARK_HOME/bin:$SPARK_HOME/sbin"
ENV PYTHONPATH="$SPARK_HOME/python"
ENV PYSPARK_PYTHON="$SPARK_HOME/python"
ENV PYSPARK_DRIVER_PYTHON="$SPARK_HOME/python"
ENV HADOOP_HOME="/usr/local/hadoop"
ENV PATH="$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin"

# Copy the requirements for the Python app
COPY requirements.txt /usr/local/airflow/app/requirements.txt

# Install needed packages specified in the requirements.txt file and make special directories
RUN pip install --trusted-host pypi.python.org -r /usr/local/airflow/app/requirements.txt \
 && mkdir -p /tmp/etl_project/log \
 && mkdir -p /tmp/etl_project/data

# Copy the DAGs for the Airflow app
COPY ./airflow/dags  /usr/local/airflow/dags

# Copy the current directory contents into the container workdir
COPY . /usr/local/airflow/app

# Run airflow when the container launches
ENTRYPOINT ["/entrypoint.sh"]

# Launch webserver
CMD ["webserver"]
