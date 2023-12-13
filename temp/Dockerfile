FROM gcr.io/datamechanics/spark:platform-3.1-dm14


ENV PYSPARK_MAJOR_PYTHON_VERSION=3

# Set the working directory in the container
WORKDIR /app


RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.5.jar
RUN mv postgresql-42.2.5.jar /opt/spark/jars

ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH
ENV PATH=$SPARK_HOME/python:$PATH


# Copy the requirements file into the container
COPY requirements.txt .


# Install the necessary dependencies
RUN pip install --no-cache-dir -r requirements.txt


# Default command that is triggered when container starts
CMD ["tail", "-f", "/dev/null"]
