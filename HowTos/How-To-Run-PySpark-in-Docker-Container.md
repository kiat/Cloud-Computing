# Running PySpark in Docker Container

This tutorial walks you through setting up a self-contained Apache Spark environment using Docker, perfect for local PySpark development, including best practices using a Dockerfile.

## Prerequisites

1.  **Docker Desktop** (Engine and Compose).
2.  **Basic understanding** of Docker and PySpark.

---

## Step 1: Set Up Project Structure

Create your main project directory and a sub-directory for your application code.

```bash
mkdir spark-pyspark-docker
cd spark-pyspark-docker
mkdir app
```
On MS Windows OS, you can create your folders similarly. 

## Step 2: Create Your PySpark Application
Place your PySpark script inside the app directory.

app/simple_job.py

```Python
from pyspark.sql import SparkSession

# Initialize Spark Session in local mode
spark = SparkSession.builder \
    .appName("SimplePySparkApp") \
    .master("local[*]") \
    .getOrCreate()

print("--- SparkSession successfully created ---")

# Create a simple DataFrame
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3), ("David", 4)]
columns = ["Name", "ID"]
df = spark.createDataFrame(data, columns)

# Perform a simple transformation
df_transformed = df.withColumn("NewID", df["ID"] * 10)

print("--- Transformed DataFrame (ID * 10) ---")
df_transformed.show()

# Stop the SparkSession
spark.stop()
```


## Method 1: Quick Run (Using Volume Mount)
This method is quick and uses the base image directly, mounting your code from the host machine.

## Execution
Run the job by mounting your local ./app folder into the container's /opt/spark-apps directory.

```bash
docker run \
    --rm \
    -v "$(pwd)/app:/opt/spark-apps" \
    apache/spark:latest-python \
    /opt/spark/bin/spark-submit \
    /opt/spark-apps/simple_job.py
```

## Method 2: Best Practice (Using a Dockerfile)

This method creates a custom image that bundles your application and dependencies, making it more portable and reproducible.

* **1. Create the Dockerfile**
In your project root (spark-pyspark-docker), create a file named Dockerfile.

```bash
# Start from the official Apache Spark base image with Python support
FROM apache/spark:latest-python

# Set the working directory inside the container
WORKDIR /opt/spark-apps

# Copy the PySpark application code into the container
# This copies ./app (local) to /opt/spark-apps (container)
COPY ./app /opt/spark-apps

# If you need Python dependencies (e.g., pandas), you would add:
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# Specify the default command to run the PySpark job
CMD ["/opt/spark/bin/spark-submit", "/opt/spark-apps/simple_job.py"]
```

* **2. Build the Docker Image**
Run the build command from your project root:


```Bash
docker build -t pyspark-app .
```

* **3. Run the Custom Image**
Execute your application using the new custom image:

```Bash
docker run --rm pyspark-app
```

## Advanced Setup: Standalone Cluster (Docker Compose)

For running a Master/Worker cluster, use docker-compose.

docker-compose.yml
```YAML

version: "3.7"
services:
  spark-master:
    image: apache/spark:latest-python
    container_name: spark-master
    ports:
      - "8080:8080" # Spark Master Web UI
      - "7077:7077" # Master communication port
    environment:
      - SPARK_WORKLOAD=master
      - SPARK_LOCAL_IP=spark-master
    volumes:
      - ./app:/opt/spark-apps # Mount app for job submission

  spark-worker:
    image: apache/spark:latest-python
    container_name: spark-worker
    depends_on:
      - spark-master
    ports:
      - "8081:8081" # Worker Web UI
    environment:
      - SPARK_WORKLOAD=worker
      - SPARK_MASTER=spark://spark-master:7077
      - SPARK_WORKER_CORES=1
      - SPARK_WORKER_MEMORY=1g
```


## Run the Cluster

Start the services: docker-compose up -d

Submit the job to the Master container:

Bash
```
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/simple_job.py
```