# Local Spark Standalone Development Environment with Docker

**Description:**

This project provides a convenient way to set up a local Spark Standalone cluster using Docker containers. It's ideal for development, testing, and small-scale experiments with Spark.

**Key Features:**

- Pre-configured Docker images for Spark master and workers
- Customizable resource allocation (cores and memory) for nodes
- Clear separation of data and code directories
- Easy access to Spark UI for monitoring

**Prerequisites:**

- Docker ([https://www.docker.com/](https://www.docker.com/))
- Docker Compose ([https://docs.docker.com/compose/](https://docs.docker.com/compose/))

**Installation:**

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/baptvit/mineracao-dados-massivos
   ```

2. **Build Docker Images:**

   The project includes a Dockerfile to build an Apache Spark 3.4 image. You can customize the Spark version by modifying the Dockerfile's `VERSION` tag.

   ```bash
   cd mineracao-dados-massivos
   docker build --progress=plain -t apache-spark:3.4.0 .
   ```

3. **Start the Local Cluster:**

   Use Docker Compose to bring up a master node with 2 cores and 2 GB of memory, along with 3 worker nodes with 2 cores and 4 GB each.

   ```bash
   docker-compose up
   ```

**Configuration:**

- **Resource Allocation:** You can easily adjust the resources allocated to the master and worker nodes by modifying the `resources` section in the `docker-compose.yml` file. For example, to increase the master's memory to 4 GB, change:

   ```yaml
   master:
     resources:
       reservations:
         memory: 4g
   ```

- **Data and Code:** The project assumes your data resides in the `./data` directory and your Spark application code is located in the `./apps` directory. You can modify these paths accordingly.

**Usage:**

1. **Access Spark UI:** Open a web browser and navigate to `https://localhost:8888` to monitor your Spark cluster's activity.

2. **Run Spark Examples:** The project includes example Spark applications in Java and Python. You can execute them using `spark-submit`:

   - **Java Spark Pi Example:**

     ```bash
     ./spark-submit \
       --master spark://0.0.0.0:7077 \
       --name spark-pi \
       --class org.apache.spark.examples.SparkPi \
       local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar \
       100
     ```

   - **Python PySpark Pi Example:**

     ```bash
     /opt/spark/bin/spark-submit \
       --master spark://0.0.0.0:7077 \
       --name pyspark-pi \
       local:////opt/spark/examples/src/main/python/pi.py \
       100
     ```

**Future Enhancements:**

- **Customizing Spark Image:** The Dockerfile can be extended to include additional Spark dependencies, such as Iceberg, Delta Lake, or Hudi formats.

**Contributing:**

We encourage contributions to this project. Feel free to submit pull requests to improve or add features.

**License:**

This project is licensed under the [MIT License](https://opensource.org/licenses/MIT).


Using HUDI
# For Spark versions: 3.2 - 3.4
```bash
export PYSPARK_PYTHON=$(which python3)
export SPARK_VERSION=3.4
pyspark 
--packages org.apache.hudi:hudi-spark$SPARK_VERSION-bundle_2.12:0.14.1 \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' \
--conf 'spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar'
```

``` bash
poetry export -f requirements.txt --without-hashes -o requirements.txt
poetry run pip install . -r requirements.txt -t package_tmp
cd package_tmp
find . -name "*.pyc" -delete
zip -r ../package .
```