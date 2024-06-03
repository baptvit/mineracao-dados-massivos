For build the spark image
-- here um can add the dependencys i.e the Iceberg, delta lake or HUDI formts

docker build --progress=plain -t apache-spark:3.4.0 .

For loading all the local cluster compose of a master and 3 workers
docker-compose up

Check spark ui
https://localhost:4040


master node are prefixed with docker-spark-master
docker exec -i -t <docker-spark-master conteiner ID> /bin/bash


go in the /bin inside the spark-master as entrypoint for running the jobs

./spark-submit --master spark://0.0.0.0:7077 --name spark-pi --class org.apache.spark.examples.SparkPi  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100


/opt/spark/bin/spark-submit --master spark://0.0.0.0:7077 --name pyspark-pi local:////opt/spark/examples/src/main/python/pi.py 100



sudo apt-get install python3.10-pip python3.10-numpy python3.10-matplotlib python3.10-scipy python3.10-pandas python3.10-simpy



update-alternatives --install "/usr/bin/python" "python" "/usr/local/lib/python3.10/" 1