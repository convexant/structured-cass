
1. Inside `setup`  run `docker-compose up -d` to launch   `zk`, `kafka` and `cassandra`

2. `docker ps` 

3.  `pip install -r requirements.txt`

4. `main.py` generates some random data and publishes it to a topic in kafka.

5. Run the spark-app using `sbt clean compile run` in a console. This app will listen on topic (check Main.scala) and writes it to Cassandra.

6. Again run `main.py` to write some test data on a kafka topic.

7. Finally check if the data has been published in cassandra.
  * Go to cqlsh `docker exec -it cas_01_test cqlsh localhost`
  * And then run `select * from my_keyspace.test_table  ;`


1. setup/ docker co