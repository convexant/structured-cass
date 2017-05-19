name := "structured-stream-kafka-to-cassandra"

version := "1.0"

scalaVersion := "2.11.8"


val repositories = Seq(
  "confluent" at "http://packages.confluent.io/maven/",
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)

resolvers ++= repositories

libraryDependencies ++= {
  val sparkV = "2.1.1"
  val cassandraV = "2.0.0-M3"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkV,
    "org.apache.spark" %% "spark-streaming" % sparkV,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkV,
    "org.apache.spark" %% "spark-sql" % sparkV,
    "org.apache.spark" %% "spark-hive" % sparkV,
    "com.datastax.spark" %% "spark-cassandra-connector" % cassandraV,
    "org.scalatest" %% "scalatest" % "3.0.0" % "test",
    "com.holdenkarau" %% "spark-testing-base" % "2.0.0_0.4.4",
    "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.2",
    "com.google.guava" % "guava" % "19.0",
    "org.apache.commons" % "commons-lang3" % "3.5",
    "org.apache.avro" % "avro" % "1.8.1",
    "io.confluent" % "kafka-avro-serializer" % "3.2.1",
     "com.twitter" % "bijection-core_2.11" % "0.9.5",
     "com.twitter" % "bijection-avro_2.11" % "0.9.5"

  )
}

parallelExecution in Test := false

javaOptions ++= Seq("-Xms512M",
                    "-Xmx4096M",
                    "-XX:MaxPermSize=2048M",
                    "-XX:+CMSClassUnloadingEnabled")

mainClass in assembly := Some("com.kafkaToSparkToCass.Main")

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") =>
    MergeStrategy.filterDistinctLines
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}
