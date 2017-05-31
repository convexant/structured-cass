
import com.datastax.spark.connector.cql.CassandraConnector
import com.samklr.kc.avro.AvroConverter
import com.samklr.avro.messages.MtmMessageValue
import com.samklr.kc.utils.CassandraUtils
import org.apache.spark.sql.SparkSession


object MtmEncoders {
  implicit def mtmEncoder: org.apache.spark.sql.Encoder[MtmMessageValue] =
    org.apache.spark.sql.Encoders.javaSerialization(classOf[MtmMessageValue])  //    kryo[MtmMessageValue]
}

object StreamingJob {

  def main(args: Array[String]) {

    // Get Spark session
    val session =
    SparkSession.builder
      .master("local[2]")
      .appName("Pipeline")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()


    val connector = CassandraConnector.apply(session.sparkContext.getConf)
    // Create keyspace and tables here, NOT in prod
    connector.withSessionDo { session =>
      CassandraUtils.createKeySpaceAndTable(session, true)
    }

    import session.implicits._
    import MtmEncoders._

    val lines = session.readStream
      .format("kafka")
      .option("subscribe", "trade-mtms")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "latest")
      .load()
      .select($"value".as[Array[Byte]])
      .map(AvroConverter.toMtm(_))
      .as[MtmMessageValue]

    //  Foreach sink writer to push the output to cassandra.
    import org.apache.spark.sql.ForeachWriter
    val writer = new ForeachWriter[MtmMessageValue] {
      override def open(partitionId: Long, version: Long) = true

      override def process(value: MtmMessageValue) = {
        connector.withSessionDo { session =>
          session.execute(CassandraUtils.cql(value.getDate.toString, value.getMtms.toString))
        }
        println("Processed And pushed====== >>  " + value)
      }

      override def close(errorOrNull: Throwable) = ???
    }

    val query =
      lines.writeStream
        .queryName("printer")
        .foreach(writer)
        .start

    query.awaitTermination
    session.close
  }
}

/* val query = lines.writeStream
   .outputMode("Append")
   .format("console")
   .start()
 query.awaitTermination()
*/