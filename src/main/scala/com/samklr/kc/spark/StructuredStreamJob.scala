
import java.util

import com.datastax.spark.connector.cql.CassandraConnector
import com.samklr.avro.messages.{MtmMessageKey, MtmMessageValue}
import com.samklr.kc.avro.AvroConverter
import com.samklr.kc.utils.CassandraUtils
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

import scala.collection.JavaConverters._


object MtmEncoders {
  implicit def mtmValuesEncoder: org.apache.spark.sql.Encoder[MtmMessageValue] =
    org.apache.spark.sql.Encoders.javaSerialization(classOf[MtmMessageValue])  //    kryo[MtmMessageValue]

  implicit def mtmKeysEncoder: org.apache.spark.sql.Encoder[MtmMessageKey] =
    org.apache.spark.sql.Encoders.javaSerialization(classOf[MtmMessageKey])
}

object StreamingJob {

  case class Rec(sc: Long, date: Int, mtms: Array[Double])

  def main(args: Array[String]) {

    val config = ConfigFactory.load

    // Get Spark session
    val session =
      SparkSession.builder
        .master("local[4]")
        .appName("Kafka Aggreg")
        .config("spark.cassandra.connection.host", "172.29.2.124")
        .getOrCreate()


    val connector = CassandraConnector.apply(session.sparkContext.getConf)
    // Create keyspace and tables here, NOT in prod
    connector.withSessionDo { session =>
      CassandraUtils.createKeySpaceAndTable(session, true)
    }

    import session.implicits._

    //Open a coonection to Kafka
    val lines = session.readStream
      .format("kafka")
      .option("subscribe", "trade-mtms")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST (key AS BINARY)",
        "CAST (value as BINARY)")
      .as[(Array[Byte], Array[Byte])]


    val df = lines.map{
      line =>
          val kv = (AvroConverter.keysToMtm(line._1), AvroConverter.valToMtm(line._2))
          (kv._1.getSc, kv._2.getDate, kv._2.getMtms.asScala.toArray)
    }.toDF(List("sc", "date", "mtms"): _*)

    val ds = df.select($"sc", $"date", $"mtms")
      .as[Rec]
      .groupByKey(rec => rec.sc)
      .mapGroups {
        case (sc, recs) => (sc, recs.map(_.mtms.mkString(" ")).toList)
      }.toDF("sc", "mtms")


    val writer = fWriter(connector)

    val query = ds.writeStream.queryName("Kafka Outputer").foreach(writer).start()

    query.awaitTermination

    session.close
  }


  //  Foreach sink writer to push the output to cassandra.

  def fWriter(connector : CassandraConnector) = new ForeachWriter[Row] {
    override def open(partitionId: Long, version: Long) = true

    override def process(row: Row) = {
      /*
        Needs some logic to bufferize enough keys, and then write them as bulks of 76 records
       */
      val sc = row.getLong(0)
      val mtmt = row.getAs[Seq[String]]("mtms")

      //mtmt.foreach(println)
      connector.withSessionDo { session =>
        CassandraUtils.bytesStmt(session, sc, mtmt )
      }
      println(" => Wrote  to Cassandra " + mtmt.length +" mtms")
    }

    override def close(errorOrNull: Throwable) = { // TODO Extra Logic to close this
      if (errorOrNull != null)
        println("Closed Foreach Writer with Error " + errorOrNull.getMessage)
      else
        println("Closing Foreach Writer ")
    }

  }
}
