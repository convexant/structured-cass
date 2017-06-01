
import com.samklr.avro.messages.{MtmMessageKey, MtmMessageValue}
import com.samklr.kc.avro.AvroConverter
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._


object MtmEncoders {
  implicit def mtmValuesEncoder: org.apache.spark.sql.Encoder[MtmMessageValue] =
    org.apache.spark.sql.Encoders.javaSerialization(classOf[MtmMessageValue])  //    kryo[MtmMessageValue]

  implicit def mtmKeysEncoder: org.apache.spark.sql.Encoder[MtmMessageKey] =
    org.apache.spark.sql.Encoders.javaSerialization(classOf[MtmMessageKey])
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


    /* val connector = CassandraConnector.apply(session.sparkContext.getConf)
     // Create keyspace and tables here, NOT in prod
     connector.withSessionDo { session =>
       CassandraUtils.createKeySpaceAndTable(session, true)
     }*/

    import session.implicits._
    val cols = List("sc", "date", "mtms")

    val lines = session.readStream
      .format("kafka")
      .option("subscribe", "trade-mtms")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(key AS BINARY)","CAST (value as BINARY)")
      .as[(Array[Byte], Array[Byte])]

    lines.printSchema()

     val df = lines.map{ line =>
        val kv = (AvroConverter.keysToMtm(line._1), AvroConverter.valToMtm(line._2))
        (kv._1.getSc, kv._2.getDate, kv._2.getMtms.asScala.toArray)
      }.toDF(cols: _*)

    df.printSchema()

    val ds = df.select($"sc", $"date", $"mtms")
               .as[Rec]
               .groupByKey(rec => rec.sc)
               .flatMapGroups{
                 // Check if the Iterator contains the 76 dates. if so return the said dates
                 case ( sc, iter) =>  iter.map(_.mtms)

               }

    ds.printSchema()



    //  Foreach sink writer to push the output to cassandra.
    import org.apache.spark.sql.ForeachWriter
    val writer = new ForeachWriter[Array[Double]] {
      override def open(partitionId: Long, version: Long) = true

      override def process(kv : Array[Double]) = {
        println (kv.mkString(" "))
      }

      override def close(errorOrNull: Throwable) = {
        println ("Error : " + errorOrNull )
      }
    }


    val query = ds.writeStream
      .queryName("Kafka Outputer")
      .foreach(writer)
      .start()

    query.awaitTermination
    session.close
  }

  case class Rec( sc : Long, date : Int, mtms : Array[Double])

}
