package com.samklr.kc.spark


import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.PairDStreamFunctions
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import java.net.InetAddress

import StreamingJob.Rec
import com.samklr.kc.avro.AvroConverter
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object DStreamJob {


  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load


    val spark =
      SparkSession.builder
        .master("local[2]")
        .appName("Flusher")
        .config("spark.cassandra.connection.host", "localhost")
        .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" ->"localhost:9001",
      "key.deserializer" -> classOf[ByteArrayDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> "mtms",
      "auto.offset.reset" -> "latest"
    )


    val topics = conf.getString("kafka.topics").split(",")

    val stream: InputDStream[ConsumerRecord[Array[Byte], Array[Byte]]] = KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](
      ssc,
      PreferConsistent,
      Subscribe[Array[Byte], Array[Byte]](Array(""), kafkaParams)
    )

    val kvs = stream.mapPartitions{ rdd =>
                            rdd.map{ data => (AvroConverter.keysToMtm(data.key()), AvroConverter.valToMtm(data.value()))}
                      }
                    .map( x => (x._1.getSc, x._2.getDate, x._2.getMtms.asScala.toArray))
                    .map{ rdd =>
                       // Create a dataset, get the latest 76 dates of the same scenario group and rewrite

                    }



    ssc.start()
    ssc.awaitTermination()




  }
}

case class Rec( sc : Long, date : Int, mtms : Array[Double])

