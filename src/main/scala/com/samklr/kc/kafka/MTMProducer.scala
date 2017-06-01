package com.samklr.kc.kafka

import java.util._

import com.samklr.avro.messages.{MtmMessageKey, MtmMessageValue}
import com.samklr.kc.avro.AvroConverter

import scala.util.Random
import scala.util.control.NonFatal

object MTMProducer {
  import org.apache.kafka.clients.producer._

  def main(args: Array[String]): Unit = {


    val conf = new java.util.Properties()

    conf.put("bootstrap.servers", "localhost:9092")
    conf.put("acks", "all")
    conf.put("request.timeout.ms", "30000")
    conf.put("retries", "3")
    conf.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    conf.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")//"io.confluent.kafka.serializers.KafkaAvroSerializer")//
    conf.put("schema.registry.url", "http://localhost:8081")

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](conf)

    for(i<- 1 to 10000){
      val message = randomMessage

      val key = AvroConverter.keysToBytes(message._1)

      message._2.foreach( v =>
      val record = new ProducerRecord("trade-mtms", key, AvroConverter.valToBytes(v))

      try {
        producer.send(record, new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            if (exception != null) {
              exception.printStackTrace()
            }
            println ("Pushed ==> " + record)
          }
        })
      }
      catch {
        case NonFatal(e) => println("Error !!!!!!!! "+ e)
      }
    }
    producer.close

      )


  }

  def randomMessage : (MtmMessageKey, IndexedSeq[MtmMessageValue])  = (
    val key = MtmMessageKey.newBuilder()
      .setJobId(UUID.randomUUID().toString)
      .setSc(new Random().nextLong())
      .build()

    val values = (0 to 76) .map(

      MtmMessageValue.newBuilder()
      .setDate(_)
      .setMtms(Arrays.asList(Random.nextDouble(), Random.nextDouble()))
      .build()
    )

    (key, values)
  )


}



