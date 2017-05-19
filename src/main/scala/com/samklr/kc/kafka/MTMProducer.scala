package com.samklr.kc.kafka

import java.util._

import com.samklr.avro.messages.MtmMessageValue
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

    val producer = new KafkaProducer[String, Array[Byte]](conf)

    for(i<- 1 to 100000000){
      val value = AvroConverter.toBytes(randomMtmMessageValueGenerator)
      val record = new ProducerRecord("trade-mtms", System.currentTimeMillis().toString, value)
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
  }

  def randomMtmMessageValueGenerator : MtmMessageValue = {
     MtmMessageValue.newBuilder().setDate(new Random().nextInt())
                                    .setMtms(Arrays.asList(Random.nextDouble(), Random.nextDouble()))
       .build()
  }
}



