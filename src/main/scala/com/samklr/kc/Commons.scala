package com.samklr.kc

import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}

import com.samklr.kc.kafka.MTMProducer
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader

object Commons {
  def getTimeStamp(timeStr: String): Timestamp = {
    val dateFormat1: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateFormat2: DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

    val date: Option[Timestamp] = {
      try {
        Some(new Timestamp(dateFormat1.parse(timeStr).getTime))
      } catch {
        case e: java.text.ParseException =>
          Some(new Timestamp(dateFormat2.parse(timeStr).getTime))
      }
    }
    date.getOrElse(Timestamp.valueOf(timeStr))
  }

  def main(args: Array[String]): Unit = {
    val m = MTMProducer.randomMessage

    m._2.foreach { v =>
      println (m._1, v)
    }
  }
}
