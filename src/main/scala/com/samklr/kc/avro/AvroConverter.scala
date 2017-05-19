package com.samklr.kc.avro


import com.samklr.avro.messages.MtmMessageValue
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs

object AvroConverter {
  implicit private val specificEventAvroBinaryInjection: Injection[MtmMessageValue, Array[Byte]] = SpecificAvroCodecs.toBinary[MtmMessageValue]

  def toBytes(mtm: MtmMessageValue): Array[Byte] = specificEventAvroBinaryInjection(mtm)

  def toMtm ( bytes : Array[Byte]) : MtmMessageValue = {
    val attempt = Injection.invert[MtmMessageValue, Array[Byte]](bytes)
    attempt.get
  }
}