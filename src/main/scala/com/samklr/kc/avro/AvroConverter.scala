package com.samklr.kc.avro


import com.samklr.avro.messages.{MtmMessageKey, MtmMessageValue}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs

object AvroConverter {
  implicit private val specificKeyAvroBinaryInjection: Injection[MtmMessageKey, Array[Byte]] =
    SpecificAvroCodecs.toBinary[MtmMessageKey]

  implicit private val specificValueAvroBinaryInjection: Injection[MtmMessageValue, Array[Byte]] =
    SpecificAvroCodecs.toBinary[MtmMessageValue]

  def valToBytes(mtm: MtmMessageValue): Array[Byte] = specificValueAvroBinaryInjection(mtm)

  def valToMtm ( bytes : Array[Byte]) : MtmMessageValue = {
    val attempt = Injection.invert[MtmMessageValue, Array[Byte]](bytes)
    attempt.get
  }

  def keysToBytes(mtm: MtmMessageKey): Array[Byte] = specificKeyAvroBinaryInjection(mtm)

  def keysToMtm ( bytes : Array[Byte]) : MtmMessageKey = {
    val attempt = Injection.invert[MtmMessageKey, Array[Byte]](bytes)
    attempt.get
  }
}
