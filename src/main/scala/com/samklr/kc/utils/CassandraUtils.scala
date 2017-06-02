package com.samklr.kc.utils

import java.nio.ByteBuffer
import java.sql.Timestamp

import com.datastax.driver.core.Session

object CassandraUtils extends Serializable {

  def simple(date: String, mtms: String): String =
    s"""
       insert into my_keyspace.test_table (date, mtms)
       values('$date', '$mtms')"""

  def createKeySpaceAndTable(session: Session, dropTable: Boolean = false) = {
    session.execute(
      """CREATE KEYSPACE  if not exists  mtms_ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };""")
    if (dropTable)
      session.execute("""drop table if exists mtms_ks.tests""")

    session.execute(
      """create table if not exists mtms_ks.tests ( sc text, mtms text, primary key(sc) )""")
  }

  def bytesStmt(session: Session, sc: Long, mtms : Seq[String]) = {

    mtms.foreach { mt =>
      val stmt= s""" insert into mtms_ks.tests (sc, mtms)  values( '${sc.toString}', '$mt') """
      session.execute(stmt)
    }
    //TODO care about index ??

  }
/*

  mtms.foreach { mt =>
    var bytes = Array.fill[Byte](8)(0)
    mt.foreach(ByteBuffer.wrap(bytes).putDouble(_))

    val stmt= s""" insert into my_keyspace.test_table (sc, mtms)  values( '$sc', '$bytes')
      """
    session.execute(stmt)
  }*/
}

