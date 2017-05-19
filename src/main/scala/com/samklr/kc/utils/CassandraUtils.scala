package com.samklr.kc.utils

import java.sql.Timestamp

import com.datastax.driver.core.Session

object CassandraUtils extends Serializable {

  def cql(date: String,  mtms: String): String = s"""
       insert into my_keyspace.test_table (date, mtms)
       values('$date', '$mtms')"""

  def createKeySpaceAndTable(session: Session, dropTable: Boolean = false) = {
    session.execute(
      """CREATE KEYSPACE  if not exists  my_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };""")
    if (dropTable)
      session.execute("""drop table if exists my_keyspace.test_table""")

    session.execute(
      """create table if not exists my_keyspace.test_table ( date text, mtms text, primary key(date) )""")
  }
}
