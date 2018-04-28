//package main.scala

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object Utils {

  def main(args: Array[String]){
    val query = "SELECT * from table where col2 > (SELECT avg(col2) from table) AND col1 > (SELECT avg(col1) from table)"
    var out = sqlparse(query)
    println(out)

  }

  def queryCompiler(spark : SparkSession, innerQuery: ListBuffer[String], curPartition: DataFrame){
    var queryOutput = new ListBuffer[Int]
    for (i <- 0 to (innerQuery.length-1) ) {
      var out = spark.sql(innerQuery(i))
      out.collectAsList()
      print(out)
//      queryOutput += out
    }
//    return queryOutput
  }

  def sqlparse(query:String) : ListBuffer[String] = {
    var qlist = query.split(" ")
    var innerList = new ListBuffer[String]()
    var innerQuery = ""
    var start = false
    for (i <- 0 to (qlist.length-1)){
      if (qlist(i) == "(SELECT" ){
        start = true
      }
      if (start) {
        innerQuery += qlist(i) + ' '
      }
      if (qlist(i) contains ")") {
        if (!(qlist(i) contains "(")) {
          start = false
          innerList += innerQuery
          innerQuery = ""
        }
      }
    }
    return innerList
  }
}
