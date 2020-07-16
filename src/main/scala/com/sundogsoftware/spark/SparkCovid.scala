package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark.sql._

object SparkCovid {
  
  case class Passager(ID:String, ZONA:String)

  def mapper(line:String): Passager = {
    val fields = line.split(',')

    val passager:Passager = Passager(fields(0), fields(1))
    return passager
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[1]")
      //.config("spark.sql.warehouse.dir", "file:///Users/andre/Documents/imai") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()


    val lines = spark.sparkContext.textFile("/Users/andre/Documents/od_2017_v2.csv")

    val passager = lines.map(mapper)

    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    val schemaPassager = passager.toDS

    schemaPassager.printSchema()
    schemaPassager.createOrReplaceTempView("passager")

    spark.sql("SELECT ID, count(1) FROM passager")


    spark.stop()
  }
}