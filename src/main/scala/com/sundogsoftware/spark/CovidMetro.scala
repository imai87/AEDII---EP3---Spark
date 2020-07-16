package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark._

import scala.collection.JavaConversions.asJavaCollection

/** Count up how many of each word appears in a book as simply as possible. */
object CovidMetro {


  def mapLine(line:String): (String, String) ={
    val passager = line.split(",")
    (passager(0),passager(1))
  }

  def mapByZone(line:(String,String)):(String, String)={
    (line._2, line._1)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordCount")   
    
    // Read each line of my book into an RDD
    val input = sc.textFile("/Users/andre/Documents/od_2017_v2.csv")
    
    // Split into words separated by a space character
    val passagers = input.map(mapLine)

    val passagersGroupById = passagers.groupByKey(1)
    val passagersGroupByZone = passagers
      .map(mapByZone).filter(p=> p._1 != 0)
      .groupByKey(1)
      .map(p=> (p._1,p._2.size(),p._2)).sortBy(p=> (p._2,true,1))

    passagersGroupByZone.saveAsTextFile("/Users/andre/Documents/Result-Imai/")



  }
  
}

