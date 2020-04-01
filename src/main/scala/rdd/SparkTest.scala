package rdd

import org.apache.spark.{SparkConf, SparkContext}

object SparkTest extends App{
//  print("Start")
////  val conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]")
//  val conf = new SparkConf()
//    .setMaster("local[2]")
//    .setAppName("SparkTest")
//  val sc = new SparkContext(conf)
//  val textFile = sc.textFile("/etc/hosts")
//  println("\r\n------- " + textFile.count() + "--------------")

  val logFile = "/etc/hosts" // Should be some file on your system
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val logData = sc.textFile(logFile, 2).cache()
  val numAs = logData.filter(line => line.contains("a")).count()
  val numBs = logData.filter(line => line.contains("b")).count()
  println(s"Lines with a: $numAs, Lines with b: $numBs")
  sc.stop()
}
