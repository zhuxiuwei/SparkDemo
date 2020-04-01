package rdd

import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCountTest extends App{
  print("Start SparkWordCountTest")
  val conf = new SparkConf().setAppName("SparkWordCountTest")
  val sc = new SparkContext(conf)
  // read source file from HDFS
  val textFile = sc.textFile("""/public/wordcount_source.txt""")
  // calculate word count
  val wordCountRdd = textFile.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
  // output word count result to HDFS
  wordCountRdd.saveAsTextFile("""/public/wordcount_test/""" + System.currentTimeMillis()) //save to hdfs
}
