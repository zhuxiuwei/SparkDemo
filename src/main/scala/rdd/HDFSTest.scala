package rdd

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HDFSTest{
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("WriteHDFS")
      .setMaster("local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

//    writeHDFS(sc, """/home/tangwei/xiuwei/Test_""" + System.currentTimeMillis())
    readHDFS(sc, """/home/tangwei/xiuwei/2017*/*""")
  }

  //test write
  def writeHDFS(sc: SparkContext, filePath:  String) = {
    val rdd = sc.parallelize(Array(("ALice", 30), ("Tony", 18), ("Jack", 43)))
    rdd.saveAsNewAPIHadoopFile(filePath, classOf[Text],classOf[IntWritable], classOf[TextOutputFormat[Text,IntWritable]])
  }

  //test read
  def readHDFS(sc: SparkContext, filePath: String) = {
    val rdd = sc.textFile(filePath)
    println("rdd: ")
    rdd.collect().foreach { println(_) }

    val distinctRdd = rdd.distinct()
    println("distinctRdd: ")
    distinctRdd.collect().foreach { println(_) }

    val kvRdd: RDD[(String, String)] = rdd.map { x => {
        var s = x.split("\t")
           (s(0), s(1))
      }
    }
    println("kvRdd: ")
    kvRdd.collect().foreach { println(_) }

    val distinctedKvRdd = kvRdd.distinct()
    println("distinctedKvRdd: ")
    distinctedKvRdd.collect().foreach { println(_) }
  }
}
