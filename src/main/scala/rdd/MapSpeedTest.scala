package rdd

import java.util.{Date, Random}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MapSpeedTest {
   def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    val arraySize = 10000000
    println(new Date() + " !!!!!!!!!!!!! Create array of size " + arraySize)
    val ran: Random = new Random
    val arr: Array[(Int, Int)] = new Array[(Int, Int)](arraySize)
    for(i <- 0 to arraySize - 1) {
      arr(i) = (i, ran.nextInt())
    }
    println(new Date() + " !!!!!!!!!!!!! Create array done " + arraySize)

    val conf = new SparkConf().setAppName("MapSpeedTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(arr)
    rdd.take(100)
    println(new Date() + " !!!!!!!!!!!!! Create origin rdd done " + arraySize)

    val rddMapped = rdd.map{case(k,v) => (v, k)}
    println(new Date() + " !!!!!!!!!!!!! rdd map done " + arraySize)
    rddMapped.take(100)
    println(new Date() + " !!!!!!!!!!!!! rdd map take done " + arraySize)


    val grouped = rddMapped.groupByKey().filter{ case (k, v) => v.toSeq.size >= 1}
    rddMapped.take(2).foreach(println(_))
    println(new Date() + " !!!!!!!!!!!!! group done " + arraySize)
  }
}
