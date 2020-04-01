package rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ExportNumberExample {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    updateCallTypes()
  }

  //统计”call_type“出现的类型
  def summaryCallTypes(){
    val conf = new SparkConf().setAppName("GraphXExample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val raw = sc.textFile("""D:\dev\graph\exported\59days_no0Degree\callDetails59""")
      .map { line => line.split(",")(2)}
      .map(x => (x,1))
      .reduceByKey(_+_)
      .map{case(k,v) => (v,k)}
      .sortByKey(false)
      .map{case(k,v) => (v,k)}
    raw.collect().foreach { println(_) }
  }



  //清洗“主叫”字段
  def updateCallTypes(){
    val conf = new SparkConf().setAppName("GraphXExample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val raw = sc.textFile("""D:\dev\graph\exported\59days_no0Degree\callDetails59""")
      .map { line => getZiduan(line)}
      .filter(line => line != "-")
    raw.saveAsTextFile("""D:\dev\graph\exported\59days_no0Degree\callDetails59_update""")
    println(raw.count)
  }
  def getZiduan(s: String): String = {
      var res = s
      var arr = s.split(",")
      if(arr.length == 4){
        if(arr(2).contains("主叫")){
          arr(2)="主叫"
          res = arr.mkString(",")
        }
        else if(arr(2).contains("被叫")){
          arr(2)="被叫"
          res = arr.mkString(",")
        }else{
          res = "-"
        }
      }
      res
    }

  //过滤掉包含非注册用户的通话记录。如果一个电话号码没在num列（第一列）出现过，则不是注册用户。
  def extractNum(){
    val conf = new SparkConf().setAppName("GraphXExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val edgeArray = Array(
      Edge(1L, 2L, (176, "主叫")),
      Edge(1L, 2L, (32, "主叫")),
      Edge(1L, 3L, (12, "主叫")),
      Edge(2L, 3L, (78, "主叫")),
      Edge(2L, 3L, (12, "被叫")),
      Edge(2L, 5L, (52, "被叫")),
      Edge(2L, 4L, (28, "主叫")))

    val edgeRDD: RDD[Edge[(Int, String)]] = sc.parallelize(edgeArray)
		val originGraph = Graph.fromEdges(edgeRDD, 0)
		originGraph.triplets.collect().foreach(println(_))

    val outDegreeGraph = originGraph.joinVertices(originGraph.outDegrees){
      (id, defaultAttr, ouDeg) => ouDeg }
    val sub = outDegreeGraph.subgraph(vpred = (id, degree) => degree != 0)

    println("剪边后")
		sub.triplets.collect().foreach(println(_))
		val resRdd = sub.triplets.map(triplet => (triplet.srcId, triplet.dstId, triplet.attr._1, triplet.attr._2 ))
		resRdd.collect().foreach(println(_))  //应该只保留1,2的
  }
}
