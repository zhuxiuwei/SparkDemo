package graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SccExample {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("GraphXExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val vertexArray = Array(
      (1L, 1),
      (2L, 1),
      (3L, 1),
      (4L, 1),
      (5L, 1),
      (6L, 1),
      (7L, 1),
      (8L, 1),
      (9L, 1),
      (10L, 1),
      (11L, 1))
    val edgeArray = Array(
      Edge(1L, 2L, 1),
      Edge(2L, 3L, 1),
      Edge(3L, 1L, 1),
      Edge(7L, 1L, 1),
      Edge(8L, 1L, 1),
      Edge(1L, 7L, 1),
      Edge(1L, 8L, 1),

      Edge(4L, 5L, 1),
      Edge(5L, 6L, 1),
      Edge(6L, 4L, 1),

      Edge(9L, 10L, 1))

    // construct the following RDDs from the vertexArray and edgeArray variables.
    val vertexRDD: RDD[(Long, Int)] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    // build a Property Graph
    val graph: Graph[Int, Int] = Graph(vertexRDD, edgeRDD)
    println("origin: ")
    graph.triplets.foreach(x => println(x))

    //run page rank
    val pageRankRdd = graph.pageRank(0.0001)
    println("page rank: ")
    pageRankRdd.vertices.sortBy(x => x._2).foreach(x => println(x))

    val mapVerticesed = graph.mapVertices{case(k, v) => (v, k)}
    println("mapVertices ed: ")
    mapVerticesed.triplets.foreach(x => println(x))
    val grouped = mapVerticesed.vertices.mapValues(x => x)
    println("grouped : ")
    grouped.collect().foreach(x => println(x))

    val graphVertices = graph.vertices.map{case(id, attr) => (id, id)}  //make attribute phone number.
    val numberGraph = graph.outerJoinVertices(graphVertices) { (id, defaultAttr, number) =>number }
		println("-----------------!!!!!!!!!!")
    numberGraph.vertices.collect().foreach(println(_))


    //run strongly Connected Components algorithm against the graph
    val scc = graph.stronglyConnectedComponents(1000)

		val cc = scc.vertices.map{case(k,v) => (v, k)}
 		println("----------SCC Result - (group name, phone number)----------")
		cc.collect().foreach(println(_))

//		val grouped = cc.combineByKey(
//        (v: VertexId) => new SccGroup(v),
//        (ret: SccGroup, v: VertexId) => ret.merge(v),
//        (ret1: SccGroup, ret2: SccGroup) => ret1.merge(ret2),
//        new HashPartitioner(1))
//        .mapValues { x => x.array.mkString(",") }
//
//		println("----------Final Result - (group name: phone numbers)----------")
//    grouped.collect().foreach(x => println(x._1 + ": " + x._2))

    //degree info
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    val outDegress: VertexRDD[Int] = graph.outDegrees
		println("----------In degree info----------")
		inDegrees.collect().foreach(x => println(x._1 + ": " + x._2))
  	println("----------Out degree info----------")
		outDegress.collect().foreach(x => println(x._1 + ": " + x._2))
  	println("----------Total degree info----------")
    val degress = graph.inDegrees.fullOuterJoin(graph.outDegrees)
  	println("----------Degree graph----------")
 		val outDegrees: VertexRDD[Int] = graph.outDegrees
    val degreeGraph = graph.outerJoinVertices(outDegrees) { (id, oldAttr, outDegOpt) =>
      outDegOpt match {
        case Some(outDeg) => outDeg
        case None => 0 // No outDegree means zero outDegree
      }
    }
		degreeGraph.vertices.collect().foreach(println(_))
  }

}
