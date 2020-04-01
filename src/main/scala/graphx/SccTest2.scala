package graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SccTest2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Test")
      .setMaster("local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val v: RDD[(VertexId, Int)] = sc.parallelize(Seq((0L,0),(1L,1),(2L,2)))
    val e: RDD[Edge[Int]] = sc.parallelize(Seq(Edge(0, 1, 0), Edge(0, 2, 0), Edge(1, 0, 0), Edge(2, 1, 0)))
    val g = Graph(v, e)

    def test(graph: Graph[Int, Int]) = {
      graph.cache()
      val ng = graph.outerJoinVertices(graph.outDegrees){
        (vid, vd, out) => (vd, out.getOrElse(vid, 0))
      }
//      ng.triplets.foreach(println(_))

      val f = ng.subgraph(epred = _.srcId != 0, vpred = (vid, vd) => vid != 0L)
      f.cache()
//      f.triplets.foreach(println(_))
      graph.unpersistVertices(blocking = false)
      f
    }

    val f1 = test(g)

    println(f1.numVertices)

  }
}
