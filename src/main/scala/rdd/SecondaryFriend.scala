package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 二度好友查找问题。
 */
object SecondaryFriend {

  def firstFriendsRdd(sc: SparkContext) : RDD[(Long, Set[Long])] = {
    //一度好友
    val firstFriends = sc.parallelize(Seq(
        (1L, "2 3 4"),
        (2L, "1 3 4 5"),
        (3L, "1 2 4"),
        (4L, "1 2 3"),
        (5L, "2 6"),
        (6L, "5 7 8"),
        (7L, "6"),
        (8L, "6")
    )).mapValues { x =>
      val arr = x.split(" ")
      val set = scala.collection.mutable.Set.empty[Long]
      arr.foreach { f => set += f.toLong}
      set.toSet
    }
    firstFriends
  }

  //方法1： 用M/R
  def way1(){
    val conf = new SparkConf().setAppName("SecondaryFriend").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val firstRdd: RDD[(Long, Set[Long])] = firstFriendsRdd(sc)

    val firstFlatten = firstRdd.flatMapValues { x => x }

    val secondToomanyRdd = firstFlatten.join(firstRdd).map(x => x._2)
    val secondToomanyRddCombinedAndRemoveSelf = secondToomanyRdd.reduceByKey{case(set1,set2) => set1 ++ set2}
      .map{case(id, set) => (id, set - id)}  //删除掉自己。

    val res = secondToomanyRddCombinedAndRemoveSelf.join(firstRdd).mapValues(twoSet => twoSet._1 -- twoSet._2)

    res.collect().foreach(println(_))
  }

  //方法2： 用GraphX
  def way2(){

  }

  def main(args: Array[String]) {
     way1()
  }
}
