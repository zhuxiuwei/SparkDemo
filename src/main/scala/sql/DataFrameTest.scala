package sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameTest {

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.WARN)
    test()
  }

  def test(): Unit = {
    val sf = new SparkConf().setAppName("DFTest").setMaster("local[*]")
    val sc = new SparkContext(sf);
    val sqlCtx = new SQLContext(sc)

    val data = List(("Lisa", 7L, 3), ("Lisa", 7L, 2), ("Tony", 8L, 3))
//    case class Student(name: String, age: Long, grage: Int)
    val df = sqlCtx.createDataFrame(data)
    df.show()
//    +----+---+---+
//    |  _1| _2| _3|
//    +----+---+---+
//    |Lisa|  7|  3|
//    |Lisa|  7|  2|
//    |Tony|  8|  3|
//    +----+---+---+
    val df2 = df.select("_1", "_2").dropDuplicates()
    df2.show()
//    +----+---+
//    |  _1| _2|
//    +----+---+
//    |Lisa|  7|
//    |Tony|  8|
//    +----+---+
  }
}
