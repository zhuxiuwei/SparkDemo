package sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HiveConnector {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("HiveConnector").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    //hiveContext.setConf("hive.metastore.uris", "thrift://spa1111rk0:9083")
    val df = hiveContext.sql("select * from orkland_data")
    df.show()
//    df.write.format("csv").save("test.csv")
//    sc.stop()
  }
}
