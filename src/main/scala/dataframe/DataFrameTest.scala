package dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameTest {

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val sf = new SparkConf().setAppName("DFTest").setMaster("local[*]")
    val sc = new SparkContext(sf);
//    val sqlCtx = new SQLContext(sc) //过时了
    val session = SparkSession
      .builder()
//      .enableHiveSupport()
      .getOrCreate()

    /* tests */
//    schemaTest(session)
    simpleDfAPITest(session)
  }

  /**
   * schema相关测试
   */
  def schemaTest(session: SparkSession): Unit = {

    val data = List(("Lisa", 7L, 3), ("Lisa", 7L, 2), ("Tony", 8L, 3))
    val df = session.createDataFrame(data)
    df.show()
//    +----+---+---+
//    |  _1| _2| _3|
//    +----+---+---+
//    |Lisa|  7|  3|
//    |Lisa|  7|  2|
//    |Tony|  8|  3|
//    +----+---+---+

    df.printSchema()
//        root
//    |-- _1: string (nullable = true)
//    |-- _2: long (nullable = false)
//    |-- _3: integer (nullable = false)
    println(df.schema)
//    StructType(StructField(_1,StringType,true), StructField(_2,LongType,false), StructField(_3,IntegerType,false))
    val df2 = df.select("_1", "_2").dropDuplicates()
    df2.show()
//    +----+---+
//    |  _1| _2|
//    +----+---+
//    |Lisa|  7|
//    |Tony|  8|
//    +----+---+

    val student = Student("Jack", 26, 8, Array(100,88,99))
    val student2 = Student("Tom", 24, 6, Array(72,85,66))
    val df3 = session.createDataFrame(Seq(student, student2))
    df3.show()
//    +----+---+-----+-------------+
//    |name|age|grade|       scores|
//    +----+---+-----+-------------+
//    |Jack| 26|    8|[100, 88, 99]|
//    | Tom| 24|    6| [72, 85, 66]|
//    +----+---+-----+-------------+
    df3.printSchema()
//    root
//    |-- name: string (nullable = true)
//    |-- age: long (nullable = false)
//    |-- grade: integer (nullable = false)
//    |-- scores: array (nullable = true)
//    |    |-- element: integer (containsNull = false)
    println(df3.schema)
//    StructType(StructField(name,StringType,true), StructField(age,LongType,false), StructField(grade,IntegerType,false), StructField(scores,ArrayType(IntegerType,false),true)

  }

  /**
   * high performance spark: Simple DataFrame transformations and SQL expressions （p7）
   */
  def simpleDfAPITest(session: SparkSession): Unit ={
    val student = Student("Jack", 26, 8, Array(100,88,99))
    val student2 = Student("Tom", 24, 6, Array(72,85,66))
    val df = session.createDataFrame(Seq(student, student2))

    /** filter usage */
    val filteredDf = df.filter((df("scores")(0) > 80) && df("age") > 24)  //组合条件。注意score部分，用apply来读取一个复杂类型(array/map等)里的数据。
//    filteredDf = df.filter((df("scores")(0) > 80).and(df("age") > 24))  //组合条件，效果同上
    filteredDf.show()
//    +----+---+-----+-------------+
//    |name|age|grade|       scores|
//    +----+---+-----+-------------+
//    |Jack| 26|    8|[100, 88, 99]|
//    +----+---+-----+-------------+

    /** select usage */
    import org.apache.spark.sql.functions._   //要加上，否则'lit'编译不过
    val df2 = df.select(df("name"), (df("age") + df("grade")).as("intSum"))
      .withColumn("c2",  lit(1));
    df2.show()
//    +----+------+---+
//    |name|intSum| c2|
//    +----+------+---+
//    |Jack|    34|  1|
//    | Tom|    30|  1|
//    +----+------+---+

    /** explode() test */
    val newCol = explode(lit(Array("a","b")))
    val df3 = df2.withColumn("newColumn", newCol)
    df3.show()
//    +----+------+---+---------+
//    |name|intSum| c2|newColumn|
//    +----+------+---+---------+
//    |Jack|    34|  1|        a|
//    |Jack|    34|  1|        b|
//    | Tom|    30|  1|        a|
//    | Tom|    30|  1|        b|
//    +----+------+---+---------+

    /** if else(when) test*/
    val df4 = df3
      .select(df3("name"), (when((df3("name") === "Jack"), "Good Student").when((df3("name") === "Tom"), "Bad Student").otherwise("Normal Student").as("StudentType")))
    df4.show()
  }

  /**
   * 这个class必须定义在DataFrameTest外，否则会报错："No TypeTag available for Student"
   * case不能省。 否则val student = Student("Jack", 26, 8, Array(100,88,99)) 编译不过。
   */
  case class Student(name: String, age: Long, grade: Int, scores: Array[Int])
}
