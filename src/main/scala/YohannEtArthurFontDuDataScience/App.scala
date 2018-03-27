package YohannEtArthurFontDuDataScience

import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD

/**
  * Hello world!
  *
  */
object App extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  private val sSession: SparkSession = SparkSession.builder().appName("YohannEtArthurFontDuDataScience")
                                       .master("local[3]").getOrCreate()

  private val sqlContext: SQLContext = sSession.sqlContext
  private val begin: Long = System.currentTimeMillis()
  private val df: DataFrame = sqlContext.read.format("csv").option("header", true).load("data/properties_2016.csv")
  println(System.currentTimeMillis() -begin)
  import sqlContext.implicits._

//  df.describe().foreach(row => println(row.fieldIndex("count")))

//  private val rdd: RDD[Row] = df.describe().filter($"summary" ==="count").rdd

//  df.columns.foreach(str => col(str).)
}

