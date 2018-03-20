package YohannEtArthurFontDuDataScience

import java.util

import YohannEtArthurFontDuDataScience.App.{df, sSession, sqlContext}
import YohannEtArthurFontDuDataScience.HouseApp.sqlContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import shapeless.ops.nat.GT.>

object HouseApp extends App {

  val path: String = "/home/arthur/grosseDonnées/k/zestimate/"

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  //  val conf = new SparkConf().setAppName("house").setMaster("local[8]").set("spark.executor.memory", "2g")
  //  val sc = new SparkContext(conf)

  private val sSession: SparkSession = SparkSession.builder().appName("YohannEtArthurFontDuDataScience").master("local[8]").getOrCreate()
  //  private val flights: RDD[String] = sc.textFile("/home/arthur/grosseDonnées//2008.csv")
  private val sqlContext: SQLContext = sSession.sqlContext

  import sqlContext.implicits._

  val fic = "properties_2017.csv"
  println("loading " + fic)
  var props = sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(path + fic)
  println(fic + " loaded")

  val size = props
    .count()
  println(size)

  private val longs: Array[(String, Double)] = props
    .columns
    .map(str => (str, props
      .select(str)
      .filter(row => row.isNullAt(0))
      .count().toDouble / size.toDouble
    )).sortWith(_._2 > _._2)
  var i = 0
  private val length1: Int = props.columns.length
  while (i < length1) {
    printf("%30s : %f    %s\n", longs(i)._1, longs(i)._2, if (longs(i)._2 > 0.60) "EXCLURE" else "")
    if (longs(i)._2 > 0.60)
      props = props.drop(longs(i)._1)
    i += 1
  }

  props.describe().show()

  private val list: List[String] = props.columns.filter(_.contains("id")).filter(!_.equals("parcelid")).toList
  val hd :: tail = list

  private val frame: DataFrame = props.select(hd, tail: _*)

  frame.columns.map(str => {
    props.select(str)
      .filter(row => !row.isNullAt(0))
      .map(row => (row.get(0), 1))
  })
}
