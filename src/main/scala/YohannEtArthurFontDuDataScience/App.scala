package YohannEtArthurFontDuDataScience

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * Hello world!
 *
 */
object App extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  private val sSession: SparkSession = SparkSession.builder().appName("YohannEtArthurFontDuDataScience").master("local[5]").getOrCreate()

  private val sqlContext: SQLContext = sSession.sqlContext

  private val df: DataFrame = sqlContext.read.format("csv").option("header",true).load("properties_2016.csv")
  import sqlContext.implicits._


  df.orderBy(desc("basementsqft")).show(10)

}
