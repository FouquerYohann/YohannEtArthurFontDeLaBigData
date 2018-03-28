package YohannEtArthurFontDuDataScience

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, Metadata, StructField, StructType}

import scala.collection.mutable

object HouseApp extends App {


  def fill(repartition: Array[(Int, Double)]): Int = {
    val random = scala.util.Random
    val fl = random.nextFloat()
    var typeId = -1
    var i = 0
    while(i < repartition.length-1 && fl > repartition(i)._2) {
      i += 1
    }
    typeId = repartition(i)._1

    typeId
  }

  def flagColumns(dataFrame: DataFrame): DataFrame = {
    val flags = Seq(
      "fireplaceflag",
      "taxdelinquencyflag"
    )
    val cols = Seq(
      "fireplacecnt",
      "poolsizesum",
      "taxdelinquencyyear",
      "garagetotalsqft"
    )
    dataFrame.na.fill(false, flags)
      .na.fill(0, cols)
      .na.fill(1, Seq("unitcnt"))
  }
  def loadData(path: String, sqlContext: SQLContext): sql.DataFrame = {
    sqlContext.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(path)
  }

  def dropMissing(df: DataFrame, size: Long, sqlContext: SQLContext): sql.DataFrame = {
    import sqlContext.implicits._
    val longs = df
                .columns
                .map(str => (str, df
                                  .select(str)
                                  .filter(row => row.isNullAt(0))
                                  .count().toDouble / size.toDouble
                )).sortWith(_._2 > _._2)
    var i = 0
    val length = df.columns.length
    var tmp = List[String]()
    while (i < length) {
      printf("%30s : %f    %s\n", longs(i)._1, longs(i)._2, if (longs(i)._2 > 0.60) "EXCLURE" else "")
      if (longs(i)._2 > 0.60)
        tmp = longs(i)._1 :: tmp
      i += 1
    }
    df.drop(tmp: _*)
  }


  def fillNaWeightedDistribution(df: DataFrame, sqlContext: SQLContext): sql.DataFrame = {
    import sqlContext.implicits._

    var dfU = df

    dfU.columns.filter(n => n.contains("id") && !n.equals("parcelid")).foreach(name => {
      val l = dfU.select(name).filter(!_.anyNull).count()
      println(s"name = ${name}")
      println(s"l = ${l}")

      dfU.groupBy(name).count().show()
      val repartition = dfU.groupBy(name).count().filter(!_.anyNull).map(r => (r.getInt(0), r.getLong(1) / l.toDouble))
                   .collect().sortWith(_._2 < _._2)

      for (i <- 1 until repartition.length) {
        repartition(i) = (repartition(i)._1, repartition(i)._2 + repartition(i - 1)._2)
      }

      val frame = dfU.select(name).map(r => if(r.anyNull) fill(repartition) else r.getInt(0)).toDF()
          .withColumn("UniqueID", monotonically_increasing_id)
          .withColumnRenamed("value",name)
      dfU = dfU.withColumn("UniqueID",monotonically_increasing_id())

      dfU = dfU.drop(name).join(frame,"UniqueID").drop("UniqueID")

      frame.describe().show()

    })
    dfU
  }

  override def main(args: Array[String]): Unit = {
    val path: String = "data/"

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val nbCore = Runtime.getRuntime().availableProcessors()
    println(s"nbCore = ${nbCore}")
    val sSession = SparkSession.builder().appName("YohannEtArthurFontDuDataScience")
                   .master(s"local[$nbCore]").getOrCreate()
    val sqlContext = sSession.sqlContext

    import sqlContext.implicits._

    val fic = "properties_2017.csv"
    println("loading " + fic)
    var props = loadData(path + fic, sqlContext)
    println(fic + " loaded")

    props = flagColumns(props)

    val size = props.count()
    println(size)

    props = dropMissing(props,size,sqlContext)

    props = fillNaWeightedDistribution(props,sqlContext)


    props.describe().show()


    props.coalesce(1).write.csv("data/cleaned/test.csv")
  }
}
