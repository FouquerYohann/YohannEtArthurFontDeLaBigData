package YohannEtArthurFontDuDataScience

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

import scala.collection.mutable

object HouseApp extends App {


  def fill(repartition: Array[(Int, Double)]): Int = {
    val random = scala.util.Random
    val fl = random.nextFloat()
    var typeId = -1
    var i = -1
    do {
      i += 1
      typeId = repartition(i)._1
    } while (fl > repartition(i)._2)
    println(fl)
    for (elem <- repartition) {
      println("\t"+elem._1)
      println("\t"+elem._2)
    }
    println(typeId)
    println()
    typeId
  }


  override def main(args: Array[String]): Unit = {
    val path: String = "data/"

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val i1 = 1

    //  val conf = new SparkConf().setAppName("house").setMaster("local[8]").set("spark.executor.memory", "2g")
    //  val sc = new SparkContext(conf)
    val nbCore = Runtime.getRuntime().availableProcessors()
    println(s"nbCore = ${nbCore}")
    val sSession = SparkSession.builder().appName("YohannEtArthurFontDuDataScience")
                  .master(s"local[$nbCore]").getOrCreate()
    //  private val flights: RDD[String] = sc.textFile("/home/arthur/grosseDonnées//2008.csv")
    val sqlContext = sSession.sqlContext

    import sqlContext.implicits._

    val fic = "properties_2017.csv"
    println("loading " + fic)
    var props = sqlContext.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path + fic)
    println(fic + " loaded")

      val size = props
                 .count()
      println(size)

    val longs = props
                  .columns
                  .map(str => (str, props
                                    .select(str)
                                    .filter(row => row.isNullAt(0))
                                    .count().toDouble / size.toDouble
                  )).sortWith(_._2 > _._2)
      var i = 0
      val length = props.columns.length
      while (i < length) {
        printf("%30s : %f    %s\n", longs(i)._1, longs(i)._2, if (longs(i)._2 > 0.60) "EXCLURE" else "")
        if (longs(i)._2 > 0.60)
          props = props.drop(longs(i)._1)
        i += 1
      }

//    println()
//    println()
//
      props.describe().show()
//
      val list = props.columns.filter(_.contains("id")).filter(!_.equals("parcelid")).toList
      val hd :: tail = list




//

    for (name <- list) {
      var mapping = mutable.HashMap()


      val l = props.select(name).filter(!_.anyNull).count()

      var tuples = props.select(name).filter(!_.anyNull).map(r => (r.getInt(0), 1)).groupByKey(_._1)
                   .reduceGroups((t1, t2) => {
                     (t1._1, t1._2 + t2._2)
                   }).map(st => (st._2._1, st._2._2 / l.toDouble)).collect()

      tuples = tuples.sortWith(_._2 < _._2)
      for (i <- 1 until tuples.length) {
        tuples(i) = (tuples(i)._1, tuples(i)._2 + tuples(i - 1)._2)
      }

      props = props.na.fill(fill(tuples), Seq(name))

    }

    props.describe().show()

  }
}
