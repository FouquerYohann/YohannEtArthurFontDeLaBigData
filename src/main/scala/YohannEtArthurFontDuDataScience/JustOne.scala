package YohannEtArthurFontDuDataScience

import YohannEtArthurFontDuDataScience.HouseApp.loadData
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.mean

object JustOne extends App {

  override def main(args: Array[String]): Unit = {
    val path: String = "data/"

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val nbCore = Runtime.getRuntime().availableProcessors()
    println(s"nbCore = ${nbCore}")
    val sSession = SparkSession.builder().appName("YohannEtArthurFontDuDataScience")
                   .master(s"local[${nbCore}]").getOrCreate()
    val sqlContext = sSession.sqlContext

    var fic = "properties_2016.csv"
    println("loading " + fic)
    var df = loadData(path + fic, sqlContext)
    println(fic + " loaded")


    var props = loadData("data/train_2016_v2.csv", sqlContext)

    props = props.join(df, "parcelid")

    props = HouseApp.flagColumns(props)


    val whatShouldBeLeft = Array("bathroomcnt", "bedroomcnt", "calculatedbathnbr", "calculatedfinishedsquarefeet",
      "finishedsquarefeet12", "fireplacecnt", "fullbathcnt", "garagetotalsqft", "latitude", "longitude",
      "lotsizesquarefeet", "poolsizesum", "roomcnt", "unitcnt", "yearbuilt", "structuretaxvaluedollarcnt",
      "taxvaluedollarcnt", "assessmentyear", "landtaxvaluedollarcnt", "taxamount", "taxdelinquencyyear")


    props = HouseApp.fillNaWeightedDistribution(props,sqlContext)

    props = props.na.fill(props.columns.filter(whatShouldBeLeft.contains(_)).zip(
      props.select(props.columns.filter(whatShouldBeLeft.contains(_)).map(mean(_)): _*).first.toSeq
    ).toMap)


    props.show()
    props
    .coalesce(1)
    .write.format("com.databricks.spark.csv")
    .option("header", "true")
    .save("2016FilledWithMeanAndWeighted2016.csv")

  }
}
