package YohannEtArthurFontDuDataScience

import YohannEtArthurFontDuDataScience.HouseApp.loadData
import YohannEtArthurFontDuDataScience.LoadCsv.linearRegression
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.mean

object JustOne extends App {

  override def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val nbCore = Runtime.getRuntime().availableProcessors()

    println(s"nbCore = ${nbCore}")
    val sSession = SparkSession.builder().appName("YohannEtArthurFontDuDataScience")
      .master(s"local[${nbCore}]").getOrCreate()
    val sqlContext = sSession.sqlContext

    var fic = args(0)
    println("loading " + fic)
    var df = loadData(fic, sqlContext)
    println(fic + " loaded")

    fic = args(2)
    println("loading " + fic)
    var dfTest = loadData(fic, sqlContext)
    println(fic + " loaded")


    var props = loadData(args(1), sqlContext)
    var propsTest = loadData(args(3), sqlContext)

    props = props.join(df, "parcelid")
    propsTest = propsTest.join(dfTest, "parcelid")

    props = HouseApp.flagColumns(props)
    propsTest = HouseApp.flagColumns(propsTest)

    val whatShouldBeLeft = Array("bathroomcnt", "bedroomcnt", "calculatedbathnbr", "calculatedfinishedsquarefeet",
      "finishedsquarefeet12", "fireplacecnt", "fullbathcnt", "garagetotalsqft", "latitude", "longitude",
      "lotsizesquarefeet", "poolsizesum", "roomcnt", "unitcnt", "yearbuilt", "structuretaxvaluedollarcnt",
      "taxvaluedollarcnt", "assessmentyear", "landtaxvaluedollarcnt", "taxamount", "taxdelinquencyyear")


    props = HouseApp.fillNaWeightedDistribution(props, sqlContext)
    propsTest = HouseApp.fillNaWeightedDistribution(propsTest, sqlContext)

    props = props.na.fill(props.columns.filter(whatShouldBeLeft.contains(_)).zip(
      props.select(props.columns.filter(whatShouldBeLeft.contains(_)).map(mean(_)): _*).first.toSeq
    ).toMap)
    propsTest = propsTest.na.fill(propsTest.columns.filter(whatShouldBeLeft.contains(_)).zip(
      propsTest.select(propsTest.columns.filter(whatShouldBeLeft.contains(_)).map(mean(_)): _*).first.toSeq
    ).toMap)

    val modelLR = linearRegression(props, sqlContext)

    propsTest = modelLR.transform(propsTest)

    val predictionAndObservations = propsTest
      .select("prediction", Array("logerror"): _*)
      .rdd
      .map(row => (row.getDouble(0), row.getDouble(1)))
    val metrics = new RegressionMetrics(predictionAndObservations)
    println("rootMeanSquaredError: " + metrics.rootMeanSquaredError)
    println("meanAbsoluteError: " + metrics.meanAbsoluteError)
    println("explainedVariance: " + metrics.explainedVariance)
    println("meanSquaredError: " + metrics.meanSquaredError)
    println("r2: " + metrics.r2)

  }
}
