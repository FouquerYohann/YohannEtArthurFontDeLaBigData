package YohannEtArthurFontDuDataScience

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HouseApp extends App {


  def fill(repartition: Array[(Int, Double)]): Int = {
    val random = scala.util.Random
    val fl = random.nextFloat()
    var typeId = -1
    var i = 0
    while (i < repartition.length - 1 && fl > repartition(i)._2) {
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

    val list = df.columns.map(name => (name, df.select(name).filter(col(name).isNull).count() / size.toDouble)).toList
      .sortWith(_._2 > _._2)

    var tmp = List[String]()
    for (i <- df.columns.indices) {
      printf("%30s : %f    %s\n", list(i)._1, list(i)._2, if (list(i)._2 > 0.60) "EXCLURE" else "")
      if (list(i)._2 > 0.60)
        tmp = list(i)._1 :: tmp
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

      val frame = dfU.select(name).map(r => if (r.anyNull) fill(repartition) else r.getInt(0)).toDF()
        .withColumn("UniqueID", monotonically_increasing_id)
        .withColumnRenamed("value", name)
      dfU = dfU.withColumn("UniqueID", monotonically_increasing_id())

      dfU = dfU.drop(name).join(frame, "UniqueID").drop("UniqueID")

      frame.describe().show()

    })
    dfU
  }

  //  def fillMean(df:DataFrame, names: String*): sql.DataFrame ={
  //
  //    var dfR = df
  //    for (name <- names) {
  //      var nb=0
  //      var sum:Double = 0
  //      dfR.select(name).foreach(r => {
  //        if(!r.anyNull){
  //          nb +=1
  //          sum += if(r.get.getDouble(0)
  //        }
  //      })
  //      dfR = dfR.na.fill(sum/nb,Array(name))
  //    }
  //    dfR
  //  }

  def decisionTreeFiller(df: DataFrame, sqlContext: SQLContext, colu: String): sql.DataFrame = {

    var discreetCol = Array("buildingqualitytypeid", "heatingorsystemtypeid", "propertylandusetypeid",
      "regionidcity", "regionidzip", "regionidcounty")

    val iDontKnowWhattoDo = Array("fips", "rawcensustractandblock", "censustractandblock", "propertycountylandusecode",
      "propertyzoningdesc")

    val dfNull = df.drop(iDontKnowWhattoDo: _*).filter(col(colu).isNull).toDF()
    var dfNotNull = df.drop(iDontKnowWhattoDo: _*).filter(!col(colu).isNull).toDF()

    discreetCol = discreetCol.filter(!_.equals(colu))

    dfNotNull = dfNotNull.na.fill(404, discreetCol)

    dfNotNull.columns.foreach(println(_))

    val whatShouldBeLeft = Array("bathroomcnt", "bedroomcnt", "calculatedbathnbr", "calculatedfinishedsquarefeet",
      "finishedsquarefeet12", "fireplacecnt", "fullbathcnt", "garagetotalsqft", "latitude", "longitude",
      "lotsizesquarefeet", "poolsizesum", "roomcnt", "unitcnt", "yearbuilt", "structuretaxvaluedollarcnt",
      "taxvaluedollarcnt", "assessmentyear", "landtaxvaluedollarcnt", "taxamount", "taxdelinquencyyear")


    val labelIndexer = new StringIndexer()
      .setInputCol(colu)
      .setOutputCol("indexedLabel")
      .fit(dfNotNull)

    //    val labelEstimator = new OneHotEncoderEstimator().setInputCols(Array(colu)).setOutputCols(Array(colu+"labeled")).fit(dfNotNull)
    //val imputer = new Imputer().setInputCols(whatShouldBeLeft).setOutputCols(whatShouldBeLeft.map(_+"imput")).fit(dfNotNull)
    val oneHotEncoder = new OneHotEncoderEstimator().setInputCols(discreetCol).setOutputCols(discreetCol.map(_ + "vect")).fit(dfNotNull)

    val VectorAssembler = new VectorAssembler().setInputCols(discreetCol.map(_ + "vect") ++ whatShouldBeLeft).setOutputCol("features")

    dfNotNull = dfNotNull.na.fill(dfNotNull.columns.filter(whatShouldBeLeft.contains(_)).zip(
      dfNotNull.select(dfNotNull.columns.filter(whatShouldBeLeft.contains(_)).map(mean(_)): _*).first.toSeq
    ).toMap)

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)


    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("features")

    val pipeline = new Pipeline().setStages(Array(labelIndexer, oneHotEncoder, VectorAssembler, dt, labelConverter))

    val model = pipeline.fit(dfNotNull)

    val predictions = model.transform(dfNull)


    predictions.show()


    df
  }

  def linearRegression(df: DataFrame, sqlContext: SQLContext): DataFrame = {

    val numeriCols = Array("bathroomcnt",
      "bedroomcnt",
      "calculatedbathnbr",
      "calculatedfinishedsquarefeet",
      "fireplacecnt",
      "fullbathcnt",
      "garagecarcnt",
      "latitude",
      "longitude",
      "lotsizesquarefeet",
      "numberofstories",
      "poolcnt",
      "roomcnt",
      "yearbuilt",
      "taxamount",
      "taxdelinquencyyear"

    ).filter(df.columns.contains(_))


    val assembler = new VectorAssembler()
      .setInputCols(numeriCols)
      .setOutputCol("features")

    val lr = new LinearRegression().setLabelCol("logerror")
    val pipeline = new Pipeline()
      .setStages(Array(assembler, lr))

    val Array(trainSet, testSet) = df.na.fill(0).randomSplit(Array(0.9, 0.1))
    // Entrainement du modèle sur trainSet
    val modelLR = pipeline.fit(trainSet)
    // Prédiction sur testSet
    val predictions = modelLR.transform(testSet)
    predictions.select("logerror", "prediction").show()
    df
  }

  override def main(args: Array[String]): Unit = {
    val path: String = "data/"

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val nbCore = Runtime.getRuntime().availableProcessors()
    println(s"nbCore = ${nbCore}")
    val sSession = SparkSession.builder().appName("YohannEtArthurFontDuDataScience")
      .master(s"local[${nbCore}]").getOrCreate()
    val sqlContext = sSession.sqlContext

    val fic = "properties_2017.csv"
    println("loading " + fic)
    var props = loadData(path + fic, sqlContext)
    println(fic + " loaded")

    val predFile = "train_2017.csv"
    println("loading " + predFile)
    var pred = loadData(path + predFile, sqlContext)
    println(predFile + " loaded")

    props = props.join(pred, "parcelid")
    props.show()
//    props = props.sample(0.01)

    props = flagColumns(props)

    val size = props.count()
    println(size)

    props = dropMissing(props, size, sqlContext)

//    props = fillNaWeightedDistribution(props, sqlContext)

    //    props = decisionTreeFiller(props, sqlContext, "buildingqualitytypeid")

    props = linearRegression(props, sqlContext)

    props
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("mydata.csv")
    //    props.describe().show()

  }
}
