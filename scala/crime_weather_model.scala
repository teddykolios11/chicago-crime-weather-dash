import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{RandomForestRegressor, RandomForestRegressionModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator

object CrimeWeatherModel {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CrimeWeatherHourlyModel")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    println(s"Spark master = ${spark.sparkContext.master}")

    // gather batch data

    val crimes_weather_hourly = spark.table("tkolios_crime_weather_hourly")

    println(s"Crimes rows = ${crimes_weather_hourly.count()}")

    val df = crimes_weather_hourly
      .withColumn("day_of_week", dayofweek($"day"))
      .withColumn("month", month($"day"))
      .withColumn("is_weekend", when($"day_of_week" >= 6, 1).otherwise(0))


    // train model

    val featureCols = Array(
      "hour",
      "day_of_week",
      "month",
      "is_weekend",
      "tmax",
      "tmin",
      "prcp",
      "snow",
      "snwd"
    )

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val rf = new RandomForestRegressor()
      .setFeaturesCol("features")
      .setLabelCol("crime_count")
      .setPredictionCol("prediction")
      .setNumTrees(50)
      .setMaxDepth(10)

    val pipeline = new Pipeline().setStages(Array(assembler, rf))

    val Array(train, test) = df.randomSplit(Array(0.8, 0.2), seed = 42L)

    println(s"train rows = ${train.count()}, test rows = ${test.count()}")

    val model: PipelineModel = pipeline.fit(train)
    println("model trained")


    // gather crime count predictions

    val testPreds = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("crime_count")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(testPreds)
    println(s"test RMSE = $rmse")


    // predict for full dataset
    val fullPreds = model.transform(df)

    fullPreds
    .select(
        $"day",
        $"hour",
        $"crime_count",
        $"prediction",
        $"tmax",
        $"tmin",
        $"prcp",
        $"snow",
        $"snwd"
    )
    .write
    .mode("overwrite")
    .saveAsTable("tkolios_crime_weather_hourly_scala")


    // construct feature importance table --> gather feature weights

    val rfModel = model.stages
      .collectFirst { case m: RandomForestRegressionModel => m }
      .getOrElse(throw new RuntimeException("RandomForestRegressionModel not found in pipeline stages"))

    val importances = rfModel.featureImportances.toArray
    val featureNames = featureCols

    val featureImportanceSeq = featureNames.zip(importances).toSeq
    val featureImportanceDF = featureImportanceSeq.toDF("feature", "importance")

    // write to hive
    featureImportanceDF.write
      .mode("overwrite")
      .saveAsTable("tkolios_crime_feature_importance")
    

    // generate time-weather crime predictions

    val hours       = 0 to 23
    val daysOfWeek  = 1 to 7

    // instantiate buckets for classifying weather conditions
    val tmaxBins    = Seq(-10.0, 0.0, 10.0, 20.0, 30.0, 40.0)
    val prcpBins    = Seq(0.0, 5.0)
    val snowBins    = Seq(0.0, 5.0)
    val snwdBins    = Seq(0.0, 10.0)

    // pick neutral month
    val monthVal = 6

    val grid = for {
      h   <- hours
      dow <- daysOfWeek
      tmx <- tmaxBins
      tmi  = tmx - 10.0
      pr  <- prcpBins
      sn  <- snowBins
      swd <- snwdBins
    } yield (h, dow, monthVal, if (dow >= 6) 1 else 0, tmx, tmi, pr, sn, swd)

    val gridDf = grid.toDF(
      "hour",
      "day_of_week",
      "month",
      "is_weekend",
      "tmax",
      "tmin",
      "prcp",
      "snow",
      "snwd"
    )

    val timeWeatherPreds = model.transform(gridDf)
      .select(
        $"hour",
        $"day_of_week",
        $"tmax",
        $"tmin",
        $"prcp",
        $"snow",
        $"snwd",
        $"prediction"
      )

    // write to hive
    timeWeatherPreds.write
      .mode("overwrite")
      .saveAsTable("tkolios_crime_predictor_grid")


    spark.stop()
  }
}