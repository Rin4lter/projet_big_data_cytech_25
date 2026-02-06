package fr.cytech.integration

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("Ex2_Branch1_Cleaning")
      .master("local[*]")
      .config("fs.s3a.access.key", "minio")
      .config("fs.s3a.secret.key", "minio123")
      .config("fs.s3a.endpoint", "http://localhost:9000")
      .config("fs.s3a.path.style.access", "true")
      .config("fs.s3a.connection.ssl.enabled", "false")
      .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      val inputPath = "s3a://nyc-raw/yellow_tripdata_2023-01.parquet"
      println(s"Reading...: $inputPath ...")

      val rawDF = spark.read.parquet(inputPath)
      val initialCount = rawDF.count()
      println(s"Number of rows in the raw data: $initialCount")

      var cleanDF = rawDF.filter(
        col("trip_distance") > 0.1 &&
          col("total_amount") > 2.5 &&
          col("passenger_count") > 0 &&
          col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")
      )

      cleanDF = cleanDF.withColumn("price_per_mile", col("total_amount") / col("trip_distance"))

      cleanDF = cleanDF.filter(
        (col("RatecodeID") === 1 &&
          col("price_per_mile") > 2.0 &&
          col("price_per_mile") < 25.0
          )
          ||
          (col("RatecodeID") === 2 &&
            col("total_amount") > 50 &&
            col("total_amount") < 150
            )
          ||
          (col("RatecodeID").isInCollection(Seq(3,4,5)) &&
            col("total_amount") < 500
            )
      )

      val quantiles = cleanDF.stat.approxQuantile("trip_distance", Array(0.997), 0.001)
      val maxDistance = quantiles(0)
      cleanDF = cleanDF.filter(col("trip_distance") < maxDistance)

      println(s"Trips exceeding $maxDistance miles have been excluded")

      val cleanCount = cleanDF.count()
      println(s"Number of rows after cleaning: $cleanCount")
      println(s"Removed ${initialCount - cleanCount} rows of invalid data")

      val outputPath = "s3a://nyc-processed/yellow_tripdata_2023-01_clean.parquet"

      println(s"Writing cleaned data to: $outputPath ...")
      cleanDF.write
        .mode("overwrite")
        .parquet(outputPath)

      println("Branch 1 complete! Data is ready.")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}