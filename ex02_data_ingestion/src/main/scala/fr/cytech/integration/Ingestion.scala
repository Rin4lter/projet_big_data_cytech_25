package fr.cytech.integration

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import java.util.Properties

object Ingestion {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("Ex2_Branch2_Ingestion")
      .master("local[*]")
      .config("fs.s3a.access.key", "minio")
      .config("fs.s3a.secret.key", "minio123")
      .config("fs.s3a.endpoint", "http://localhost:9000")
      .config("fs.s3a.path.style.access", "true")
      .config("fs.s3a.connection.ssl.enabled", "false")
      .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val jdbcUrl = "jdbc:postgresql://localhost:5432/nyc_warehouse"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "admin")
    connectionProperties.put("password", "password123")
    connectionProperties.put("driver", "org.postgresql.Driver")

    try {
      val inputPath = "s3a://nyc-processed/yellow_tripdata_2023-01_clean.parquet"
      println("Reading cleaned data...")
      val cleanDF = spark.read.parquet(inputPath)

      println("Generating and writing time data (dim_datetime)...")

      val pickupTimes = cleanDF.select(col("tpep_pickup_datetime").as("ts"))
      val dropoffTimes = cleanDF.select(col("tpep_dropoff_datetime").as("ts"))

      val allTimesDF = pickupTimes.union(dropoffTimes)
        .withColumn("hour_timestamp", date_trunc("hour", col("ts")))
        .select("hour_timestamp").distinct()

      val dimTimeDF = allTimesDF
        .withColumn("datetime_id", date_format(col("hour_timestamp"), "yyyyMMddHH"))
        .withColumn("timestamp_value", col("hour_timestamp"))
        .withColumn("year", year(col("hour_timestamp")))
        .withColumn("month", month(col("hour_timestamp")))
        .withColumn("day", dayofmonth(col("hour_timestamp")))
        .withColumn("hour", hour(col("hour_timestamp")))
        .withColumn("weekday", dayofweek(col("hour_timestamp")))
        .withColumn("is_weekend", when(dayofweek(col("hour_timestamp")).isin(1, 7), true).otherwise(false))
        .select(
          "datetime_id", "timestamp_value", "year", "month", "day", "hour", "weekday", "is_weekend"
        )

      dimTimeDF.write
        .mode(SaveMode.Append)
        .jdbc(jdbcUrl, "dim_datetime", connectionProperties)

      println("Generating and writing fact data (fact_trips)...")

      val factDF = cleanDF
        .withColumn("pickup_datetime_id", date_format(col("tpep_pickup_datetime"), "yyyyMMddHH"))
        .withColumn("dropoff_datetime_id", date_format(col("tpep_dropoff_datetime"), "yyyyMMddHH"))
        .withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .withColumnRenamed("RatecodeID", "rate_code_id")
        .withColumnRenamed("payment_type", "payment_type_id")
        .select(
          "vendor_id",
          "pickup_datetime_id", "dropoff_datetime_id",
          "pickup_location_id", "dropoff_location_id",
          "rate_code_id", "payment_type_id",
          "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax",
          "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
          "congestion_surcharge", "airport_fee"
        )

      factDF.write
        .mode(SaveMode.Append)
        .jdbc(jdbcUrl, "fact_trips", connectionProperties)

      println("Branch 2 complete!")

    } catch {
      case e: Exception =>
        e.printStackTrace()
        println("ERROR")
    } finally {
      spark.stop()
    }
  }
}