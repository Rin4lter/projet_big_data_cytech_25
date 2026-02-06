package fr.cytech.integration

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import java.util.Properties

object Ingestion {
  def main(args: Array[String]): Unit = {
    // 1. 初始化 SparkSession
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

    // === 配置数据库连接 ===
    val jdbcUrl = "jdbc:postgresql://localhost:5432/nyc_warehouse"
    val connectionProperties = new Properties()
    connectionProperties.put("user", "admin")
    connectionProperties.put("password", "password123")
    connectionProperties.put("driver", "org.postgresql.Driver")

    try {
      // 2. 读取 Minio 中清洗后的数据 (来自 Branch 1)
      // 注意：确保路径对应你生成的文件夹
      val inputPath = "s3a://nyc-processed/yellow_tripdata_2023-01_clean.parquet"
      println("正在读取清洗后的数据...")
      val cleanDF = spark.read.parquet(inputPath)

      // ==========================================
      // 任务 A: 填充 dim_datetime (时间维度表)
      // ==========================================
      println("正在生成并写入时间维度数据 (dim_datetime)...")

      // 逻辑：把“上车时间”和“下车时间”都取出来，放到一列里，去重，生成时间字典
      val pickupTimes = cleanDF.select(col("tpep_pickup_datetime").as("ts"))
      val dropoffTimes = cleanDF.select(col("tpep_dropoff_datetime").as("ts"))

      val allTimesDF = pickupTimes.union(dropoffTimes)
        .withColumn("hour_timestamp", date_trunc("hour", col("ts"))) // 截断到小时
        .select("hour_timestamp").distinct() // 去重，只保留唯一的小时时刻

      // 生成维度表的各个字段
      val dimTimeDF = allTimesDF
        .withColumn("datetime_id", date_format(col("hour_timestamp"), "yyyyMMddHH")) // 生成主键 ID
        .withColumn("timestamp_value", col("hour_timestamp")) // 对应 SQL 里的 timestamp 字段
        .withColumn("year", year(col("hour_timestamp")))
        .withColumn("month", month(col("hour_timestamp")))
        .withColumn("day", dayofmonth(col("hour_timestamp")))
        .withColumn("hour", hour(col("hour_timestamp")))
        .withColumn("weekday", dayofweek(col("hour_timestamp"))) // Spark: 1=Sun, 2=Mon...
        .withColumn("is_weekend", when(dayofweek(col("hour_timestamp")).isin(1, 7), true).otherwise(false))
        .select(
          "datetime_id", "timestamp_value", "year", "month", "day", "hour", "weekday", "is_weekend"
        )

      // 写入 Postgres
      // 注意：使用 Append 模式。如果表里已经有相同的 ID，会报错（主键冲突）。
      // 生产环境中通常用 "INSERT IGNORE" 或先查后插，这里为简化，假设是第一次运行或先清空表
      dimTimeDF.write
        .mode(SaveMode.Append)
        .jdbc(jdbcUrl, "dim_datetime", connectionProperties)

      // ==========================================
      // 任务 B: 填充 fact_trips (事实表)
      // ==========================================
      println("正在生成并写入事实表数据 (fact_trips)...")

      val factDF = cleanDF
        // 1. 生成外键 ID (对应 dim_datetime 的主键)
        .withColumn("pickup_datetime_id", date_format(col("tpep_pickup_datetime"), "yyyyMMddHH"))
        .withColumn("dropoff_datetime_id", date_format(col("tpep_dropoff_datetime"), "yyyyMMddHH"))
        // 2. 字段重命名 (Parquet 列名 -> SQL 列名)
        .withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .withColumnRenamed("RatecodeID", "rate_code_id")
        .withColumnRenamed("payment_type", "payment_type_id")
        // 3. 选出事实表需要的列
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

      println("Ex2 Branch 2 入库完成！")

    } catch {
      case e: Exception =>
        e.printStackTrace()
        println("出错提示：如果是主键冲突(Duplicate Key)，请尝试清空 Postgres 表数据后重试。")
    } finally {
      spark.stop()
    }
  }
}