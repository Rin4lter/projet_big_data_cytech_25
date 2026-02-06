package fr.cytech.integration

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    // 1. 初始化 SparkSession
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
      // 2. 读取原始数据 (来自 Ex1 上传的 nyc_raw Bucket)
      // 注意：请确保文件名与你上传的一致
      val inputPath = "s3a://nyc-raw/yellow_tripdata_2023-01.parquet"
      println(s"正在读取: $inputPath ...")

      val rawDF = spark.read.parquet(inputPath)
      val initialCount = rawDF.count()
      println(s"原始数据行数: $initialCount")

      // 3. 数据验证 (Branch 1 核心逻辑)
      // 根据 'Data Contract' 进行过滤
      val cleanDF = rawDF.filter(
        col("trip_distance") > 0 &&
          col("passenger_count") > 0 &&
          col("total_amount") > 0 &&
          col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")
      )

      val cleanCount = cleanDF.count()
      println(s"清洗后数据行数: $cleanCount")
      println(s"移除了 ${initialCount - cleanCount} 行无效数据")

      // 4. 将清洗后的数据写回 Minio (用于 ML Training)
      // 目标 Bucket: nyc_processed (如果没有会自动创建文件夹，但最好先在 Minio Console 创建 Bucket)
      val outputPath = "s3a://nyc-processed/yellow_tripdata_2023-01_clean.parquet"

      println(s"正在写入清洗后的数据到: $outputPath ...")
      cleanDF.write
        .mode("overwrite")
        .parquet(outputPath)

      println("Branch 1 完成！数据已就绪。")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}