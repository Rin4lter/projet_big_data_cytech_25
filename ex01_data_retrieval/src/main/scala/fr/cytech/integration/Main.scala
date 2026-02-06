package fr.cytech.integration

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import java.net.URL
import java.io.BufferedInputStream

object Main {
  def main(args: Array[String]): Unit = {
    // 1. 初始化 SparkSession
    val spark = SparkSession.builder()
      .appName("Ex1_Data_Retrieval")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      // ==========================================
      // 关键修复：直接配置 Hadoop Configuration
      // ==========================================
      val hadoopConf = spark.sparkContext.hadoopConfiguration

      // Minio 认证信息
      hadoopConf.set("fs.s3a.access.key", "minio")
      hadoopConf.set("fs.s3a.secret.key", "minio123")
      hadoopConf.set("fs.s3a.endpoint", "http://localhost:9000")

      // 关键配置：强制使用 SimpleAWSCredentialsProvider (即读取上面的 key/secret)
      hadoopConf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

      // Minio 兼容性配置
      hadoopConf.set("fs.s3a.path.style.access", "true")
      hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
      hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

      // 2. 定义数据源和目标
      val fileUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"

      // 注意：这里使用你上次修改后的新 bucket 名称 "nyc-raw" (中间是横杠)
      val bucketName = "nyc-raw"
      val fileName = "yellow_tripdata_2023-01.parquet"
      val destPath = new Path(s"s3a://$bucketName/$fileName")

      println(s"开始从 $fileUrl 下载...")

      // 3. 获取 Hadoop FileSystem 对象 (传入我们配置好的 hadoopConf)
      val fs = FileSystem.get(new URI(s"s3a://$bucketName"), hadoopConf)

      // 4. 执行流式传输
      val in = new BufferedInputStream(new URL(fileUrl).openStream())
      val out = fs.create(destPath, true) // true = overwrite

      // 使用 Scala 风格的 Array[Byte]
      val buffer = new Array[Byte](8192)
      var bytesRead = 0
      var totalBytes = 0L

      while ({bytesRead = in.read(buffer); bytesRead != -1}) {
        out.write(buffer, 0, bytesRead)
        totalBytes += bytesRead
      }

      out.close()
      in.close()

      println(s"成功! 文件已保存到: $destPath")
      println(s"总大小: ${totalBytes / 1024 / 1024} MB")

    } catch {
      case e: Exception =>
        e.printStackTrace()
        println("发生错误，请检查 Minio 是否启动以及网络连接。")
    } finally {
      spark.stop()
    }
  }
}