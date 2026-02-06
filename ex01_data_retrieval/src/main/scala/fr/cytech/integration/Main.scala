package fr.cytech.integration

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import java.net.URL
import java.io.BufferedInputStream

object Main {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("Ex1_Data_Retrieval")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      // Hadoop Configuration
      val hadoopConf = spark.sparkContext.hadoopConfiguration

      // Minio Authentication Information
      hadoopConf.set("fs.s3a.access.key", "minio")
      hadoopConf.set("fs.s3a.secret.key", "minio123")
      hadoopConf.set("fs.s3a.endpoint", "http://localhost:9000")

      // SimpleAWSCredentialsProvider
      hadoopConf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

      // Minio Compatibility Configuration
      hadoopConf.set("fs.s3a.path.style.access", "true")
      hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
      hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

      val fileUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"

      val bucketName = "nyc-raw"
      val fileName = "yellow_tripdata_2023-01.parquet"
      val destPath = new Path(s"s3a://$bucketName/$fileName")

      println(s"Begin to download from $fileUrl")

      val fs = FileSystem.get(new URI(s"s3a://$bucketName"), hadoopConf)

      // Execute streaming
      val in = new BufferedInputStream(new URL(fileUrl).openStream())
      val out = fs.create(destPath, true) // true = overwrite

      val buffer = new Array[Byte](8192)
      var bytesRead = 0
      var totalBytes = 0L

      while ({bytesRead = in.read(buffer); bytesRead != -1}) {
        out.write(buffer, 0, bytesRead)
        totalBytes += bytesRead
      }

      out.close()
      in.close()

      println(s"Success! The file has been saved to: $destPath")
      println(s"Total size: ${totalBytes / 1024 / 1024} MB")

    } catch {
      case e: Exception =>
        e.printStackTrace()
        println("ERROR")
    } finally {
      spark.stop()
    }
  }
}