package net.anzix.hadoop

import org.apache.spark.sql.SparkSession

trait Application {


  def main(args: Array[String]) = {
    if (args.length != 3) {
      println("Usage fsUrl inputDir outputDir")
    } else {
      //val session =
      val session = if (args(0).startsWith("hdfs"))
        SparkSession.builder().getOrCreate()
      else
        SparkSession.builder
          .master("local")
          .config("spark.driver.port", 4353)
          .config("spark.driver.host", "localhost")
          .appName("btc")
          .getOrCreate()

      run(session, args(0) + args(1), args(0) + args(2))
    }
  }

  def run(session: SparkSession, inputPattern: String, outputDir: String)
}