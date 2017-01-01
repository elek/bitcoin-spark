package net.anzix.hadoop

import org.apache.spark.sql.SparkSession
import scopt.OptionParser

trait ConfigurableApplication[T] {

  def session(config: T, applicationName: String): SparkSession = {
    if (!config.asInstanceOf[JobConfiguration].localMode)
      SparkSession.builder
        .appName(applicationName)
        .getOrCreate()

    else
      SparkSession.builder
        .master("local")
        .config("spark.driver.port", 4353)
        .config("spark.driver.host", "localhost")
        .appName(applicationName)
        .getOrCreate()
  }

  def main(args: Array[String]) = {
    val parser = new scopt.OptionParser[T]("scopt") {


      optionDefinition(this, args)
    }

    parser.parse(args, defaultConfiguration()) match {
      case Some(config) =>
        run(session(config, getClass.getName), config.asInstanceOf[T])
      case None =>
      // arguments are bad, error message will have been displayed
    }
  }

  def optionDefinition(parser: OptionParser[T], args: Array[String])

  def defaultJobOptionDefinition(parser: OptionParser[DefaultJobConfiguration], args: Array[String]) = {
    parser.opt[Boolean]('l', "local").action((argument, c) =>
      c.copy(localMode = argument)).text("True if spark session should be initialized locally")
    parser.opt[Boolean]('o', "output").action((argument, c) =>
      c.copy(localMode = argument)).text("Output directory")
  }

  def defaultConfiguration(): T

  def run(session: SparkSession, configuration: T): Unit

}

//
//opt[Boolean]('l', "local").action((argument, c) =>
//c.copy(localMode = argument)).text("True if spark session should be initialized locally")
//opt[Boolean]('o', "output").action((argument, c) =>
//c.copy(localMode = argument)).text("Output directory")