package net.anzix.hadoop.hbase

import net.anzix.hadoop.{ConfigurableApplication, DefaultJobConfiguration}
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OptionParser

object BlockWithFewTransactions extends ConfigurableApplication[DefaultJobConfiguration] {

  def optionDefinition(parser: OptionParser[DefaultJobConfiguration], args: Array[String]) = {
    defaultJobOptionDefinition(parser, args)
  }

  def defaultConfiguration() = {
    new DefaultJobConfiguration(outputDir = "/result/block-few-trans")
  }

  def run(session: SparkSession, configuration: DefaultJobConfiguration): Unit = {
    import session.sqlContext.implicits._
    HBaseLoader.blocks(session.sqlContext).sort($"transactionCounter".asc).limit(1000).write.mode(SaveMode.Overwrite).csv(configuration.outputDir)
  }


}