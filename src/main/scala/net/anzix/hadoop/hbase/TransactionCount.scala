package net.anzix.hadoop.hbase

import net.anzix.hadoop.{ConfigurableApplication, DefaultJobConfiguration}
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OptionParser

object TransactionCount extends ConfigurableApplication[DefaultJobConfiguration] {
  
  def optionDefinition(parser: OptionParser[DefaultJobConfiguration], args: Array[String]) = {
    defaultJobOptionDefinition(parser, args)
  }

  def defaultConfiguration() = {
    new DefaultJobConfiguration(outputDir = "/result/transaction-count")
  }

  def run(session: SparkSession, configuration: DefaultJobConfiguration): Unit = {
    import session.sqlContext.implicits._
    val sum = (a: Int, b: Int) => a + b
    val reduce: Int = HBaseLoader.transactions(session.sqlContext).toDF().map(a => 1).reduce(sum)
    session.sparkContext.parallelize(Map("result" -> reduce).toSeq).toDF().write.mode(SaveMode.Overwrite).csv(configuration.outputDir)
  }


}