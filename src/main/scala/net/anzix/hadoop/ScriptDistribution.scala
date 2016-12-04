package net.anzix.hadoop

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.bitcoinj.script.ScriptUtil
import org.zuinnote.hadoop.bitcoin.format._

import scala.collection.JavaConversions._

case class TransactionType(scriptType: String)

object ScriptDistribution extends Application {

  override def run(session: SparkSession, inputPattern: String, outputDir: String): Unit = {
//    import session.sqlContext.implicits._
//    val sc = session.sparkContext
//    val conf = new org.apache.hadoop.mapred.JobConf()
//    FileInputFormat.addInputPath(conf, new Path(inputPattern))
//    val rdd = sc.hadoopRDD(conf, classOf[BitcoinTransactionFileInputFormat], classOf[BytesWritable], classOf[BitcoinTransactionWithBlockInfo], 2)
//    val result = rdd
//      .flatMap(t => t._2.getListOfOutputs())
//      .map(_.getTxOutScript())
//      .map(a => TransactionType(ScriptUtil.parseToString(a)))
//
//    result.toDF().createOrReplaceTempView("scripts")
//
//    val sql: DataFrame = session.sqlContext.sql("SELECT scriptType,count(1) AS count FROM scripts GROUP by scriptType")
//    sql.repartition(1).sortWithinPartitions("count").write.mode(SaveMode.Overwrite).csv(outputDir)


  }
}
