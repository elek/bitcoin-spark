package net.anzix.hadoop

;

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.bitcoinj.script.ScriptUtil
import org.zuinnote.hadoop.bitcoin.format._


object BlockWithScript extends Application {

  override def run(session: SparkSession, inputPattern: String, outputDir: String): Unit = {

    import session.sqlContext.implicits._
    val sc = session.sparkContext
    val conf = new org.apache.hadoop.mapred.JobConf()
    FileInputFormat.addInputPath(conf, new Path(inputPattern))
    val rdd: RDD[(BytesWritable, BitcoinTransactionElement)] = sc.hadoopRDD(conf, classOf[BitcoinTransactionElementFileInputFormat], classOf[BytesWritable], classOf[BitcoinTransactionElement], 2)


    rdd.map(a => {
      val blockHash = BitcoinUtil.convertByteArrayToHexString(a._2.getBlockHash)
      BlockWithScriptType(blockHash, ScriptUtil.parseToString(a._2.getScript))
    }).toDF().createOrReplaceTempView("scripts")

    val sql: DataFrame = session.sqlContext.sql("SELECT * FROM scripts WHERE scriptStruct=\"\" LIMIT 30")
    sql.repartition(1).write.mode(SaveMode.Overwrite).csv(outputDir)


  }
}

case class BlockWithScriptType(blockId: String, scriptStruct: String)
