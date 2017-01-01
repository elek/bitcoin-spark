package net.anzix.hadoop.blockfiles

import net.anzix.hadoop.Application
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.zuinnote.hadoop.bitcoin.format._


object UnspentScriptTypes extends Application {


  def run(session: SparkSession, inputPattern: String, outputDir: String) = {
    val sqlContext: SQLContext = session.sqlContext
    val sparkContext = sqlContext.sparkContext
    import sqlContext.implicits._

    val conf = new org.apache.hadoop.mapred.JobConf()
    FileInputFormat.addInputPath(conf, new Path(inputPattern))
    val rdd: RDD[(BytesWritable, BitcoinTransactionElement)] = sparkContext.hadoopRDD(conf, classOf[BitcoinTransactionElementFileInputFormat], classOf[BytesWritable], classOf[BitcoinTransactionElement], 2)
    //    unspent.toDF().write.mode(SaveMode.Overwrite).csv(HDFS + "unspent")

    val parsed = rdd.map(r => {
      val transactionHash = BitcoinUtil.convertByteArrayToHexString(r._2.getTransactionHash)
      TransactionElementWithBlock(r._2.getType, r._2.getTransactionIdxInBlock, transactionHash, r._2.getIndexInTransaction, r._2.getAmount, r._2.getScript)
    })


    val unspent = parsed.map(t => (t.transactionHash + "_" + t.indexInTransaction, t))
      .reduceByKey((t1, t2) => {
        var t = if (t1.transactionType == 1) t1 else t2
        t.copy(transactionType = 2)

      }).map(_._2)

    //    unspent.toDF().write.mode(SaveMode.Overwrite).csv(HDFS + "unspent")

    val unspentTxs = unspent.filter(_.transactionType == 1)
    unspentTxs.map(t => org.bitcoinj.script.ScriptUtil.parseToString(t.script))

    unspentTxs.toDF().createOrReplaceTempView("unspent")
    val sql: DataFrame = sqlContext.sql("SELECT * FROM unspent ORDER BY VALUE desc LIMIT 100")
    sql.repartition(1).write.mode(SaveMode.Overwrite).csv(outputDir)
  }


  case class TransactionElementWithBlock(transactionType: Int, trIndexInBlock: Int, transactionHash: String, indexInTransaction: Long, value: Long, script: Array[Byte])

  case class TransactionType(scriptType: String)

}