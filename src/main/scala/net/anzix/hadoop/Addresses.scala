package net.anzix.hadoop

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.bitcoinj.script.ScriptUtil
import org.zuinnote.hadoop.bitcoin.format._


object Addresses extends Application {

  def run(session: SparkSession, inputPattern: String, outputDir: String) = {

    val sqlContext: SQLContext = session.sqlContext
    val sparkContext = sqlContext.sparkContext
    import sqlContext.implicits._

    val conf = new org.apache.hadoop.mapred.JobConf()
    FileInputFormat.addInputPath(conf, new Path(inputPattern))
    val rdd: RDD[(BytesWritable, BitcoinTransactionElement)] = sparkContext.hadoopRDD(conf, classOf[BitcoinTransactionElementFileInputFormat], classOf[BytesWritable], classOf[BitcoinTransactionElement], 2)

    val parsed = rdd.map(r => {
      val blockHash = BitcoinUtil.convertByteArrayToHexString(r._2.getBlockHash)
      val transactionHash = BitcoinUtil.convertByteArrayToHexString(r._2.getTransactionHash)
      TransactionElement(r._2.getType, blockHash, r._2.getTransactionIdxInBlock, transactionHash, r._2.getIndexInTransaction, r._2.getAmount, r._2.getScript)
    })

    val unspent = parsed.map(t => (t.transactionHash + "_" + t.indexInTransaction, t))
      .reduceByKey((t1, t2) => {
        var t = if (t1.transactionType == 1) t1 else t2
        t.copy(transactionType = 2)

      }).map(_._2)

    val unspentTxs = unspent.filter(_.transactionType == 1)

    unspentTxs
      .map((transactionElement: TransactionElement) => (ScriptUtil.toAddress(transactionElement.script), transactionElement.value))
      .filter(_._1.isPresent)
      .map(pair => AddressBalance(pair._1.get(), pair._2))
      .toDF()
      .createOrReplaceTempView("accounts")
    val topUnspent: DataFrame = sqlContext.sql("SELECT * FROM accounts ORDER BY balance DESC LIMIT 10000")
    topUnspent.repartition(1).write.mode(SaveMode.Overwrite).csv(outputDir)
  }

  case class TransactionElement(transactionType: Int, blockId: String, trIndexInBlock: Int, transactionHash: String, indexInTransaction: Long, value: Long, script: Array[Byte])

  case class AddressBalance(address: String, balance: Long)

}



