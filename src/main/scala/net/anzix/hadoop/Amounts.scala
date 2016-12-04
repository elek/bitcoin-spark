package net.anzix.hadoop

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.zuinnote.hadoop.bitcoin.format._


object Amounts extends Application {

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
      TransactionElement(r._2.getType, blockHash, r._2.getTransactionIdxInBlock, transactionHash, r._2.getIndexInTransaction, r._2.getAmount)
    })

    val unspent = parsed.map(t => (t.transactionHash + "_" + t.indexInTransaction, t))
      .reduceByKey((t1, t2) => {
        var t = if (t1.transactionType == 1) t1 else t2
        t.copy(transactionType = 2)

      }).map(_._2)

    val unspentTxs = unspent.filter(_.transactionType == 1)

    unspentTxs.toDF().createOrReplaceTempView("unspent")
    val topUnspent: DataFrame = sqlContext.sql("SELECT * FROM unspent ORDER BY VALUE desc LIMIT 100")
    topUnspent.repartition(1).write.mode(SaveMode.Overwrite).csv(outputDir)
  }

  case class TransactionElement(transactionType: Int, blockId: String, trIndexInBlock: Int, transactionHash: String, indexInTransaction: Long, value: Long)
}



