package net.anzix.hadoop.blockfiles

import java.text.SimpleDateFormat
import java.util.Date

import net.anzix.hadoop.Application
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.zuinnote.hadoop.bitcoin.format._

import scala.collection.JavaConversions._

object TransactionAmount extends Application {

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH::mm");

  def run(session: SparkSession, inputPattern: String, outputDir: String) = {

    val sqlContext: SQLContext = session.sqlContext
    val sparkContext = sqlContext.sparkContext
    import sqlContext.implicits._

    val conf = new org.apache.hadoop.mapred.JobConf()
    FileInputFormat.addInputPath(conf, new Path(inputPattern))
    val rdd: RDD[(BytesWritable, BitcoinBlock)] = sparkContext.hadoopRDD(conf, classOf[BitcoinBlockFileInputFormat], classOf[BytesWritable], classOf[BitcoinBlock], 2)

    val from = new Date().getTime() - 60l * 60 * 24 * 30

    val blocks: RDD[BlockWithValue] = rdd
      .map(a => a._2)
      //.filter(a => a.getTime > from)
      .map(block => {
      val sum = block.getTransactions().toList.map(trans => trans.getListOfOutputs.toList.map(output => output.getValue).sum).sum
      val d = new Date();
      d.setTime(1000l * block.getTime)
      BlockWithValue(block.getTime, sdf.format(d), BitcoinUtil.convertByteArrayToHexString(BitcoinUtil.getBlockHash(block)), sum, block.getTransactions.size())
    })

    blocks.toDF().repartition(1).sortWithinPartitions("t").write.mode(SaveMode.Overwrite).csv(outputDir)

  }
}

case class BlockWithValue(t: Int, time: String, hash: String, value: Long, no: Int)

