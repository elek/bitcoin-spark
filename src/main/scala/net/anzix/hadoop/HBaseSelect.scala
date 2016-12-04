package net.anzix.hadoop

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}

object HBaseSelect extends Application {
  override def run(session: SparkSession, inputPattern: String, outputDir: String): Unit = {
    val cat =
      s"""{
          |"table":{"namespace":"default", "name":"block"},
          |"rowkey":"key",
          |"columns":{
          |"rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
          |"transactionCounter":{"cf":"b", "col":"t", "type":"long"},
          |"transactionDate":{"cf":"b", "col":"d", "type":"int"},
          |"file":{"cf":"b", "col":"f", "type":"string"},
          |"size":{"cf":"b", "col":"s", "type":"int"}
          |}
          |}""".stripMargin

    val load: DataFrame = session.sqlContext
      .read.options(Map(HBaseTableCatalog.tableCatalog -> cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    println(load)
    import session.sqlContext.implicits._
    load.filter($"size" > 0).sort($"size").show(10)
  }
}
