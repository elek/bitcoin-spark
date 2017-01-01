package net.anzix.hadoop.hbase

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SQLContext}

object HBaseLoader {
  def blocks(context: SQLContext): DataFrame = {
    val blockCat =
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


    val blocks: DataFrame = context
      .read.options(Map(HBaseTableCatalog.tableCatalog -> blockCat)).format("org.apache.spark.sql.execution.datasources.hbase").load()

    return blocks
  }


  def transactions(context: SQLContext): DataFrame = {
    val transCat =
      s"""{
          |"table":{"namespace":"default", "name":"transaction"},
          |"rowkey":"key",
          |"columns":{
          |"rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
          |"inputNo":{"cf":"t", "col":"is", "type":"int"},
          |"outputNo":{"cf":"t", "col":"os", "type":"int"}
          |}
          |}""".stripMargin

    val blocks: DataFrame = context
      .read.options(Map(HBaseTableCatalog.tableCatalog -> transCat)).format("org.apache.spark.sql.execution.datasources.hbase").load()

    return blocks
  }
}
