package org.apache.spark.sql.hudi.sources

import org.apache.hadoop.fs.Path
import org.apache.hudi.HoodieStreamingSink
import org.apache.spark.sql.hudi.commands.BinlogSyncHoodieCommand
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.streaming.OutputMode

class BinlogHoodieSink(sqlContext: SQLContext,
                       options: Map[String, String],
                       partitionColumns: Seq[String],
                       outputMode: OutputMode) extends HoodieStreamingSink(
    sqlContext: SQLContext,
    options: Map[String, String],
    partitionColumns: Seq[String],
    outputMode: OutputMode) {

    override def addBatch(batchId: Long, data: DataFrame): Unit = {
        BinlogSyncHoodieCommand.run(data, options)
    }

}
