package org.apache.spark.sql.hudi.sources

import org.apache.hudi.{DataSourceWriteOptions, DefaultSource, HoodieWriterUtils}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/**
 * BinlogHoodieDataSource is an enhanced extension of the Hoodie Spark Datasource.
 *
 * Helps with ingesting streaming data about cdc data (e.g. mysql binlog) into hoodie table.
 *
 * @author Town
 */
class BinlogHoodieDataSource extends DefaultSource {

    override def createRelation(sqlContext: SQLContext,
                                mode: SaveMode,
                                parameters: Map[String, String],
                                data: DataFrame): BaseRelation = BinlogHoodieRelation(sqlContext, data)

    override def createSink(sqlContext: SQLContext,
                            optParams: Map[String, String],
                            partitionColumns: Seq[String],
                            outputMode: OutputMode): Sink = {

        val parameters = HoodieWriterUtils.parametersWithWriteDefaults(optParams)
        val translatedOptions = DataSourceWriteOptions.translateSqlOptions(parameters)

        new BinlogHoodieSink(sqlContext, translatedOptions, partitionColumns, outputMode)
    }

    override def shortName(): String = "binlog-hudi"
}

case class BinlogHoodieRelation(override val sqlContext: SQLContext, data: DataFrame) extends BaseRelation {
    override def schema: StructType = data.schema
}
