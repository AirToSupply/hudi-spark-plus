package org.apache.spark.sql.hudi.commands

import com.alibaba.fastjson.parser.Feature
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.JsonToStructs
import org.apache.spark.sql.hudi.sources.BinlogHoodieDataSource
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession, functions => F}
import tech.odes.common.util.Md5Util

import java.util.UUID
import scala.collection.JavaConverters._
import scala.util.Try

case class TableMetaInfo(db: String, table: String, schema: String)

object BinlogSyncHoodieCommand extends Logging {

  private val CONFIG_SOURCE_SHUFFLE_PARALLELISM = "option.source.shuffle.parallelism"
  private val CONFIG_SOURCE_SHUFFLE_PARALLELISM_VAL = "8"

  private val CONFIG_SINK_SHUFFLE_PARALLELISM = "option.sink.shuffle.parallelism"
  private val CONFIG_SINK_SHUFFLE_PARALLELISM_VAL = "2"

  private val CONFIG_KEEP_BINLOG_ENABLE = "option.keepbinlog.enable"
  private val CONFIG_KEEP_BINLOG_ENABLE_VAL = "false"

  private val CONFIG_BINLOG_PATH = "option.binlog.path"

  private val CONFIG_HOODIE_PATH = "option.hoodie.path"

  private val CONFIG_HOODIE_TRANSFORMER_SQL = "hoodie.transformer.sql"

  private val _META_KEY_ = "__meta__"
  private val _KEY_DATABASE_NAME_ = "databaseName"
  private val _KEY_TABLE_NAME_ = "tableName"
  private val _KEY_SCHEMA_ = "schema"
  private val _KEY_ROWS_ = "rows"
  private val _KEY_TIMESTAMP_ = "timestamp"
  private val _KEY_OPERATION_TYPE_ = "type"
  private val _KEY_OPERATION_TYPE_VAL_UPSERT_ = "upsert"
  private val _KEY_OPERATION_TYPE_VAL_DELETE_ = "delete"
  private val _FIELD_VALUE = "value"
  private val _FIELD_DATA_ = "data"

  private val _PLACEHOLDER_DATABASE_NAME = "{db}"
  private val _PLACEHOLDER_TABLE_NAME = "{table}"

  private val _SPARK_CONF_TIMESTAMPFORMAT = "timestampFormat"
  private val _SPARK_CONF_TIMESTAMPFORMAT_VAL = "yyyy-MM-dd'T'HH:mm:ss'['.SSS']['XXX']'"

  private val _TRANSFORMER_SRC_PATTERN = "<SRC>";
  private val _TRANSFORMER_TMP_TABLE = "HOODIE_SRC_TMP_TABLE_";

  def convertStreamDataFrame(_data: Dataset[_]) = {
    if (_data.isStreaming) {
      class ConvertStreamDataFrame[T](encoder: ExpressionEncoder[T]) {

        def toBatch(data: Dataset[_]): Dataset[_] = {
          val resolvedEncoder = encoder.resolveAndBind(
            data.logicalPlan.output,
            data.sparkSession.sessionState.analyzer)
          val rdd = data.queryExecution.toRdd.map(resolvedEncoder.createDeserializer())(encoder.clsTag)
          val ds = data.sparkSession.createDataset(rdd)(encoder)
          ds
        }

      }
      new ConvertStreamDataFrame[Row](_data.asInstanceOf[Dataset[Row]].exprEnc).toBatch(_data)
    } else _data
  }

  def _getInfoFromMeta(record: JSONObject, key: String): String = record.getJSONObject(_META_KEY_).getString(key)

  def databaseName(record: JSONObject): String = _getInfoFromMeta(record, _KEY_DATABASE_NAME_)

  def tableName(record: JSONObject): String = _getInfoFromMeta(record, _KEY_TABLE_NAME_)

  def schemaName(record: JSONObject): String = _getInfoFromMeta(record, _KEY_SCHEMA_)

  def getTableInfoMetaForBatch(batch: RDD[JSONObject]) = batch.map { r =>
    TableMetaInfo(databaseName(r), tableName(r), schemaName(r))
  }.distinct()
   .collect()
   .zipWithIndex
   .toMap
   .keySet

  def validateHoodieTableOptions(hoodieTableConfig: Map[String, String]): Boolean =
    hoodieTableConfig.contains(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()) &&
      hoodieTableConfig.contains(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key()) &&
      hoodieTableConfig.contains(HoodieWriteConfig.TBL_NAME.key())

  def sqlQueryBasedTransformer(spark: SparkSession, hoodieTableDataFrame: DataFrame, query: String): DataFrame = {
    // tmp table name doesn't like dashes
    val tmpTable = _TRANSFORMER_TMP_TABLE.concat(UUID.randomUUID.toString.replace("-", "_"))
    logInfo(s"Registering tmp table : ${tmpTable}")
    hoodieTableDataFrame.registerTempTable(tmpTable)
    val sqlQuery = query.replaceAll(_TRANSFORMER_SRC_PATTERN, tmpTable)
    spark.sql(sqlQuery)
  }

  def sink(spark: SparkSession,
           batch: RDD[JSONObject],
           metadate: Set[TableMetaInfo],
           options: Map[String, String],
           operate: String): Unit = {
    if (batch.isEmpty) {
      return
    }

    import spark.implicits._ 

    // multi table foreach
    metadate.foreach { meta =>

      // acquire hoodie table config
      val dbConfig = options.filterKeys(_.startsWith(s"${meta.db}.${meta.table}"))
      if (dbConfig.isEmpty) {
        logError(s"table [${meta.db}.${meta.table}] , Hoodie Table Config not found!")
        return
      }
      var hoodieTableConfig = dbConfig.map { case (k, v) => (k.stripPrefix(s"${meta.db}.${meta.table}."), v) }

      // validate hoodie table config
      if (!validateHoodieTableOptions(hoodieTableConfig)) {
        logError(s"table [${meta.db}.${meta.table}] , Hoodie Table Config verification failed!")
        return
      }

      // single table batch datat
      val tempRDD = batch.filter(r => databaseName(r) == meta.db && tableName(r) == meta.table).map{ r =>
        r.remove(_META_KEY_)
        r.toString
      }

      if (tempRDD.isEmpty()) {
        logInfo(s"table [${meta.db}.${meta.table}] , rows not found!")
        return
      }

      def _deserializeSchema(json: String): StructType = {
        Try(DataType.fromJson(json)).get match {
          case t: StructType => t
          case _ => throw new RuntimeException(s"Failed parsing StructType: $json")
        }
      }

      // hoodie table path
      val hoodieTablePath = options.getOrElse(HoodieWriteConfig.BASE_PATH.key(), {
        options.get(CONFIG_HOODIE_PATH) match {
          case Some(basePath) =>
            basePath.replace(_PLACEHOLDER_DATABASE_NAME, meta.db).replace(_PLACEHOLDER_TABLE_NAME, meta.table)
          case None =>
            throw new HoodieException(s"table [${meta.db}.${meta.table}] ${HoodieWriteConfig.BASE_PATH.key()} is empty, " +
              s"${CONFIG_HOODIE_PATH} must be required!")
        }
      })
      hoodieTableConfig = hoodieTableConfig.updated(HoodieWriteConfig.BASE_PATH.key(), hoodieTablePath)

      // popluate hoodie table config
      hoodieTableConfig = hoodieTableConfig.updated(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key(),
        hoodieTableConfig.getOrElse(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key(),
          options.getOrElse(CONFIG_SINK_SHUFFLE_PARALLELISM, CONFIG_SINK_SHUFFLE_PARALLELISM_VAL)))
      hoodieTableConfig = hoodieTableConfig.updated(HoodieWriteConfig.DELETE_PARALLELISM_VALUE.key(),
        hoodieTableConfig.getOrElse(HoodieWriteConfig.DELETE_PARALLELISM_VALUE.key(),
          options.getOrElse(CONFIG_SINK_SHUFFLE_PARALLELISM, CONFIG_SINK_SHUFFLE_PARALLELISM_VAL)))
      hoodieTableConfig = hoodieTableConfig.updated(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key(),
        hoodieTableConfig.getOrElse(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key(),
          options.getOrElse(CONFIG_SINK_SHUFFLE_PARALLELISM, CONFIG_SINK_SHUFFLE_PARALLELISM_VAL)))
      hoodieTableConfig = hoodieTableConfig.updated(HoodieWriteConfig.BULKINSERT_PARALLELISM_VALUE.key(),
        hoodieTableConfig.getOrElse(HoodieWriteConfig.BULKINSERT_PARALLELISM_VALUE.key(),
          options.getOrElse(CONFIG_SINK_SHUFFLE_PARALLELISM, CONFIG_SINK_SHUFFLE_PARALLELISM_VAL)))

      // Determine whether to trigger the hoodie table delete operation according to the delete event of binlog
      if (_KEY_OPERATION_TYPE_VAL_DELETE_.equalsIgnoreCase(operate)) {
        logInfo(s"table [${meta.db}.${meta.table}] is ready for deletion!")
        hoodieTableConfig = hoodieTableConfig.updated(
          DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
      }

      // popluate spark sql config
      if (!hoodieTableConfig.contains(_SPARK_CONF_TIMESTAMPFORMAT)) {
        hoodieTableConfig = hoodieTableConfig.updated(_SPARK_CONF_TIMESTAMPFORMAT, _SPARK_CONF_TIMESTAMPFORMAT_VAL)
      }

      val sourceSchema = _deserializeSchema(meta.schema)

      // udf
      val columnFromJsonStrUDF = new Column(JsonToStructs(sourceSchema, hoodieTableConfig, F.col(_FIELD_VALUE).expr, None))

      // table DataFrame for batch
      var hoodieTableDataFrame = spark.createDataset[String](tempRDD)
        .toDF(_FIELD_VALUE)
        .select(columnFromJsonStrUDF.as(_FIELD_DATA_))
        .select(s"${_FIELD_DATA_}.*")

      // transformer
      hoodieTableConfig.get(CONFIG_HOODIE_TRANSFORMER_SQL) match {
        case Some(query) if StringUtils.isNoneEmpty(query) =>
          hoodieTableDataFrame = sqlQueryBasedTransformer(spark, hoodieTableDataFrame, query)
        case _ =>
      }

      // sink hoodie
      hoodieTableDataFrame.write.format("hudi").options(hoodieTableConfig).mode(Append).save(hoodieTablePath)
    }
  }

  def run(_ds: Dataset[Row], options: Map[String, String]): Unit = {
    val processParallelismNum = options.getOrElse(CONFIG_SOURCE_SHUFFLE_PARALLELISM, CONFIG_SOURCE_SHUFFLE_PARALLELISM_VAL).toInt
    var ds = convertStreamDataFrame(_ds).asInstanceOf[Dataset[Row]]
    // repartition
    if (processParallelismNum != ds.rdd.partitions.size) {
      ds = ds.repartition(processParallelismNum)
    }
    // cache
    ds.cache()

    try {
      if (options.getOrElse(CONFIG_KEEP_BINLOG_ENABLE, CONFIG_KEEP_BINLOG_ENABLE_VAL).toBoolean) {
        val originalLogPath = options(CONFIG_BINLOG_PATH)
        ds.write.format(classOf[BinlogHoodieDataSource].getName).mode(SaveMode.Append).save(originalLogPath)
      } else {
        // do cache
        ds.count()
      }

      val spark = ds.sparkSession

      val dataSet = ds.rdd.flatMap { row =>
        val wow = JSON.parseObject(row.getString(0), Feature.OrderedField)
        val rows = wow.remove(_KEY_ROWS_)
        rows.asInstanceOf[JSONArray].asScala.map { r =>
          val record = r.asInstanceOf[JSONObject]
          record.put(_META_KEY_, wow)
          record
        }
      }

      val finalDataSet = dataSet.map { r =>
        val _OPTION_TABLE_IDS = s"${databaseName(r)}.${tableName(r)}.${KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()}"
        val recordkeys = options.getOrElse(_OPTION_TABLE_IDS, {
          throw new HoodieException(s"config [${_OPTION_TABLE_IDS}] must be required!")
        })
        val recordkeyValues = recordkeys.split(",").map{ r.get(_).toString }.mkString("_")
        // gen row record key to mark row`s unqiue key
        val key = Md5Util.md5Hash(s"${databaseName(r)}_${tableName(r)}_${recordkeyValues}")
        (key, r.toString(SerializerFeature.WriteMapNullValue))
      }.groupBy(_._1).
        map { f => f._2.map(m => JSON.parseObject(m._2, Feature.OrderedField)) }.
        map { records =>
        // we get the same record operations, and sort by timestamp, get the last operation
        val items = records.toSeq.sortBy(record => record.getJSONObject(_META_KEY_).getLong(_KEY_TIMESTAMP_))
        items.last
      }

      // get table meta for each batch
      val metadata = getTableInfoMetaForBatch(finalDataSet)

      // upsert record for hoodie table
      val upsertRDD = finalDataSet.filter { _.getJSONObject(_META_KEY_).getString(_KEY_OPERATION_TYPE_) != _KEY_OPERATION_TYPE_VAL_DELETE_}
      sink(spark, upsertRDD, metadata, options, _KEY_OPERATION_TYPE_VAL_UPSERT_)

      // delete record for hoodie table
      val deleteRDD = finalDataSet.filter { _.getJSONObject(_META_KEY_).getString(_KEY_OPERATION_TYPE_) == _KEY_OPERATION_TYPE_VAL_DELETE_ }
      sink(spark, deleteRDD, metadata,options, _KEY_OPERATION_TYPE_VAL_DELETE_)

    } finally {
      // unpersist
      ds.unpersist()
    }
  }
}
