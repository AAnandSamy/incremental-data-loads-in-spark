package com.org.data.loader

import scala.collection.JavaConversions._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

case class HWDs(hwm_key: String, hwm_value: Long)

object
IncrementalLoads extends App with Logging {
  try {
    // load and set application conf
    val apc = ConfigFactory.load
    val waterMarkCol = apc.getString("jdbc.srcTbl.highWaterMarkColumnName").toLowerCase
    val watermarkDir = apc.getString("jdbc.srcTbl.highWaterMarkDir")
    var jdbcOps = readWriteOptions(apc.getConfig("jdbc.srcTbl.options"))

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val fileExists = fs.exists(new Path(watermarkDir))
    val wmks = waterMarkCol.split("\\.").toList

    var mkv = Row("0")
    if (fileExists) {
      val chkDf = spark.read.parquet(watermarkDir).as[HWDs]
      mkv = chkDf.filter($"hwm_key" === waterMarkCol)
        .select("hwm_value")
        .sort(col("hwm_value").desc).head
    }
    jdbcOps += ("dbtable" -> s"${jdbcOps.get("dbtable").mkString.format(deserialize2SrcHWV(mkv, waterMarkCol))}")
    val srcDf = spark.read.format("jdbc").options(jdbcOps).load()
    srcDf.show(false) // <-- write into target system
    mkv = srcDf.select(wmks.get(2))
      .sort(col(wmks.get(2)).desc).head
    val hwDs = Seq(HWDs(waterMarkCol, serializable2HWV(mkv, waterMarkCol))).toDF()
    hwDs.write.mode(SaveMode.Append).parquet(watermarkDir)
  }


  catch {
    case e: Exception => {
      logError(s"${e.printStackTrace()}")
    }
  }


  /**
   * Serialize the value as per Water mark value
   *
   * @param rv
   * @param fwk
   * @return
   */
  def serializable2HWV(rv: Row, fwk: String): Long = {
    val dty = fwk.split("\\.").last
    if (dty.equalsIgnoreCase("Timestamp")) {
      rv.mkString.slice(0, 23).replaceAll("[^0-9]", "").toLong
    } else {
      rv.getLong(0)
    }
  }

  /**
   * Deserialize the water mark as per target system.
   *
   * @param rv
   * @param fwk
   * @return
   */
  def deserialize2SrcHWV(rv: Row, fwk: String): String = {
    val dty = fwk.split("\\.").last
    val wv = rv.mkString
    if (dty.equalsIgnoreCase("timestamp")) {
      if (wv.length == 16 || wv.length == 17)
        s"${wv.slice(0, 4)}-${wv.slice(4, 6)}-${wv.slice(6, 8)} ${wv.slice(8, 10)}:${wv.slice(10, 12)}:${wv.slice(12, 14)}.${wv.slice(14, 17)}"
      else if (wv.length == 14)
        s"${wv.slice(0, 4)}-${wv.slice(4, 6)}-${wv.slice(6, 8)} ${wv.slice(8, 10)}:${wv.slice(10, 12)}:${wv.slice(12, 14)}"
      else "2000-01-01 00:00:00.000"
    } else if (wv != null || wv.isEmpty) {
      rv.mkString
    } else {
      "0"
    }
  }

  /**
   * This this is used to convert the config objects to map
   * @param conf:TypeSafeObject
   * @return Map
   */
  def readWriteOptions(conf: Config): Map[String, String] = {
    var opts = Map.empty[String, String]
    for (kvPairs <- conf.entrySet()) {
      opts += (kvPairs.getKey.mkString -> kvPairs.getValue.unwrapped().toString)
    }
    opts
  }
}
