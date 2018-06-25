/**
  * Created by gh0ipon on 2018-04-17.
  */
/*
 * Rule Based Data Ingestion Engine. This code will take in a raw text file, score and clean the data and ingest it to hive.
 */

package com.csaa.ati.services

import java.io.InputStreamReader
import java.io.File
import java.text.SimpleDateFormat
import java.time.Instant
import java.util.Date

import scala.util.Try
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark.rdd._
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.typesafe.config._
import org.apache.spark.sql.types._

// ES imports
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._

//HDFS imports
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

//core engine
import com.csaa.ati.core.sonatinaTools._
import com.csaa.ati.core.sonatinaRules._

object octoJourneyIngest {

  //val journeyMap = Map("Journey ID" -> 0, "Timestamp" -> 1, "LATITUDE" -> 2, "LONGITUDE" -> 3, "BEARING" -> 4, "GPS_FIX_STATUS" -> 5, "VEHICLE_SPEED" -> 6, "ENGINE_RPM" -> 7, "MAF" -> 8, "X_AVERAGE" -> 9, "Y_AVERAGE" -> 10, "Z_AVERAGE" -> 11, "Project Identifier" -> 15, "TSP Sequential Journey ID" -> 16, "Device Sequential Journey ID" -> 17, "Journey Start Date" -> 18, "Device Identifier" -> 19, "Vehicle Identifier" -> 20, "Device Identifier Type" -> 21, "Vehicle Identifier Type" -> 22, "fileName" -> 23)

  val scoredSchema = StructType(
    StructField("journeyId", StringType, true) ::
      StructField("date_time", StringType, true) ::
      StructField("latitude", StringType, true) ::
      StructField("longitude", StringType, true) ::
      StructField("bearing", StringType, true) ::
      StructField("gps_fix_status", StringType, true) ::
      StructField("vehicle_speed", StringType, true) ::
      StructField("engine_rpm", StringType, true) ::
      StructField("MAF", StringType, true) ::
      StructField("X_average", StringType, true) ::
      StructField("Y_average", StringType, true) ::
      StructField("Z_average", StringType, true) ::
      StructField("longitutinal_avearage", StringType, true) ::
      StructField("lateral_average", StringType, true) ::
      StructField("vertical_average", StringType, true) ::
      StructField("project_identifier", StringType, true) ::
      StructField("tsp_sequential_journey_id", StringType, true) ::
      StructField("device_sequential_journey_id", StringType, true) ::
      StructField("journey_start_date", StringType, true) ::
      StructField("device_id", StringType, true) ::
      StructField("vehicle_identifier", StringType, true) ::
      StructField("device_id_type", StringType, true) ::
      StructField("vehicle_identifier_type", StringType, true) ::
      StructField("fileName", StringType, true) ::
      StructField("score", IntegerType, true) ::
      StructField("rulesFailed", StringType, true) ::
      StructField("journey_date", StringType, true) :: Nil)

  val journeySchema = StructType(
    StructField("journeyId", StringType, true) ::
      StructField("date_time", StringType, true) ::
      StructField("latitude", DoubleType, true) ::
      StructField("longitude", DoubleType, true) ::
      StructField("bearing", IntegerType, true) ::
      StructField("gps_fix_status", IntegerType, true) ::
      StructField("vehicle_speed", IntegerType, true) ::
      StructField("engine_rpm", IntegerType, true) ::
      StructField("MAF", IntegerType, true) ::
      StructField("X_average", IntegerType, true) ::
      StructField("Y_average", IntegerType, true) ::
      StructField("Z_average", IntegerType, true) ::
      StructField("longitutinal_avearage", IntegerType, true) ::
      StructField("lateral_average", IntegerType, true) ::
      StructField("vertical_average", IntegerType, true) ::
      StructField("project_identifier", StringType, true) ::
      StructField("tsp_sequential_journey_id", StringType, true) ::
      StructField("device_sequential_journey_id", StringType, true) ::
      StructField("journey_start_date", StringType, true) ::
      StructField("device_id", StringType, true) ::
      StructField("vehicle_identifier", StringType, true) ::
      StructField("device_id_type", StringType, true) ::
      StructField("vehicle_identifier_type", StringType, true) ::
      StructField("fileName", StringType, true) ::
      StructField("rulesFailed", StringType, true) ::
      StructField("journey_date", StringType, true) :: Nil)

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val fs = FileSystem.get(new Configuration())
    val config = ConfigFactory.parseReader(new InputStreamReader(fs.open(new Path(args(0)))))
    //val sparkConf = new SparkConf().setAppName("OctoJourneyValidation")
    val sparkConf = new SparkConf().setAppName(ConfigFactory.load(config).getString("rulesEngine.spark.app"))
    //sparkConf.set("es.net.http.auth.user", "ubi_pu")
    //sparkConf.set("es.net.http.auth.pass", "ubi!@#$")
    //sparkConf.set("es.net.ssl", "true")
    //sparkConf.set("es.net.ssl.cert.allow.self.signed", "true")
    //sparkConf.set("es.net.ssl.truststore.location", "file:///usr/jdk64/jdk1.8.0_112/jre/lib/security/cacerts")
    //sparkConf.set("es.net.ssl.truststore.pass", "changeit")
    //sparkConf.set("es.nodes", "esdev.tent.trt.csaa.pri")
    //sparkConf.set("es.port", "443")
    //sparkConf.set("es.ssl_certificate_validation", "false")
    //sparkConf.set("es.http.timeout", "5m")
    //sparkConf.set("pushdown", "true")
    sparkConf.set("es.net.http.auth.user", ConfigFactory.load(config).getString("rulesEngine.elastic.user"))
    sparkConf.set("es.net.http.auth.pass", ConfigFactory.load(config).getString("rulesEngine.elastic.pass"))
    sparkConf.set("es.net.ssl", ConfigFactory.load(config).getString("rulesEngine.elastic.ssl"))
    sparkConf.set("es.net.ssl.cert.allow.self.signed", ConfigFactory.load(config).getString("rulesEngine.elastic.selfSigned"))
    sparkConf.set("es.net.ssl.truststore.location", ConfigFactory.load(config).getString("rulesEngine.elastic.trustLoc"))
    sparkConf.set("es.net.ssl.truststore.pass", ConfigFactory.load(config).getString("rulesEngine.elastic.trustPass"))
    sparkConf.set("es.nodes", ConfigFactory.load(config).getString("rulesEngine.elastic.nodes"))
    sparkConf.set("es.port", ConfigFactory.load(config).getString("rulesEngine.elastic.port"))
    sparkConf.set("es.ssl_certificate_validation", ConfigFactory.load(config).getString("rulesEngine.elastic.certValid"))
    sparkConf.set("es.http.timeout", ConfigFactory.load(config).getString("rulesEngine.elastic.es_http_timeout"))
    sparkConf.set("pushdown", ConfigFactory.load(config).getString("rulesEngine.elastic.pushdown"))

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val count = 1
    val hiveContext = new HiveContext(sc)

    val journeyFile = sc.textFile(args(1))

    //val rulesFromES = sqlContext.read.format("org.elasticsearch.spark.sql").load("ubi_ingest_rules/journey")
    val rulesFromES = sqlContext.read.format("org.elasticsearch.spark.sql").load("ubi_ingestion/octo")
    val rulesDf = rulesFromES.na.fill("")
    val ruleArray = rulesDf.collect()
    val ruleStringArray = ruleArray.map(_.toString)
    val ruleStrArray: Array[Array[String]] = ruleStringArray.map(x => ruleStringProc(x))
    val broadcastRuleArray = sc.broadcast(ruleStrArray)

    //val broadcastSchemaMap = sc.broadcast(journeyMap)

    //val scored = journeyFile.map(y=>ingestEngine(y,broadcastRuleArray.value,broadcastSchemaMap.value))
    val scored = journeyFile.map(y=>ingestEngine(y,broadcastRuleArray.value))
    val goodData = scored.filter(x => x(0).equals("0"))
    val badData = scored.filter(x=> !x(0).equals("0"))

    
    val journeyValid = goodData.map(line => Row(try {line(2) } catch { case _: Throwable => null.asInstanceOf[String]},
      try {line(3)} catch {case _: Throwable => ""}, try {line(4).toDouble} catch {case _: Throwable => null.asInstanceOf[Double]},
      try {line(5).toDouble} catch {case _: Throwable => null.asInstanceOf[Double]}, try {line(6).toInt} catch {case _: Throwable => null.asInstanceOf[Int]},
      try {line(7).toInt} catch {case _: Throwable => null.asInstanceOf[Int]},
      try {line(8).toInt} catch {case _: Throwable => null.asInstanceOf[Int]}, try {line(9).toInt} catch {case _: Throwable => null.asInstanceOf[Int]},
      try {line(10).toInt} catch {case _: Throwable => null.asInstanceOf[Int]}, try {line(11).toInt} catch {case _: Throwable => null.asInstanceOf[Int]},
      try {line(12).toInt} catch {case _: Throwable => null.asInstanceOf[Int]}, try {line(13).toInt} catch {case _: Throwable => null.asInstanceOf[Int]},
      try {line(14).toInt} catch {case _: Throwable => null.asInstanceOf[Int]}, try {line(15).toInt} catch {case _: Throwable => null.asInstanceOf[Int]},
      try {line(16).toInt} catch {case _: Throwable => null.asInstanceOf[Int]}, try {line(17)} catch {case _: Throwable => null.asInstanceOf[String]},
      try {line(18)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(19)} catch {case _: Throwable => null.asInstanceOf[String]},
      try {line(20)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(21)} catch {case _: Throwable => null.asInstanceOf[String]},
      try {line(22)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(23)} catch {case _: Throwable => null.asInstanceOf[String]},
      try {line(24)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(25)} catch {case _: Throwable => null.asInstanceOf[String]},
      try {line(1)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(26)} catch {case _: Throwable => null.asInstanceOf[String]}))


    val journeyInvalid = badData.map(line => Row(try {line(2)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(3)} catch {
      case _: Throwable => null.asInstanceOf[String]}, try {line(4)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(5)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(6)} catch {case _: Throwable => null.asInstanceOf[String]},
      try {line(7)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(8)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(9)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(10)} catch {case _: Throwable => null.asInstanceOf[String]},
      try {line(11)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(12)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(13)} catch {case _: Throwable => null.asInstanceOf[String]},
      try {line(14)} catch {case _: Throwable=>null.asInstanceOf[String]}, try {line(15)} catch {case _: Throwable=>null.asInstanceOf[String]}, try {line(16)} catch {case _: Throwable=>null.asInstanceOf[String]},
      try {line(17)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(18)} catch {case _: Throwable => null.asInstanceOf[String]},
      try {line(19)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(20)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(21)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(22)} catch {
        case _: Throwable => null.asInstanceOf[String]}, try {line(23)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(24)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(25)} catch {case _: Throwable => null.asInstanceOf[String]},
      try {line(0).toInt} catch {case _: Throwable => -1}, try {line(1)} catch {case _: Throwable => null.asInstanceOf[String]}, try {line(26)} catch {case _: Throwable => null.asInstanceOf[String]}))


    //val journeyInvalid = badData.map(line => Row(try {line(2)} catch {case _: Throwable => ""}, try {line(3)} catch {
      //case _: Throwable => ""}, try {line(4)} catch {case _: Throwable => ""}, try {line(5)} catch {case _: Throwable => ""}, try {line(6)} catch {case _: Throwable => ""},
      //try {line(7)} catch {case _: Throwable => ""}, try {line(8)} catch {case _: Throwable => ""}, try {line(9)} catch {case _: Throwable => ""}, try {line(10)} catch {case _: Throwable => ""},
      //try {line(11)} catch {case _: Throwable => ""}, try {line(12)} catch {case _: Throwable => ""}, try {line(13)} catch {case _: Throwable => ""},
      //try {line(14)} catch {case _: Throwable=>""}, try {line(15)} catch {case _: Throwable=>""}, try {line(16)} catch {case _: Throwable=>""},
      //try {line(17)} catch {case _: Throwable => ""}, try {line(18)} catch {case _: Throwable => ""},
      //try {line(19)} catch {case _: Throwable => ""}, try {line(20)} catch {case _: Throwable => ""}, try {line(21)} catch {case _: Throwable => ""}, try {line(22)} catch {
      //  case _: Throwable => ""}, try {line(23)} catch {case _: Throwable => ""}, try {line(24)} catch {case _: Throwable => ""}, try {line(25)} catch {case _: Throwable => ""},
      //try {line(0).toInt} catch {case _: Throwable => -1}, try {line(1)} catch {case _: Throwable => ""}, try {line(26)} catch {case _: Throwable => ""}))


    val journeyClean = hiveContext.createDataFrame(journeyValid, journeySchema)
    val journeyUnclean = hiveContext.createDataFrame(journeyInvalid, scoredSchema)
    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    //insertData(journeyClean, "journey_date", "ubi.octoJourneyValid")
    insertData(journeyClean, "journey_date", ConfigFactory.load(config).getString("rulesEngine.hive.goodTable"))
    //insertData(journeyUnclean, "journey_date", "ubi.octoJourneyInvalid")
    insertData(journeyUnclean, "journey_date", ConfigFactory.load(config).getString("rulesEngine.hive.badTable"))
  }
  private def insertData(journeyData: DataFrame, partition: String, table: String): Unit = {journeyData.write.partitionBy(partition).insertInto(table)}
}









