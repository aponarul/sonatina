package com.csaa.ati.services


// ES imports
import java.io.InputStreamReader

import java.io.InputStreamReader
import java.util.concurrent.Semaphore
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

import scala.collection.mutable.ListBuffer
import scala.concurrent._
import scala.util.control.Breaks.{break, breakable}


import com.typesafe.config.ConfigFactory
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


object createIndex {

  def main(args:Array[String]): Unit = {
    val fs = FileSystem.get(new Configuration())
    val config = ConfigFactory.parseReader(new InputStreamReader(fs.open(new Path(args(0)))))
    val sparkConf = new SparkConf().setAppName("OctoJourneyIndexLoad")
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
    val rules = sqlContext.read.json(args(1))
    rules.saveToEs("ubi_ingestion/octo")
  }
}
