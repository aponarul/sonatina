/**
  * Created by gh0ipon on 2018-04-16.
  */


import com.csaa.ati.core.sonatinaRules._
import com.csaa.ati.core.sonatinaTools._
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark._
import com.csaa.ati.core.sonatinaRules._
import com.csaa.ati.core.sonatinaTools._
import java.io.{File, _}


import org.apache.spark._

import scala.collection.mutable.ArrayBuffer
import com.csaa.ati.core.sonatinaTools.stringToArray

import com.csaa.ati.services.veriskValidator.{sparkConf, track, veriskMap}

object test {
  def main(args: Array[String]): Unit = {
    println(validOctoTimeStamp("2017-11-16 02:16:45.000"))
    println(validOctoTimeStamp("2012-11-16 02:16:45.000"))
    println(validOctoTimeStamp("2019-11-16 02:16:45.000"))

    println(validOctoTimeStamp("2018-06-10 00:34:04.000"))

    println(validOctoTimeStamp("2019-11-16"))
  }
}
