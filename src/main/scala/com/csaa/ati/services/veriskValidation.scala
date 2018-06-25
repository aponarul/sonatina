/**
  * Created by gh0ipon on 2018-04-15.
  */
package com.csaa.ati.services

import java.io.{File, _}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkConf, SparkContext}

import com.csaa.ati.core.sonatinaTools._

object veriskValidator {
  val veriskMap = Map("SCORE_DATE" -> 0,"VOUCHER_ID" -> 1,"IG_ENROLLED" -> 2,"FIRST_NAME" -> 3,"LAST_NAME" -> 4, "VIN" -> 5, "IMEI" -> 6, "FIRST_TRIP" -> 7, "START_DATE" -> 8, "LAST_TRIP_DATE" -> 9, "SCORE_PERIOD" -> 10, "VALID_WEEKS" -> 11, "INVALID_WEEKS" -> 12, "WEEKLY_MILEAGE" -> 13, "TOTAL_MILES" -> 14, "SCORE" -> 15, "SPEED_SCORE" -> 16, "BRAKE_SCORE" -> 17, "ACC_SCORE" -> 18, "CORNER_SCORE" -> 19, "ERROR_CODE" -> 20, "LAST_VALID_SCORE_DATE" -> 21, "LAST_VALID_SCORE" -> 22, "JUNCTION_SCORE" -> 23, "PARKING_SCORE" -> 24, "WEEKEND_SCORE" -> 25, "NIGHT_SCORE" -> 26, "LOCATION_SCORE" -> 27, "TIME_SCORE" ->28)

  val sparkConf = new SparkConf().setAppName("sonatinaVeriskValidate").setMaster("local")

  val file = new File("C:\\Users\\gh0ipon\\Desktop\\UBI\\verisk\\20180604_output.txt")
  var track = 0

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(sparkConf)
    //val file = new File(args(0) + "_output.txt")
    val veriskFile = sc.textFile("C:\\Users\\gh0ipon\\Desktop\\UBI\\verisk\\20180604_UBI_Score_CSAA.csv")
    val bw = new BufferedWriter(new FileWriter(file))
    val mapArray = sc.broadcast(veriskMap)
    //val veriskFile = sc.textFile(args(0))
    //val veriskFile = sc.textFile("C:\\Users\\gh0ipon\\Desktop\\UBI\\verisk\\test.csv")

    //val fileName = args(0) + "_UBI_SCORE_CSAA.CSV"
    //val veriskFile = sc.textFile("/ubidata/axway/ubi_verisk_score_summary/" + fileName)
    //val veriskFile = sc.textFile("C:\\Users\\gh0ipon\\Desktop\\UBI\\verisk\\20180423_UBI_Score_CSAA.csv")
    var header = veriskFile.first()
    val veriskFileData = veriskFile.filter(row => row != header)
    val veriskCount = veriskFileData.count().toInt
    var truthArray = new Array[Boolean](veriskCount)
    var checkArrayBuffer = ArrayBuffer[Boolean]()
    bw.write("Total record count is : " + veriskCount)
    val veriskAry = veriskFileData.collect()
    //val veriskArray = veriskFileData.map(x=>x.split(",",-1))

    val veriskArray = veriskFileData.map(x=>stringToArray(x))
    val veriskNestedArray = veriskArray.collect()
    val uniqueIMEI = veriskArray.map(x=> x(veriskMap("IMEI"))).distinct().count().toInt
    if(uniqueIMEI==veriskCount){
      bw.write("\nIMEIs are all unique")
    }
    val countActuarialValid = veriskArray.filter(x=>x(veriskMap("ERROR_CODE")).equals("ACTUARIAL VALID;")).count().toInt
    bw.write("\nCount of Actuarial Valid is : " + countActuarialValid)

    var truth = 0

    var trace:Boolean = false
    veriskAry.foreach{x=>
      trace=veriskFileValidator(x,veriskMap,bw)
      //trace = (trace & hold)
      if(!trace){truth=truth+1}
    }


    if(truth==0){
      bw.write("\nFile is valid")
    }
    else{
      bw.write("\nFile is invalid")
    }
    bw.close()
  }

}
