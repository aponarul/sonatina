
package com.csaa.ati.core
import java.io.BufferedWriter

import com.csaa.ati.core.sonatinaRules._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by gh0ipon on 2018-04-16.
  */


object sonatinaTools {
  def stringToArray(data: String) = {data.split(",",-1)}

  def ruleStringProc(rule: String) = { rule.substring(1,rule.length-1).split(",",-1)}

  //def ingestEngine(Data: String, ruleSet: Array[Array[String]], schemaMap: Map[String, Int]): Array[String] = {
  def ingestEngine(Data: String, ruleSet: Array[Array[String]]): Array[String] = {
    val ruleMap = Map("dataSet" -> 0, "enable" -> 1, "errorDescription" -> 2, "fieldOne" -> 3, "operatorOne" -> 4, "valueOne" -> 5, "valueTwo" -> 6, "ruleType" -> 7, "severity" -> 8)
    //val ruleMap = Map("dataSet" -> 0, "errorDescription" -> 1, "fieldOne" -> 2, "operatorOne" -> 3, "ruleType" -> 4, "severity" -> 5, "valueOne" -> 6, "valueTwo" -> 7)
    var score: Int = 0
    var rulesFailed, scoredData = ""
    var scoredRecord: Array[String] = Array[String]()
    var check:Boolean = true
    var ruleType, resOperator1, resultField, resultValue1, resultValue2:String = ""
    var recordField = stringToArray(Data)
    var schemaMap:Map[String,Int] = Map()
    if(recordField(9).equals("") && recordField(10).equals("") && recordField(11).equals("")){
      schemaMap = Map("Journey ID" -> 0, "Timestamp" -> 1, "LATITUDE" -> 2, "LONGITUDE" -> 3, "BEARING" -> 4, "GPS_FIX_STATUS" -> 5, "VEHICLE_SPEED" -> 6, "ENGINE_RPM" -> 7, "MAF" -> 8, "X_AVERAGE" -> 12, "Y_AVERAGE" -> 13, "Z_AVERAGE" -> 14, "Project Identifier" -> 15, "TSP Sequential Journey ID" -> 16, "Device Sequential Journey ID" -> 17, "Journey Start Date" -> 18, "Device Identifier" -> 19, "Vehicle Identifier" -> 20, "Device Identifier Type" -> 21, "Vehicle Identifier Type" -> 22, "fileName" -> 23)
    }
    else{
      schemaMap = Map("Journey ID" -> 0, "Timestamp" -> 1, "LATITUDE" -> 2, "LONGITUDE" -> 3, "BEARING" -> 4, "GPS_FIX_STATUS" -> 5, "VEHICLE_SPEED" -> 6, "ENGINE_RPM" -> 7, "MAF" -> 8, "X_AVERAGE" -> 9, "Y_AVERAGE" -> 10, "Z_AVERAGE" -> 11, "Project Identifier" -> 15, "TSP Sequential Journey ID" -> 16, "Device Sequential Journey ID" -> 17, "Journey Start Date" -> 18, "Device Identifier" -> 19, "Vehicle Identifier" -> 20, "Device Identifier Type" -> 21, "Vehicle Identifier Type" -> 22, "fileName" -> 23)
    }
    //val journeyMap = Map("Journey ID" -> 0, "Timestamp" -> 1, "LATITUDE" -> 2, "LONGITUDE" -> 3, "BEARING" -> 4, "GPS_FIX_STATUS" -> 5, "VEHICLE_SPEED" -> 6, "ENGINE_RPM" -> 7, "MAF" -> 8, "X_AVERAGE" -> 9, "Y_AVERAGE" -> 10, "Z_AVERAGE" -> 11, "Project Identifier" -> 15, "TSP Sequential Journey ID" -> 16, "Device Sequential Journey ID" -> 17, "Journey Start Date" -> 18, "Device Identifier" -> 19, "Vehicle Identifier" -> 20, "Device Identifier Type" -> 21, "Vehicle Identifier Type" -> 22, "fileName" -> 23)
    var ruleField: Array[String] = new Array[String](ruleMap.size)
    for (r <- ruleSet) {
      ruleField = r
      resultField = try{recordField(schemaMap(ruleField(ruleMap("fieldOne"))))} catch {case _: Throwable =>""}
      resultValue1 = try{ruleField(ruleMap("valueOne"))} catch {case _: Throwable =>""}
      resultValue2 = try{ruleField(ruleMap("valueTwo"))} catch {case _: Throwable => ""}
      resOperator1 = try{ruleField(ruleMap("operatorOne"))} catch {case _: Throwable => ""}
      ruleType = try{ruleField(ruleMap("ruleType"))} catch {case _: Throwable => ""}
      check = ruleType.toUpperCase() match {
        case "VALIDLENGTH" => sonatinaRules.validLength(resultField, try{resultValue1.toInt} catch {case _: Throwable => -1})
        case "ISBLANK" => sonatinaRules.isNotBlank(resultField.trim)
        case "BETWEEN_INT" => sonatinaRules.betweenInt(try{BigInt(resultValue2)} catch {case _: Throwable => -1}, try{BigInt(resultValue1)}  catch {case _: Throwable => -1}, try{BigInt(resultField)} catch {case _: Throwable => -999})
        case "BETWEEN_FL" => sonatinaRules.betweenDBL(try{resultValue2.toDouble} catch {case _: Throwable => -1.0}, try{resultValue1.toDouble} catch {case _: Throwable => -1.0}, try{resultField.toDouble} catch {case _: Throwable => -999.0})
        case "COMPARATOR_STRING" => sonatinaRules.comparatorString(resultField,resultValue1)
        case "COMPARATOR_NUM" => sonatinaRules.comparatorInt(resOperator1, try{BigInt(resultField)} catch { case _: Throwable => -1}, try{BigInt(resultValue1)} catch {case _: Throwable => -999})
        case "JOURNEYDATEFORMAT" => sonatinaRules.journeyStartDateFormat(resultField)
        //case "TIMESTAMPFORMAT" => sonatinaRules.timeStampFormat(resultField)
        case "TIMESTAMPFORMAT" => sonatinaRules.validOctoTimeStamp(resultField)
        case "VALIDVIN" => sonatinaRules.validVIN(resultField.trim)
        case "VALIDIMEI" => sonatinaRules.validIMEI(resultField.trim)
        case "" => false
        case _ => false
      }
      if(check!=true){
        rulesFailed += ruleField(ruleMap("errorDescription")) + "; "
        if(ruleField(ruleMap("severity")).equals("High")) score += 1
      }
    }
    //if(!recordField(12).equals("") || !recordField(13).equals("") || !recordField(14).equals("")){
    //  score +=1
    //  rulesFailed += "expected empty columns are populated; "
    //}
    scoredData = score.toString + "," + rulesFailed + "," + Data
    scoredRecord = scoredData.split(",", -1)
    scoredRecord
  }



  def veriskFileValidator(Data: String, schemaMap: Map[String, Int], bw: BufferedWriter): Boolean = {
    //Map("SCORE_DATE" -> 0,"VOUCHER_ID" -> 1,"IG_ENROLLED" -> 2,"FIRST_NAME" -> 3,"LAST_NAME" -> 4, "VIN" -> 5, "IMEI" -> 6,
    // "FIRST_TRIP" -> 7, "START_DATE" -> 8, "LAST_TRIP_DATE" -> 9, "SCORE_PERIOD" -> 10, "VALID_WEEKS" -> 11, "INVALID_WEEKS" -> 12, "WEEKLY_MILEAGE" -> 13, "TOTAL_MILES" -> 14,
    // "SCORE" -> 15, "SPEED_SCORE" -> 16, "BRAKE_SCORE" -> 17, "ACC_SCORE" -> 18, "CORNER_SCORE" -> 19, "ERROR_CODE" -> 20,
    // "LAST_VALID_SCORE_DATE" -> 21, "LAST_VALID_SCORE" -> 22, "JUNCTION_SCORE" -> 23, "PARKING_SCORE" -> 24, "WEEKEND_SCORE" -> 25, "NIGHT_SCORE" -> 26, "LOCATION_SCORE" -> 27, "TIME_SCORE" ->28)
    var ruleType, resOperator1, resultField, resultValue1, resultValue2, rulesFailed: String = ""
    var recordField = Data.split(",", -1)
    var check: Boolean = true
    var lastScoreNotBlank, scoreNotBlank: Boolean = false
    val truthArray = new ArrayBuffer[Boolean]()

    check = validIMEI(recordField(schemaMap("IMEI")))

    if (!check) {
      rulesFailed += "invalid IMEI; "
    }
    truthArray += check



    scoreNotBlank = isNotBlank(recordField(schemaMap("SCORE")))
    if (scoreNotBlank) {
      check = between("100", "10", recordField(schemaMap("SCORE")))

      truthArray += check
      if (!check && recordField(schemaMap("SCORE")).toInt < 9) {
        rulesFailed += "SCORE might not be valid; "
      }
    }

    if (scoreNotBlank) {
      if(isNotBlank(recordField(schemaMap("START_DATE")))){
        check = veriskStartDateValid(recordField(schemaMap("START_DATE")))

        if (!check) {
          rulesFailed += "invalid START_DATE; "
        }
      }

      truthArray += check

      check = between("14", "2", recordField(schemaMap("SCORE_PERIOD")))
      if (!check) {
        rulesFailed += "invalid SCORE_PERIOD; "
      }
      truthArray += check

      check = between("12", "2", recordField(schemaMap("VALID_WEEKS")))

      if (!check) {
        rulesFailed += "invalid number of VALID_WEEKS; "
      }
      truthArray += check

      check = between("2", "0", recordField(schemaMap("INVALID_WEEKS")))


      if (!check) {
        rulesFailed += "invalid number of INVALID_WEEKS; "
      }
      truthArray += check
      check = between("100", "0", recordField(schemaMap("BRAKE_SCORE")))


      if (!check) {
        rulesFailed += "BRAKE_SCORE invalid; "
      }
      truthArray += check

      check = between("100", "0", recordField(schemaMap("ACC_SCORE")))

      if (!check) {
        rulesFailed += "ACC_SCORE invalid; "
      }
      truthArray += check

      check = between("100", "0", recordField(schemaMap("CORNER_SCORE")))
      if (!check) {
        rulesFailed += "CORNER_SCORE invalid; "
      }
      truthArray += check

      check = between("100", "0", recordField(schemaMap("CORNER_SCORE")))

      if (!check) {
        rulesFailed += "CORNER_SCORE invalid; "
      }
      truthArray += check

      check = between("100", "0", recordField(schemaMap("JUNCTION_SCORE"))) || !isNotBlank(recordField(schemaMap("JUNCTION_SCORE")))
      if (!check) {
        rulesFailed += "JUNCTION_SCORE invalid; "
      }
      truthArray += check

      check = between("100", "0", recordField(schemaMap("PARKING_SCORE"))) || !isNotBlank(recordField(schemaMap("PARKING_SCORE")))
      if (!check) {
        rulesFailed += "PARKING_SCORE invalid; "
      }
      truthArray += check

      check = between("100", "0", recordField(schemaMap("WEEKEND_SCORE"))) || !isNotBlank(recordField(schemaMap("WEEKEND_SCORE")))
      if (!check) {
        rulesFailed += "WEEKEND_SCORE invalid; "
      }
      truthArray += check

      check = between("100", "0", recordField(schemaMap("NIGHT_SCORE"))) || !isNotBlank(recordField(schemaMap("NIGHT_SCORE")))
      if (!check) {
        rulesFailed += "NIGHT_SCORE invalid; "
      }
      truthArray += check

      check = between("100", "0", recordField(schemaMap("LOCATION_SCORE"))) || !isNotBlank(recordField(schemaMap("LOCATION_SCORE")))
      if (!check) {
        rulesFailed += "LOCATION_SCORE invalid; "
      }
      truthArray += check

      check = between("100", "0", recordField(schemaMap("TIME_SCORE"))) || !isNotBlank(recordField(schemaMap("TIME_SCORE")))
      if (!check) {
        rulesFailed += "TIME_SCORE invalid; "
      }
      truthArray += check
    }
    lastScoreNotBlank = isNotBlank(recordField(schemaMap("LAST_VALID_SCORE")))
    if (lastScoreNotBlank) {
      check = between("100", "0", recordField(schemaMap("LAST_VALID_SCORE")))
      truthArray += check
      if (!check && (try{recordField(schemaMap("LAST_VALID_SCORE")).toInt} catch {case _: Throwable => 0}) < 9) {
        rulesFailed += "LAST_VALID_SCORE might not be valid; "
      }
    }
    //var truth:Boolean = true
    var output = truthArray.toArray.reduceLeft(_ & _)
    if (!output) {
      bw.write("\nRecord: " + Data + "\nFailed rules: " + rulesFailed + "\nStatus: " + output)
    }
    //bw.write("\nRecord: " + Data + "\n Failed rules: " + rulesFailed + "\n Status: " + truthArray.toArray.reduceLeft(_ & _))
    output
  }

}
