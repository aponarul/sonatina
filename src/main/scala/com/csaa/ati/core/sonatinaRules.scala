
package com.csaa.ati.core
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.Calendar
import java.time.temporal.ChronoUnit

import scala.util.control.Breaks._
import scala.util.{Failure, Success, Try}

@SerialVersionUID(100L)
object sonatinaRules extends Serializable{
  def stringToArray(data: String) = {data.split(",",-1)}

  def between(upper: String, lower: String, value: String): Boolean ={
    var check: Boolean = false
    if(isNotBlank(upper) && isNotBlank(lower) && isNotBlank(value)){
      if(isAllDigits(upper) && isAllDigits(lower) && isAllDigits(value)){
        check = betweenInt(upper.toInt, lower.toInt, value.toInt)
      }
      else if(isNumeric(upper) && isNumeric(lower) && isNumeric(value)){
        check = betweenDBL(upper.toDouble, lower.toDouble, value.toDouble)
      }
    }
    check
  }


  def betweenInt(upper: BigInt, lower: BigInt, value: BigInt): Boolean = {value>=lower && value<=upper}

  def betweenDBL(upper: Double, lower: Double, value: Double): Boolean = {value>=lower && value<=upper}

  def isNotBlank(value: String): Boolean = {!value.equals("")}

  def isAllDigits(x: String): Boolean = { x forall Character.isDigit }

  def validLength(value: String, length: Int): Boolean = {value.length() <= length}

  def validIMEI(value: String): Boolean = {(value.length() == 15) && isAllDigits(value)}

  def validVoucher(value: String): Boolean = {value.length()==7 && isAllDigits(value)}

  def validVIN(value: String): Boolean = {value.length() == 17 && isAlphaNumeric(value)}

  def isAlphaNumeric(value: String): Boolean = {
    var check: Boolean = true
    val valAsChar = value.toCharArray
    val alphaNums = ('A' to 'Z') ++ ('0' to '9')
    breakable{
      for (a <- 0 until value.length()) {
        if (!alphaNums.contains(valAsChar(a))) {
          check = false
          break
        }
      }
    }
    check
  }

  def isNumeric(x: String): Boolean = {scala.util.Try(BigInt(x)).isSuccess }



  def comparatorInt(compare: String, value: BigInt, target: BigInt): Boolean ={
    val comp = compare.toLowerCase().trim()
    val what = comp match {
      case "=" => value == target
      case "<=" => value <= target
      case ">=" => value >= target
      case "<" => value < target
      case ">" => value > target
      case "!=" => value != target
      case _ => false
    }
    what
  }

  def comparatorString(value: String, target: String): Boolean = {value.equalsIgnoreCase(target)}

  def dateValidator(value: String, format: String): Boolean = {
    var date = value.trim()
    var datePattern = format.trim()
    var formatter: SimpleDateFormat = new SimpleDateFormat(datePattern)
    var dateCheck = Try(formatter.parse(date))
    val valid = dateCheck match{
      case Success(dateCheck) => true
      case Failure(dateCheck) => false
    }
    valid
  }

  def validOctoTimeStamp(value: String): Boolean = { timeStampFormat(value) && validOctoTimeStampRange(value)}

  def validOctoTimeStampRange(value: String): Boolean ={
    var date = value.trim()
    var formatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss")
    var dateToCheck = formatter.parse(date)
    val oldestDate = formatter.parse("2013-01-01 00:00:00.000")
    val today = Calendar.getInstance().getTime()
    (today.compareTo(dateToCheck) == 1) && (dateToCheck.compareTo(oldestDate) == 1)
  }

  def timeStampFormat(value: String): Boolean = {
    dateValidator(value, "yyyy-MM-dd HH:mm:ss.sss")
  }
  //  var date = value.trim()
  //  var datePattern = "yyyy-MM-dd HH:mm:ss.sss"
  //  //var datePattern = "m/dd/yyy H:MM aa"
  //  var formatter: SimpleDateFormat = new SimpleDateFormat(datePattern)
  //  var dateCheck = Try(formatter.parse(date))
  //  val valid = dateCheck match{
  //    case Success(dateCheck) => true
  //    case Failure(dateCheck) => false
  //  }
  //  valid
  //}

  def journeyStartDateFormat(value: String): Boolean = {dateValidator(value, "yyyy-MM-dd-HH-mm-ss")}

  def veriskStartDateValid(value: String): Boolean = {
    val today = LocalDate.now()
    val dateObjects = value.split(" ")(0).split("/")
    val fileDate = LocalDate.of(dateObjects(2).toInt, dateObjects(0).toInt, dateObjects(1).toInt)
    //val compareDate = today.plus(-3, ChronoUnit.MONTHS)
    val compareDate = today.plus(-15, ChronoUnit.WEEKS)
    compareDate.isBefore(fileDate) || compareDate.equals(fileDate)
  }
  //def journeyStartDateFormat(value: String): Boolean = {
  //  var date = value.trim()
  //  var datePattern = "yyyy-MM-dd-HH-mm-ss"
  //  var formatter: SimpleDateFormat = new SimpleDateFormat(datePattern)
  //  var dateCheck = Try(formatter.parse(date))
  //  val valid = dateCheck match{
  //    case Success(dateCheck) => true
  //    case Failure(dateCheck) => false
  //  }
  //  valid
  //}

  def ifEqOrThenEqOrInt(firstValue: Int = 0, firstCheck: Int = 0, secondCheck: Int = 0, secondValue: Int = 0, thirdCheck: Int = 0, fourthCheck: Int = 0): Boolean ={
    if((firstValue==firstCheck) || (firstValue==secondCheck)){
      if((secondValue==thirdCheck) || (secondValue==fourthCheck))
        return true
    }
    false
  }

  def ifBetweenThenEqualsInt(between: Int, lower: Int, upper: Int, equal: Int, target: Int):Boolean = {
    if(betweenInt(upper,lower,between)){
      if(equal == target){
        return true
      }
    }
    false
  }
}
