/**
  * Created by gh0ipon on 2018-04-16.
  */

package com.csaa.ati

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

import org.scalatest.FunSuite

class test extends FunSuite {
  test("check between integer function"){
    assert(betweenInt(15,0,7)==true)
    assert(betweenInt(8934,7654,1000232)==false)
  }

}
