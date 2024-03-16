package com.mobvista.data.more_offer.exp1.day.v2

import java.text.SimpleDateFormat
import java.util.{Locale, TimeZone}

import org.apache.spark.SparkContext

class DataCheck {

  def check(sc: SparkContext, date: String): Unit = {

    val nginxInputPath = "s3://mob-emr-test/baihai/test/more_offer/train_data_daily_v2/%s".format(date)
    val nginxOutputPath = "s3://mob-emr-test/baihai/test/nginx_mof_check/%s".format(date)

    sc.textFile(nginxInputPath)
      .flatMap { line =>
        val lineSplit = line.trim.split("\002")
        val mofFeaList =  lineSplit.slice(7, 10) ++ lineSplit.slice(340, 349)
        val feaNameList = Array("appidCategory", "app_id", "unit_id", "isMof", "mofType", "parentUnitId", "parentCrtCid", "parentRvTid", "parentEcId", "parentTplGroup", "parentVfMd5", "parentIfMd5")

        feaNameList.zip(mofFeaList).map { case (feaName, feaValue) => (feaName + "_" + feaValue, 1) }
      }
      .reduceByKey { case (cnt1, cnt2) => cnt1 + cnt2 }
      .map { case (feaNameValue, cnt) => "%s\t%d".format(feaNameValue, cnt) }
      .repartition(10)
      .saveAsTextFile(nginxOutputPath)


    val reqInputPath = "s3://mob-emr-test/baihai/m_sys_model/more_offer/exp1/train_data_daily_v2/%s".format(date)
    val reqOutputPath = "s3://mob-emr-test/baihai/test/req_mof_check/%s".format(date)

    sc.textFile(reqInputPath)
      .flatMap { line =>
        val lineSplit = line.trim.split("\002")
        val mofFeaList =  lineSplit.slice(7, 10) ++ lineSplit.slice(340, 349)
        val feaNameList = Array("appidCategory", "app_id", "unit_id", "isMof", "mofType", "parentUnitId", "parentCrtCid", "parentRvTid", "parentEcId", "parentTplGroup", "parentVfMd5", "parentIfMd5")

        feaNameList.zip(mofFeaList).map { case (feaName, feaValue) => (feaName + "_" + feaValue, 1) }
      }
      .reduceByKey { case (cnt1, cnt2) => cnt1 + cnt2 }
      .map { case (feaNameValue, cnt) => "%s\t%d".format(feaNameValue, cnt) }
      .repartition(10)
      .saveAsTextFile(reqOutputPath)


    val reqInputHourPath = "s3://mob-emr-test/baihai/m_sys_model/more_offer/exp1/train_data_hourly_v2/%s".format(date)
    val reqOutputHourPath = "s3://mob-emr-test/baihai/test/req_hour_mof_check/%s".format(date)

    sc.textFile(reqInputHourPath)
      .flatMap { line =>
        val lineSplit = line.trim.split("\002")
        val mofFeaList =  lineSplit.slice(7, 10) ++ lineSplit.slice(340, 349)
        val feaNameList = Array("appidCategory", "app_id", "unit_id", "isMof", "mofType", "parentUnitId", "parentCrtCid", "parentRvTid", "parentEcId", "parentTplGroup", "parentVfMd5", "parentIfMd5")

        feaNameList.zip(mofFeaList).map { case (feaName, feaValue) => (feaName + "_" + feaValue, 1) }
      }
      .reduceByKey { case (cnt1, cnt2) => cnt1 + cnt2 }
      .map { case (feaNameValue, cnt) => "%s\t%d".format(feaNameValue, cnt) }
      .repartition(10)
      .saveAsTextFile(reqOutputHourPath)

    ()
  }

  def test(sc: SparkContext): Unit = {

    val sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val allData = sc.textFile("s3://mob-ad/adn/nginxlog/tracking/2019/08/18/*/20").filter{row =>
        row.contains("idfa=") && row.contains("unit_id=") && row.contains("mof_data=") && row.contains("/openapi/ad/v3")
      }.map{nginx =>
        val line = nginx.trim
        val url = line.split(" ").filter(_.contains("/openapi/ad/v3"))(0).replaceAll("\"", "")
        val urlMap =
          url
            .split("\\?", 2)(1)
            .split("&")
            .map{seg =>
              val segSplit = seg.split("=", 2)
              try{
                (segSplit(0), segSplit(1))
              }catch {
                case e1: ArrayIndexOutOfBoundsException =>
                  ("key", "value")
              }
            }
            .toMap
        val unitId = urlMap("unit_id")
        val mofData = urlMap("mof_data")
        (unitId, mofData)
      }
    val plFixData = allData.filter{case(unitId, mofData) => unitId == "51304"}.repartition(10)
    plFixData.saveAsTextFile("s3://mob-emr-test/baihai/test/pl_fixed_unit")
    val ecFixData = allData.filter{case(unitId, mofData) => unitId == "51309"}.repartition(10)
    ecFixData.saveAsTextFile("s3://mob-emr-test/baihai/test/ec_fixed_unit")
    allData.subtractByKey(plFixData).subtractByKey(ecFixData).repartition(10).saveAsTextFile("s3://mob-emr-test/baihai/test/split_unit")


    ()
  }

}
