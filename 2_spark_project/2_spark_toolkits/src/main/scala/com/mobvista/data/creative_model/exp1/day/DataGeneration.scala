package com.mobvista.data.creative_model.exp1.day

import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.SparkContext

class DataGeneration {

  def generate(sc: SparkContext, date: String): Unit = {

    val pathDateThres  = "20190526"
    val dataDateThres = "20190604"
    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val pathTimeThres = sdf.parse(pathDateThres).getTime
    val dataTimeThres = sdf.parse(dataDateThres).getTime

    val dateTime = sdf.parse(date).getTime
    val allDataPath =
      if(dateTime <= pathTimeThres)
        "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_daily/%s".format(date)
      else
        "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly/%s".format(date)

    val outPath = "s3://mob-emr-test/baihai/m_sys_model/creative_model_exp1/train_data_daily/%s".format(date)

    println("allDataPath: %s".format(allDataPath))
    println("outPath: %s".format(outPath))

    val tplGroupIdx =
      if(dateTime <= dataTimeThres)
        329
      else
        326

    sc.textFile(allDataPath)
      .filter{line =>
        val lineSplit = line.trim.split("\002")
        val adType = lineSplit(19)
        val tplGroup = lineSplit(tplGroupIdx)

        Array("rewarded_video", "interstitial_video").contains(adType) &&
          !Array("null", "none").contains(tplGroup)
      }
      .repartition(100)
      .saveAsTextFile(outPath)


    ()
  }

}
