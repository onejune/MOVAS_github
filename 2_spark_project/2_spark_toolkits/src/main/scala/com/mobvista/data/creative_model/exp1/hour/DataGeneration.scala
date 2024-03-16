package com.mobvista.data.creative_model.exp1.hour

import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.SparkContext

class DataGeneration {

  def generate(sc: SparkContext, dateHour: String): Unit = {


    val date = dateHour.substring(0, 8)
    val allDataPath = "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly/%s/%s".format(date, dateHour)
    val outPath = "s3://mob-emr-test/baihai/m_sys_model/creative_model_exp1/train_data_hourly/%s/%s".format(date, dateHour)

    println("allDataPath: %s".format(allDataPath))
    println("outPath: %s".format(outPath))

    sc.textFile(allDataPath)
      .filter{line =>
        val lineSplit = line.trim.split("\002")
        val adType = lineSplit(19)
        val tplGroup = lineSplit(326)

        Array("rewarded_video", "interstitial_video").contains(adType) &&
          !Array("null", "none").contains(tplGroup)
      }
      .repartition(50)
      .saveAsTextFile(outPath)

    ()
  }

}
