package com.mobvista.data.creative3.exp1.extract_cpc_label

import org.apache.spark.SparkContext

class ExtractCpcLabel {

  def extract(sc: SparkContext, dateHour: String): Unit = {

    val year = dateHour.substring(0, 4)
    val month = dateHour.substring(4, 6)
    val day = dateHour.substring(6,8)
    val hour = dateHour.substring(8, 10)
    val date = dateHour.substring(0, 8)
    val clickPath = "s3://mob-ad/adn/tracking-v3/click/%s/%s/%s/*/%s".format(year, month, day, hour)
    val preClickPath = "s3://mob-ad/adn/tracking-v3/pre_click/%s/%s/%s/*/%s".format(year, month, day, hour)
    val inputPath = Array(clickPath, preClickPath).mkString(",")
    val outputPath = "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/extract_cpc_label/%s/%s".format(date, dateHour)

    println("clickPath: %s".format(clickPath))
    println("preClickPath: %s".format(preClickPath))
    println("inputPath: %s".format(inputPath))
    println("outputPath: %s".format(outputPath))

    sc.textFile(inputPath)
      .filter{line =>
        val lineSplit = line.trim.split("\t")
        val extBp = lineSplit(89).trim

        try{
          val cpx = extBp.substring(1, extBp.length-1).split(",")(3)
          cpx == "2"
        }catch {
          case e: Exception => false
        }

      }
      .repartition(100)
      .saveAsTextFile(outputPath)

    ()
  }

}
