package com.mobvista.data.creative3.exp1.join_label

import java.net.URI
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

class JoinLabel {

  def join(sc: SparkContext, dateHour: String): Unit = {

    val fs = FileSystem.get(new URI("s3://mob-emr-test"), sc.hadoopConfiguration)

    val sdf = new SimpleDateFormat("yyyyMMddHH")
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val timestamp = sdf.parse(dateHour).getTime
    val timeThres = timestamp / 1000 - 86400

    val installPath = "s3://mob-ad/adn/tracking-v3/install"
    val cpcPath = "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/extract_cpc_label"
    val window = 6
    val (installPathList, cpcPathList) =
    (0 until window).map{i =>
      val labelDateHour = sdf.format(new Date(timestamp + i * 3600 * 1000))
      val labelDate = labelDateHour.substring(0, 8)
      val labelYear = labelDateHour.substring(0, 4)
      val labelMonth = labelDateHour.substring(4, 6)
      val labelDay = labelDateHour.substring(6, 8)
      val labelHour = labelDateHour.substring(8, 10)
      (
        "%s/%s/%s/%s/*/%s".format(installPath, labelYear, labelMonth, labelDay, labelHour),
        "%s/%s/%s".format(cpcPath, labelDate, labelDateHour)
      )
    }.unzip
    val labelInputPath =
      Array(
        installPathList.mkString(","),
        cpcPathList.filter{cpcPath => fs.exists(new Path(cpcPath))}.mkString(",")
      ).mkString(",")

    val date = dateHour.substring(0, 8)
    val featureInputPath = "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/post_process/%s/%s".format(date, dateHour)

    val outputPath = "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly/%s/%s".format(date, dateHour)

    println("labelInputPath: %s".format(labelInputPath))
    println("featureInputPath: %s".format(featureInputPath))
    println("outputPath: %s".format(outputPath))

    sc.textFile(featureInputPath)
      .filter{line =>
        val lineSplit = line.trim.split("\002")
        val ctrLabel = lineSplit(0)
        val cvrLabel = lineSplit(1)
        val time = java.lang.Long.valueOf(lineSplit(2).substring(0, 8), 16)

        ctrLabel != "1" && cvrLabel != "1" && time >= timeThres
      }
      .map{line =>
        val lineSplit = line.trim.split("\002")
        val extra5 = lineSplit(2).split("\\|")(0)
        val camId = lineSplit(20)

        (extra5+"_"+camId, line.trim)
      }
      .leftOuterJoin(
        sc.textFile(labelInputPath)
          .map{line =>
            val lineSplit = line.trim.split("\t")
            val extra5 = lineSplit(27)
            val camId = lineSplit(7)

            (extra5+"_"+camId, 1)
          }
      )
      .map{case(key, (line, labelOpt)) =>

        val label =
          labelOpt match {
            case Some(t) => "1"
            case None => "0"
          }

        val lineSplit = line.split("\002")
        lineSplit(1) = label

        (key, lineSplit.mkString("\002"))
      }
      .reduceByKey{case(line1, line2) => Array(line1, line2).apply(scala.util.Random.nextInt(2))}
      .map{case(key, line) => line}
      .repartition(100)
      .saveAsTextFile(outputPath)

    ()
  }

}
