package com.mobvista.data.trynew.dict_v1.dict_generation

import java.net.URI
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

class DataGeneration {

  def generate(sc: SparkContext, dateHour: String): Unit = {

    val fs = FileSystem.get(new URI("s3://mob-emr-test"), sc.hadoopConfiguration)
    val sdfHour = new SimpleDateFormat("yyyyMMddHH")
    sdfHour.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val timeStamp = sdfHour.parse(dateHour).getTime
    val tryCnt = 10
    val targetDateHour =
      (0 until tryCnt)
        .map{ i => sdfHour.format(new Date(timeStamp - i * 3600 * 1000L)) }
        .filter{ tryDateHour =>
          val tryDate = tryDateHour.substring(0, 8)
          val increPath = "s3://mob-emr-test/baihai/m_sys_model/trynew/dict/campaign_list/increment/%s/%s".format(tryDate, tryDateHour)
          fs.exists(new Path(increPath))
        }(0)
    val targetDate = targetDateHour.substring(0, 8)
    val inputPath = "s3://mob-emr-test/baihai/m_sys_model/trynew/dict/campaign_list/increment/%s/%s".format(targetDate, targetDateHour)
    val manualPkgPath = "s3://mob-emr-test/baihai/m_sys_model/trynew/dict/manual_info/pkg_name_list"
    val manualCamPath = "s3://mob-emr-test/baihai/m_sys_model/trynew/dict/manual_info/campaign_id_list"
    val outPath = "s3://mob-emr-test/baihai/m_sys_model/trynew/dict/merge/%s/%s".format(targetDate, targetDateHour)
    println("inputPath: %s".format(inputPath))
    println("outPath: %s".format(outPath))

    // campaign_id, advertiser_id, package_name, create_time, status
    val allData =
      sc.textFile(inputPath)
        .map{line =>
          val lineSplit = line.trim.split("\t")
          (lineSplit(0), lineSplit(1), lineSplit(2), lineSplit(3).toLong, lineSplit(4))
        }

    allData
      .filter{case(camId, advId, pkgName, time, status) => advId == "903"}
      .map{case(camId, advId, pkgName, time, status) => (pkgName, time)}
      .reduceByKey{case(time1, time2) => math.min(time1, time2)}
      .filter{case(pkgName, time) => timeStamp - time * 1000 <= 24 * 3600 * 1000}
      .union(
        sc.textFile(manualPkgPath)
          .map{pkgName => (pkgName.trim, 1L)}
      )
      .reduceByKey{case(flag1, flag2) => 1L}
      .join(
        allData
          .filter{case(camId, advId, pkgName, time, status) => advId == "903" && status == "1"}
          .map{case(camId, advId, pkgName, time, status) => (pkgName, camId)}
      )
      .map{case(pkgName, (flag, camId)) => (pkgName, camId)}
      .union(
        sc.textFile(manualCamPath)
          .map{camId => (camId.trim, 1)}
          .leftOuterJoin(
            allData.map{case(camId, advId, pkgName, time, status) => (camId, pkgName)}
          )
          .map{case(camId, (flag, pkgOpt)) =>
            val pkgName =
              pkgOpt match {
                case Some(name) => name
                case None => "default_pkg"
              }
            (pkgName, camId)
          }
      )
      .map{case(pkgName, camId) => (pkgName, Set(camId))}
      .reduceByKey{case(camSet1, camSet2) => camSet1 ++ camSet2}
      .map{case(pkgName, camSet) => Array(pkgName, camSet.mkString(",")).mkString("\t")}
      .repartition(1)
      .saveAsTextFile(outPath)

    ()
  }

}
