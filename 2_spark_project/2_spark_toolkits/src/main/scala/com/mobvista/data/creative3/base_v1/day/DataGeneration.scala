package com.mobvista.data.creative3.base_v1.day

import java.net.URI
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.TimeZone

import com.alibaba.fastjson.JSON
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.HashMap

class DataGeneration {

  def generate(sc: SparkContext, date: String): Unit = {

    val fs = FileSystem.get(new URI("s3://mob-emr-test"), sc.hadoopConfiguration)

    val sdfDate = new SimpleDateFormat("yyyyMMdd")
    sdfDate.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val sdfHour = new SimpleDateFormat("yyyyMMddHH")
    sdfHour.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val tryCnt = 20
    val (creativeMd5PathSeq, camThirdPartyPathSeq) =
      (0 until tryCnt)
        .map{i =>
          val targetTime = System.currentTimeMillis() - i * 3600 * 1000
          val targetDate = sdfDate.format(new Date(targetTime))
          val targetDateHour = sdfHour.format(new Date(targetTime))
          ("s3://mob-emr-test/wanjun/m_sys_model/creative_optimize/offline_data/history_data/%s/%s/creative_md5.dat".format(targetDate, targetDateHour),
            "s3://mob-emr-test/wanjun/m_sys_model/offline_data/history_data/%s/%s/campaign_third_party.dat".format(targetDate, targetDateHour))
        }
        .unzip
    val creativeMd5Path = creativeMd5PathSeq.filter{path => fs.exists(new Path(path))}.head
    val camThirdPartyPath = camThirdPartyPathSeq.filter{path => fs.exists(new Path(path))}.head

    val year = date.substring(0, 4)
    val month = date.substring(4, 6)
    val day = date.substring(6, 8)
    val imprePath = "s3://mob-ad/adn/tracking-v3/impression/%s/%s/%s/".format(year, month, day)

    val realtimePath = "s3://mob-emr-test/guangxue/new_base/instance/%s".format(date)
//    val realtimePath = "s3://mob-emr-test/guangxue/real_time_mwho/%s".format(date)

    val outPath = "s3://mob-emr-test/baihai/m_sys_model/creative3_v1/train_data_daily/%s".format(date)

    println("date: %s".format(date))
    println("creativeMd5Path: %s".format(creativeMd5Path))
    println("camThirdPartyPath: %s".format(camThirdPartyPath))
    println("imprePath: %s".format(imprePath))
    println("realtimePath: %s".format(realtimePath))
    println("outPath: %s".format(outPath))

    val creaMd5MapBC =
      sc.broadcast(
        sc.textFile(creativeMd5Path, 10)
          .map{line =>
            val lineSplit = line.trim.split("\t")
            val creativeId = lineSplit(0)
            val md5 = lineSplit(1).split("\\|")(0)
            (creativeId, md5)
          }
          .collectAsMap()
      )
    val camThirdMapBC =
      sc.broadcast(
        sc.textFile(camThirdPartyPath)
          .map{line =>
            val lineSplit = line.trim.split("\t")
            val camId = lineSplit(0)
            val thirdParty = lineSplit(1)

            (camId, thirdParty)
          }
          .collectAsMap()
      )


    val baseData =
      sc.textFile(realtimePath)
        .map{line =>
          val lineSplit = line.trim.split("\002")
          val extra5 = lineSplit(2).split("\\|")(0)
          val camId = lineSplit(20)
          val key = extra5 + "_" + camId

          (key, line.trim)
        }
        .reduceByKey{case(line1, line2) => Array(line1, line2).apply((new util.Random).nextInt(2))}

    val creativeData =
      sc.textFile(imprePath)
        .filter{line =>
          try{
            val lineSplit = line.trim.split("\t")
            val adType = lineSplit(10)
            val extNativeVideo = lineSplit(65)
            val strategy = lineSplit(25)
            val extAlgo = lineSplit(92)

            //              !adType.contains("appwall") &&
            !(adType == "native" && extNativeVideo == "3") &&
              strategy.contains("MNormalAlphaModelRanker") &&
              !strategy.contains("default") &&
              extAlgo.split(";")(0).split(",").length >= 18
          }catch {
            case e1: ArrayIndexOutOfBoundsException =>
              false
          }
        }
        .map{line =>
          val lineSplit = line.trim.split("\t")

          val extra5 = lineSplit(27)
          val camId = lineSplit(7)
          val key = extra5 + "_" + camId

          val extAlgo = lineSplit(92).split(";")(0).split(",")
          val (templateGroup, videoTemplate, endcardTemplate, minicardTemplate) = (extAlgo(14), extAlgo(15), extAlgo(16), extAlgo(17))

          val allowPlayable =
            if(Array("0", "1").contains(extAlgo(10)))
              extAlgo(10)
            else
              "null"

          val ext_playable = lineSplit(81)
          val extPlayable =
            if(Array("0", "1", "2", "3").contains(extAlgo(12)))
              extAlgo(12)
            else if(Array("0", "1", "2", "3").contains(ext_playable))
              ext_playable
            else if(ext_playable.length == 5)
              ext_playable(2)
            else
              "null"

          val videoEndType =
            if(Array("2", "6").contains(extAlgo(11)))
              extAlgo(11)
            else if(ext_playable.length == 5)
              ext_playable(4)
            else
              "null"

          val extStats = lineSplit(63)
          val typeMap =
            try{
              JSON
                .parseObject(extStats)
                .getJSONObject("value")
                .getJSONObject("creative")
                .getJSONArray("group_list")
                .toArray
                .map{jStr =>
                  val js = JSON.parseObject(jStr.toString)
                  (js.getString("creative_type"), js.getString("adn_creative_id"))
                }
                .toMap
            }catch {
              case e: Exception =>
                println(e.getMessage)
                new HashMap[String,String]()
            }
          val (endcardId, playableId) =
            (typeMap.getOrElse("50002", "null"), typeMap.getOrElse("61002", "null"))
          val (endcardMd5, playableMd5) =
            (creaMd5MapBC.value.getOrElse(endcardId, "null"), creaMd5MapBC.value.getOrElse(playableId, "null"))

          val thirdParty = "null"  // camThirdMapBC.value.getOrElse(camId, "null")

          (key, Array(playableId, playableMd5, templateGroup, videoTemplate,
            endcardTemplate, minicardTemplate, thirdParty, allowPlayable,
            extPlayable, endcardId, endcardMd5, videoEndType).mkString("\002"))
        }
        .reduceByKey{case(line1, line2) => Array(line1, line2).apply((new util.Random).nextInt(2))}

    val joinData =
      baseData.join(creativeData)
        .map{case(key, (baseStr, creativeStr)) =>

          val baseArr = baseStr.split("\002")
          val creativeArr = creativeStr.split("\002")

          creativeArr(6) = camThirdMapBC.value.getOrElse(baseArr(20), "null")

          Array(
            baseArr.mkString("\002"),
            creativeArr.mkString("\002")
          ).mkString("\002")
        }

    joinData.repartition(1000).saveAsTextFile(outPath)

    ()
  }

}
