package com.mobvista.data.creative3.update_v2.hour

import java.net.URI
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.TimeZone

import com.alibaba.fastjson.JSON
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

import scala.collection.immutable.HashMap

class DataGenerationUniq {

  def generate(sc: SparkContext, dateHour: String): Unit = {

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

    val year = dateHour.substring(0, 4)
    val month = dateHour.substring(4, 6)
    val day = dateHour.substring(6, 8)
    val hour = dateHour.substring(8, 10)
    val date = dateHour.substring(0, 8)
    val imprePath = "s3://mob-ad/adn/tracking-v3/impression/%s/%s/%s/*/%s".format(year, month, day, hour)
//    val realtimePath = "s3://mob-emr-test/guangxue/real_time_mwho/%s/%s".format(date, dateHour)
    val realtimePath = "s3://mob-emr-test/guangxue/new_base/instance/%s/%s".format(date, dateHour)

    val outPath = "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly/%s/%s".format(date, dateHour)

    println("dateHour: %s".format(dateHour))
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

          lineSplit(10) = lineSplit(10).toLowerCase() // device_model
          lineSplit(12) =                             // device_brand
            if(!Array("0", "none").contains(lineSplit(12)))
              lineSplit(12).toLowerCase()
            else if(lineSplit(10).contains("mi"))
              "xiaomi"
            else if(lineSplit(10).contains("sm"))
              "samsung"
            else if(lineSplit(10).contains("vivo"))
              "vivo"
            else if(lineSplit(10).contains("cph"))
              "oppo"
            else
              lineSplit(12)

          (key, lineSplit.mkString("\002"))
        }
        .reduceByKey({(base1: String, base2: String) => base1})
    //    val baseCnt = baseData.count()
    //    println("baseCnt: %d".format(baseCnt))

    //    baseData.aggregateByKey(0)({case(cnt, base) => cnt + 1}, {case(cnt1, cnt2) => cnt1 + cnt2})
    //      .sortBy({case(key, cnt) => cnt}, false)
    //      .take(50)
    //      .foreach{case(key, cnt) => println("base\t%s: %d".format(key, cnt))}

    val creativeData =
      sc.textFile(imprePath)
        .filter{line =>
          try{
            val lineSplit = line.trim.split("\t")
            val adType = lineSplit(10)
            val extNativeVideo = lineSplit(65)
            val strategy = lineSplit(25)
            val extAlgo = lineSplit(92)

//            !adType.contains("appwall") &&
              !(adType == "native" && extNativeVideo == "3") &&
              strategy.contains("MNormalAlphaModelRanker") &&
              !strategy.contains("default") &&
              extAlgo.split(";")(0).split(",").length >= 23
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

          val thirdParty = "null"

          val isCreative3 = extAlgo(13)
          val isFullScreen = extAlgo(22)

          val extModel = lineSplit(107).trim.toLowerCase() // ext_model
          val extBrand =
            if(!Array("0", "none").contains(lineSplit(106)))
              lineSplit(106).toLowerCase()
            else if(extModel.contains("mi"))
              "xiaomi"
            else if(extModel.contains("sm"))
              "samsung"
            else if(extModel.contains("vivo"))
              "vivo"
            else if(extModel.contains("cph"))
              "oppo"
            else
              lineSplit(106)

          (key, Array(playableId, playableMd5, templateGroup, videoTemplate,
            endcardTemplate, minicardTemplate, thirdParty, allowPlayable,
            extPlayable, endcardId, endcardMd5, videoEndType,
            isCreative3, isFullScreen, extBrand, extModel).mkString("\002"))
        }
        .reduceByKey({(creative1: String, creative2: String) => creative1})
    //    val creativeCnt = creativeData.count()
    //    println("creativeCnt: %d".format(creativeCnt))


    //    creativeData.aggregateByKey(0)({case(cnt, creative) => cnt + 1}, {case(cnt1, cnt2) => cnt1 + cnt2})
    //      .sortBy({case(key, cnt) => cnt}, false)
    //      .take(50)
    //      .foreach{case(key, cnt) => println("creative\t%s: %d".format(key, cnt))}

    val joinData =
      baseData
        .leftOuterJoin(creativeData)
        .map{case(key, (baseStr, creativeStrOpt)) =>

          val creativeStr =
            creativeStrOpt match {
              case Some(str) => str
              case None => "null" + "\002null"*15
            }

          val baseArr = baseStr.split("\002")
          val creativeArr = creativeStr.split("\002")

          val ignArr = Array("", "0", "none", "null")
          if(!ignArr.contains(baseArr(12)) && ignArr.contains(creativeArr(14)))
            creativeArr(14) = baseArr(12)

          creativeArr(6) = camThirdMapBC.value.getOrElse(baseArr(20), "s2s")

          Array(
            baseArr.mkString("\002"),
            creativeArr.mkString("\002")
          ).mkString("\002")
        }
    //    val joinCnt = joinData.count()
    //    println("joinCnt: %d".format(joinCnt))

    joinData.saveAsTextFile(outPath)

    ()
  }

}
