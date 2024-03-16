package com.mobvista.data.creative3.check

import java.net.URI
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.TimeZone

import com.alibaba.fastjson.JSON
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

class Check {

  def check(sc: SparkContext, dateHour: String): Unit = {

    val fs = FileSystem.get(new URI("s3://mob-emr-test"), sc.hadoopConfiguration)

    sc.textFile("s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly/20190625")
      .filter{line =>
        val lineSplit = line.trim.split("\002")
        val unitId = lineSplit(9)

        unitId == "51304" || unitId == "112223"
      }
      .map{line =>
        val lineSplit = line.trim.split("\002")
        val unitId = lineSplit(9)

        (unitId, 1)
      }
      .reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}
      .map{case(unitId, cnt) => "%s\t%d".format(unitId, cnt)}
      .collect()
      .foreach(println)


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
    val realtimePath = "s3://mob-emr-test/guangxue/real_time_mwho/%s/%s".format(date, dateHour)
//    val realtimePath = "s3://mob-emr-test/guangxue/new_base/instance/%s/%s".format(date, dateHour)

//    val outPath = "s3://mob-emr-test/baihai/m_sys_model/creative3_v1/train_data_hourly/%s/%s".format(date, dateHour)

    val infoArr = new ArrayBuffer[String]()
    infoArr.appendAll(
      Array(
        "\n",
        "dateHour: %s".format(dateHour),
        "creativeMd5Path: %s".format(creativeMd5Path),
        "camThirdPartyPath: %s".format(camThirdPartyPath),
        "imprePath: %s".format(imprePath),
        "realtimePath: %s".format(realtimePath)
      )
    )
//    println("outPath: %s".format(outPath))

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

    val v1_base =
      sc.textFile(realtimePath)
        .map{line =>
          val lineSplit = line.trim.split("\002")
          val extra5 = lineSplit(2).split("\\|")(0)
          val camId = lineSplit(20)
          val key = extra5 + "_" + camId
          (key, line.trim)
        }
    val v1_base_cnt = v1_base.count()
    infoArr.append("\nv1_base_cnt: %d".format(v1_base_cnt))

    val v1_base_uniq = v1_base.reduceByKey{case(line1, line2) => Array(line1, line2).apply((new util.Random).nextInt(2))}
    val v1_base_uniq_cnt = v1_base_uniq.count()
    infoArr.append("v1_base_uniq_cnt: %d".format(v1_base_uniq_cnt))

    val v1_creative =
      sc.textFile(imprePath)
        .filter{line =>
          val lineSplit = line.trim.split("\t")

          try{
            val adType = lineSplit(10)
            val extNativeVideo = lineSplit(65)
            val strategy = lineSplit(25)
            val extAlgo = lineSplit(92)

            //              !adType.contains("appwall") &&
            !(adType == "native" && extNativeVideo == "3") &&
              strategy.contains("MNormalAlphaModelRanker") &&
              !strategy.contains("default") &&
              extAlgo.split(";")(0).split(",").length >= 18
          }catch{
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

          val thirdParty = "null" // camThirdMapBC.value.getOrElse(camId, "null")

          (key, Array(playableId, playableMd5, templateGroup, videoTemplate,
            endcardTemplate, minicardTemplate, thirdParty, allowPlayable,
            extPlayable, endcardId, endcardMd5, videoEndType))
        }
    val v1_creative_cnt = v1_creative.count()
    infoArr.append("v1_creative_cnt: %d".format(v1_creative_cnt))

    val v1_creative_fea_cnt =
      v1_creative
        .flatMap{case(key, feaArr) =>

          val nameArr = Array("playableId", "playableMd5", "templateGroup", "videoTemplate",
            "endcardTemplate", "minicardTemplate", "thirdParty", "allowPlayable",
            "extPlayable", "endcardId", "endcardMd5", "videoEndType")

          nameArr.zip(feaArr).map {case(name, fea) =>
            val vCnt = new collection.mutable.HashMap[String, Int]()
            vCnt.put(fea.toString, 1)
            (name, vCnt)
          }
        }
        .reduceByKey{case(vCnt1, vCnt2) =>
          val mCnt = new collection.mutable.HashMap[String, Int]()
          vCnt1.foreach{case(fea, cnt) => mCnt.put(fea, cnt)}
          vCnt2.foreach{case(fea, cnt) => mCnt.put(fea, cnt + mCnt.getOrElse(fea, 0))}
          mCnt
        }
        .map{case(name, feaCnt) =>
          Array(
            "v1_creative_feature: %s".format(name),
            feaCnt.filter{case(fea, cnt) => Array("null", "none", "0").contains(fea)}.map{case(fea, cnt) => "\tv1_creative value: %s, cnt: %d".format(fea, cnt)}.mkString("\n")
          ).mkString("\n")
        }
        .collect()
    infoArr.appendAll(v1_creative_fea_cnt)

    val v1_creative_uniq = v1_creative.reduceByKey{case(arr1, arr2) => Array(arr1, arr2).apply((new util.Random).nextInt(2))}
    val v1_creative_uniq_cnt = v1_creative_uniq.count()
    infoArr.append("v1_creative_uniq_cnt: %d".format(v1_creative_uniq_cnt))

    val v1_creative_uniq_fea_cnt =
      v1_creative_uniq
        .flatMap{case(key, feaArr) =>

          val nameArr = Array("playableId", "playableMd5", "templateGroup", "videoTemplate",
            "endcardTemplate", "minicardTemplate", "thirdParty", "allowPlayable",
            "extPlayable", "endcardId", "endcardMd5", "videoEndType")

          nameArr.zip(feaArr).map {case(name, fea) =>
            val vCnt = new collection.mutable.HashMap[String, Int]()
            vCnt.put(fea.toString, 1)
            (name, vCnt)
          }
        }
        .reduceByKey{case(vCnt1, vCnt2) =>
          val mCnt = new collection.mutable.HashMap[String, Int]()
          vCnt1.foreach{case(fea, cnt) => mCnt.put(fea, cnt)}
          vCnt2.foreach{case(fea, cnt) => mCnt.put(fea, cnt + mCnt.getOrElse(fea, 0))}
          mCnt
        }
        .map{case(name, feaCnt) =>
          Array(
            "v1_creative_uniq_feature: %s".format(name),
            feaCnt.filter{case(fea, cnt) => Array("null", "none", "0").contains(fea)}.map{case(fea, cnt) => "\tv1_creative_uniq value: %s, cnt: %d".format(fea, cnt)}.mkString("\n")
          ).mkString("\n")
        }
        .collect()
    infoArr.appendAll(v1_creative_uniq_fea_cnt)

    val v1_left_join =
      v1_base
        .leftOuterJoin(v1_creative)
        .map{case(key, (realtime, creArrOpt)) =>
          val creArr =
            creArrOpt match {
              case Some(array) => array
              case None => (0 until 12).toArray.map{i => "null"}
            }
          creArr(6) = camThirdMapBC.value.getOrElse(realtime.split("\002")(20), "null")
          (key, Array(realtime, creArr.mkString("\002")).mkString("\002"))
        }
    val v1_left_join_cnt = v1_left_join.count()
    infoArr.append("v1_left_join_cnt: %d".format(v1_left_join_cnt))

    val v1_left_join_fea_cnt =
      v1_left_join
        .flatMap{case(key, line) =>
          val lineSplit = line.split("\002")
          val creativeArr = lineSplit.slice(lineSplit.length-12, lineSplit.length)

          val nameArr = Array("playableId", "playableMd5", "templateGroup", "videoTemplate",
            "endcardTemplate", "minicardTemplate", "thirdParty", "allowPlayable",
            "extPlayable", "endcardId", "endcardMd5", "videoEndType")

          nameArr.zip(creativeArr).map {case(name, fea) =>
            val vCnt = new collection.mutable.HashMap[String, Int]()
            vCnt.put(fea.toString, 1)
            (name, vCnt)
          }
        }
        .reduceByKey{case(vCnt1, vCnt2) =>
          val mCnt = new collection.mutable.HashMap[String, Int]()
          vCnt1.foreach{case(fea, cnt) => mCnt.put(fea, cnt)}
          vCnt2.foreach{case(fea, cnt) => mCnt.put(fea, cnt + mCnt.getOrElse(fea, 0))}
          mCnt
        }
        .map{case(name, feaCnt) =>
          Array(
            "v1_left_join_feature: %s".format(name),
            feaCnt.filter{case(fea, cnt) => Array("null", "none", "0").contains(fea)}.map{case(fea, cnt) => "\tv1_left_join value: %s, cnt: %d".format(fea, cnt)}.mkString("\n")
          ).mkString("\n")
        }
        .collect()
    infoArr.appendAll(v1_left_join_fea_cnt)

    val v1_left_join_uniq = v1_left_join.reduceByKey{case(line1, line2) => Array(line1, line2).apply((new util.Random).nextInt(2))}
    val v1_left_join_uniq_cnt = v1_left_join_uniq.count()
    infoArr.append("v1_left_join_uniq_cnt: %d".format(v1_left_join_uniq_cnt))

    val v1_left_join_uniq_fea_cnt =
      v1_left_join_uniq
        .flatMap{case(key, line) =>
          val lineSplit = line.split("\002")
          val creativeArr = lineSplit.slice(lineSplit.length-12, lineSplit.length)

          val nameArr = Array("playableId", "playableMd5", "templateGroup", "videoTemplate",
            "endcardTemplate", "minicardTemplate", "thirdParty", "allowPlayable",
            "extPlayable", "endcardId", "endcardMd5", "videoEndType")

          nameArr.zip(creativeArr).map {case(name, fea) =>
            val vCnt = new collection.mutable.HashMap[String, Int]()
            vCnt.put(fea.toString, 1)
            (name, vCnt)
          }
        }
        .reduceByKey{case(vCnt1, vCnt2) =>
          val mCnt = new collection.mutable.HashMap[String, Int]()
          vCnt1.foreach{case(fea, cnt) => mCnt.put(fea, cnt)}
          vCnt2.foreach{case(fea, cnt) => mCnt.put(fea, cnt + mCnt.getOrElse(fea, 0))}
          mCnt
        }
        .map{case(name, feaCnt) =>
          Array(
            "v1_left_join_uniq_feature: %s".format(name),
            feaCnt.filter{case(fea, cnt) => Array("null", "none", "0").contains(fea)}.map{case(fea, cnt) => "\tv1_left_join_uniq value: %s, cnt: %d".format(fea, cnt)}.mkString("\n")
          ).mkString("\n")
        }
        .collect()
    infoArr.appendAll(v1_left_join_uniq_fea_cnt)


    val v1_join =
      v1_base
        .join(v1_creative)
        .map{case(key, (realtime, creArr)) =>
          creArr(6) = camThirdMapBC.value.getOrElse(realtime.split("\002")(20), "null")
          (key, Array(realtime, creArr.mkString("\002")).mkString("\002"))
        }
    val v1_join_cnt = v1_join.count()
    infoArr.append("v1_join_cnt: %d".format(v1_join_cnt))

    val v1_join_fea_cnt =
      v1_join
        .flatMap{case(key, line) =>
          val lineSplit = line.split("\002")
          val creativeArr = lineSplit.slice(lineSplit.length-12, lineSplit.length)

          val nameArr = Array("playableId", "playableMd5", "templateGroup", "videoTemplate",
            "endcardTemplate", "minicardTemplate", "thirdParty", "allowPlayable",
            "extPlayable", "endcardId", "endcardMd5", "videoEndType")

          nameArr.zip(creativeArr).map {case(name, fea) =>
            val vCnt = new collection.mutable.HashMap[String, Int]()
            vCnt.put(fea.toString, 1)
            (name, vCnt)
          }
        }
        .reduceByKey{case(vCnt1, vCnt2) =>
          val mCnt = new collection.mutable.HashMap[String, Int]()
          vCnt1.foreach{case(fea, cnt) => mCnt.put(fea, cnt)}
          vCnt2.foreach{case(fea, cnt) => mCnt.put(fea, cnt + mCnt.getOrElse(fea, 0))}
          mCnt
        }
        .map{case(name, feaCnt) =>
          Array(
            "v1_join_feature: %s".format(name),
            feaCnt.filter{case(fea, cnt) => Array("null", "none", "0").contains(fea)}.map{case(fea, cnt) => "\tv1_join value: %s, cnt: %d".format(fea, cnt)}.mkString("\n")
          ).mkString("\n")
        }
        .collect()
    infoArr.appendAll(v1_join_fea_cnt)

    val v1_join_uniq = v1_join.reduceByKey{case(line1, line2) => Array(line1, line2).apply((new util.Random).nextInt(2))}
    val v1_join_uniq_cnt = v1_join_uniq.count()
    infoArr.append("v1_join_uniq_cnt: %d".format(v1_join_uniq_cnt))

    val v1_join_uniq_fea_cnt =
      v1_join_uniq
        .flatMap{case(key, line) =>
          val lineSplit = line.split("\002")
          val creativeArr = lineSplit.slice(lineSplit.length-12, lineSplit.length)

          val nameArr = Array("playableId", "playableMd5", "templateGroup", "videoTemplate",
            "endcardTemplate", "minicardTemplate", "thirdParty", "allowPlayable",
            "extPlayable", "endcardId", "endcardMd5", "videoEndType")

          nameArr.zip(creativeArr).map {case(name, fea) =>
            val vCnt = new collection.mutable.HashMap[String, Int]()
            vCnt.put(fea.toString, 1)
            (name, vCnt)
          }
        }
        .reduceByKey{case(vCnt1, vCnt2) =>
          val mCnt = new collection.mutable.HashMap[String, Int]()
          vCnt1.foreach{case(fea, cnt) => mCnt.put(fea, cnt)}
          vCnt2.foreach{case(fea, cnt) => mCnt.put(fea, cnt + mCnt.getOrElse(fea, 0))}
          mCnt
        }
        .map{case(name, feaCnt) =>
          Array(
            "v1_join_uniq_feature: %s".format(name),
            feaCnt.filter{case(fea, cnt) => Array("null", "none", "0").contains(fea)}.map{case(fea, cnt) => "\tv1_join_uniq value: %s, cnt: %d".format(fea, cnt)}.mkString("\n")
          ).mkString("\n")
        }
        .collect()
    infoArr.appendAll(v1_join_uniq_fea_cnt)


    val v2_base_uniq =
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
    val v2_base_uniq_cnt = v2_base_uniq.count()
    infoArr.append("v2_base_uniq_cnt: %d".format(v2_base_uniq_cnt))

    val v2_creative_uniq =
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

          val extModel = lineSplit(107).toLowerCase() // ext_model
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
    val v2_creative_uniq_cnt = v2_creative_uniq.count()
    infoArr.append("v2_creative_uniq_cnt: %d".format(v2_creative_uniq_cnt))


    val v2_creative_uniq_fea_cnt =
      v2_creative_uniq
        .flatMap{case(key, line) =>

          val nameArr = Array("playableId", "playableMd5", "templateGroup", "videoTemplate",
            "endcardTemplate", "minicardTemplate", "thirdParty", "allowPlayable",
            "extPlayable", "endcardId", "endcardMd5", "videoEndType",
            "isCreative3", "isFullScreen", "extBrand", "extModel")

          nameArr.zip(line.split("\002")).map {case(name, fea) =>
            val vCnt = new collection.mutable.HashMap[String, Int]()
            vCnt.put(fea.toString, 1)
            (name, vCnt)
          }
        }
        .reduceByKey{case(vCnt1, vCnt2) =>
          val mCnt = new collection.mutable.HashMap[String, Int]()
          vCnt1.foreach{case(fea, cnt) => mCnt.put(fea, cnt)}
          vCnt2.foreach{case(fea, cnt) => mCnt.put(fea, cnt + mCnt.getOrElse(fea, 0))}
          mCnt
        }
        .map{case(name, feaCnt) =>
          Array(
            "v2_creative_uniq_feature: %s".format(name),
            feaCnt.filter{case(fea, cnt) => Array("null", "none", "0").contains(fea)}.map{case(fea, cnt) => "\tv2_creative_uniq value: %s, cnt: %d".format(fea, cnt)}.mkString("\n")
          ).mkString("\n")
        }
        .collect()
    infoArr.appendAll(v2_creative_uniq_fea_cnt)

    val v2_join_uniq =
      v2_base_uniq
        .join(v2_creative_uniq)
        .map{case(key, (baseStr, creativeStr)) =>

          val baseArr = baseStr.split("\002")
          val creativeArr = creativeStr.split("\002")

          val ignArr = Array("0", "none")
          if(!ignArr.contains(baseArr(12)) && ignArr.contains(creativeArr(14)))
            creativeArr(14) = baseArr(12)

          creativeArr(6) = camThirdMapBC.value.getOrElse(baseArr(20), "null")

          Array(
            baseArr.mkString("\002"),
            creativeArr.mkString("\002")
          ).mkString("\002")
        }
    val v2_join_uniq_cnt = v2_join_uniq.count()
    infoArr.append("v2_join_uniq_cnt: %d".format(v2_join_uniq_cnt))

    val v2_join_uniq_fea_cnt =
    v2_join_uniq
      .flatMap{line =>
        val lineSplit = line.split("\002")
        val creativeArr = lineSplit.slice(lineSplit.length-16, lineSplit.length)

        val nameArr = Array("playableId", "playableMd5", "templateGroup", "videoTemplate",
          "endcardTemplate", "minicardTemplate", "thirdParty", "allowPlayable",
          "extPlayable", "endcardId", "endcardMd5", "videoEndType",
          "isCreative3", "isFullScreen", "extBrand", "extModel")

        nameArr.zip(creativeArr).map {case(name, fea) =>
          val vCnt = new collection.mutable.HashMap[String, Int]()
          vCnt.put(fea.toString, 1)
          (name, vCnt)
        }
      }
      .reduceByKey{case(vCnt1, vCnt2) =>
        val mCnt = new collection.mutable.HashMap[String, Int]()
        vCnt1.foreach{case(fea, cnt) => mCnt.put(fea, cnt)}
        vCnt2.foreach{case(fea, cnt) => mCnt.put(fea, cnt + mCnt.getOrElse(fea, 0))}
        mCnt
      }
      .map{case(name, feaCnt) =>
        Array(
          "v2_join_uniq_feature: %s".format(name),
          feaCnt.filter{case(fea, cnt) => Array("null", "none", "0").contains(fea)}.map{case(fea, cnt) => "\tv2_join_uniq value: %s, cnt: %d".format(fea, cnt)}.mkString("\n")
        ).mkString("\n")
      }
      .collect()
    infoArr.appendAll(v2_join_uniq_fea_cnt)


    infoArr.foreach{line => println(line)}



    ()
  }

}
