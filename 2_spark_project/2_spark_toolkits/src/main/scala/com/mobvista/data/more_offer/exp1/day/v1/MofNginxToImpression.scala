package com.mobvista.data.more_offer.exp1.day.v1

import java.text.SimpleDateFormat
import java.util.{Locale, TimeZone}

import com.alibaba.fastjson.{JSON, JSONException, JSONObject}
import org.apache.spark.SparkContext

class MofNginxToImpression {

  def generate(sc: SparkContext, date: String): Unit = {

    val year = date.substring(0, 4)
    val month = date.substring(4, 6)
    val day = date.substring(6, 8)
    val sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))

    val nginxPath = "s3://mob-ad/adn/nginxlog/tracking/%s/%s/%s".format(year, month, day)
    val requestPath = "s3://mob-ad/adn/tracking-v3/request/%s/%s/%s".format(year, month, day)
    val impressionPath = "s3://mob-ad/adn/tracking-v3/impression/%s/%s/%s".format(year, month, day)
    val creative3Path = "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly/%s".format(date)

//    val mofDataPath = "s3://mob-emr-test/baihai/test/more_offer/mofData/%s".format(date)
    val outputPath = "s3://mob-emr-test/baihai/m_sys_model/more_offer/exp1/train_data_daily/%s".format(date)

    println("nginxPath: %s".format(nginxPath))
    println("requestPath: %s".format(requestPath))
    println("impressionPath: %s".format(impressionPath))
    println("creative3Path: %s".format(creative3Path))
//    println("mofDataPath: %s".format(mofDataPath))
    println("outputPath: %s".format(outputPath))

    val nginxData =
      sc.textFile(nginxPath)
        .filter{row =>
          row.contains("idfa=") && row.contains("unit_id=") && row.contains("mof_data=") && row.contains("/openapi/ad/v3")
        }
        .map{nginx =>
          val line = nginx.trim
          val clientIp = line.substring(0, line.indexOf("-")).trim.split(",")(0)
          val timeStr = (sdf.parse(line.substring(line.indexOf("[")+1, line.indexOf("]")).split(" ")(0)).getTime / 1000).toString
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
          val idfa = urlMap("idfa")
          val unitId = urlMap("unit_id")
          val mofData = urlMap("mof_data")

          (Array(idfa, unitId, clientIp, timeStr).mkString("_"), mofData)
        }
        .repartition(200)
//    val nginxCnt = nginxData.count()
//    println("date: %s, nginxCnt: %d".format(date, nginxCnt))

    val requestData =
      sc.textFile(requestPath)
        .filter{request =>
          try{
            val reqSplit = request.trim.split("\t")
            val extData = reqSplit(108)
            val platform = reqSplit(13)
            val idfa = reqSplit(43)

            extData.contains("parent_id") && platform == "ios" && idfa.length > 10
          }catch {
            case e1: ArrayIndexOutOfBoundsException =>
              false
          }
        }
        .flatMap{request =>
          val reqSplit = request.trim.split("\t")
          val idfa = reqSplit(43)
          val unitId = reqSplit(5)
          val clientIp = reqSplit(34)
          val requestId = reqSplit(27)
          val time = java.lang.Long.valueOf(requestId.substring(0, 8), 16)

          Array(
            (Array(idfa, unitId, clientIp, (time-3).toString).mkString("_"), requestId),
            (Array(idfa, unitId, clientIp, (time-2).toString).mkString("_"), requestId),
            (Array(idfa, unitId, clientIp, (time-1).toString).mkString("_"), requestId),
            (Array(idfa, unitId, clientIp, time.toString).mkString("_"), requestId),
            (Array(idfa, unitId, clientIp, (time+1).toString).mkString("_"), requestId),
            (Array(idfa, unitId, clientIp, (time+2).toString).mkString("_"), requestId),
            (Array(idfa, unitId, clientIp, (time+3).toString).mkString("_"), requestId)
          )
        }
        .repartition(600)
//    val requestCnt = requestData.count()
//    println("date: %s, requestCnt: %d".format(date, requestCnt))

    val requestMofData =
      nginxData.join(requestData)
        .map{case(key, (mofData, requestId)) => (requestId, mofData)}
        .reduceByKey{case(mofData1, mofData2) => Array(mofData1, mofData2).apply(scala.util.Random.nextInt(2))}
//    val requestMofCnt = requestMofData.count()
//    println("date: %s, requestMofCnt: %d".format(date, requestMofCnt))

    val impressionData =
      sc.textFile(impressionPath)
        .filter{impression =>
          try{
            val impressionSplit = impression.trim.split("\t")
            val extData2 = impressionSplit(122)
            val platform = impressionSplit(13)

            extData2.contains("parent_id") && platform == "ios"
          }catch {
            case e1: ArrayIndexOutOfBoundsException =>
              false
          }
        }
        .map{impression =>

          val impressionSplit = impression.trim.split("\t")
          val requestId = impressionSplit(27)
          val unitId = impressionSplit(5)

          val isMof = "1"
          val extData2 = JSON.parseObject(impressionSplit(122))
          val mofType =
            if(extData2.containsKey("h5_t")){
              val h5t = extData2.getString("h5_t")
              if(h5t == "1")
                "ec"
              else
                "pl"
            }else if(unitId == "51304")
              "pl"
            else if(unitId == "51309")
              "ec"
            else
              "none"
          val parentId = extData2.getString("parent_id")
          val isFixedUnit =
            if(Array("51304", "51309").contains(unitId))
              "1"
            else
              "0"
          val camId = impressionSplit(7)

          (requestId, Array(isMof, mofType, parentId, isFixedUnit, camId).mkString("\002"))
        }
        .repartition(200)
//    val impressionCnt = impressionData.count()
//    println("date: %s, impressionCnt: %d".format(date, impressionCnt))

//    impressionData.map{case(requestId, feature) =>
//        val feaSplit = feature.split("\002")
//        val unitId = feaSplit(3)
//      (unitId, 1)
//    }
//      .reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}
//      .map{case(unitId, cnt) => "%s\t%d".format(unitId, cnt)}
//      .repartition(1)
//      .saveAsTextFile("s3://mob-emr-test/baihai/test/more_offer/impressionUnitCnt/%s".format(date))

    val mofImpreData =
      requestMofData.rightOuterJoin(impressionData)
        .map{case(requestId, (mofDataOpt, impreFea)) =>
          val impreFeaSplit = impreFea.split("\002")
          val camId = impreFeaSplit(4)
          val key = requestId + "_" + camId
          val mofData =
            if(mofDataOpt == None)
              "none"
            else
              mofDataOpt.get

          (key, Array(mofData, impreFea).mkString("\002"))
        }
//    mofImpreData.map{case(key, fea) => "%s\t%s".format(key, fea)}.repartition(200).saveAsTextFile(mofDataPath)

//    mofImpreData.map{case(key, fea) =>
//        val feaSplit = fea.split("\002")
//        val unitId = feaSplit(4)
//      (unitId, 1)
//    }
//      .reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}
//      .map{case(unitId, cnt) => "%s\t%d".format(unitId, cnt)}
//      .repartition(1)
//      .saveAsTextFile("s3://mob-emr-test/baihai/test/more_offer/mofDataUnitCnt/%s".format(date))

    val creative3Data =
      sc.textFile(creative3Path)
        .map{line =>
          val lineSplit = line.trim.split("\002")
          val requestId = lineSplit(2).split("\\|")(0)
          val camId = lineSplit(20)
          val key = requestId + "_" + camId

          (key, line.trim)
        }
//    val creative3Cnt = creative3Data.count()
//    println("creative3Cnt: %s".format(creative3Cnt))

    creative3Data
      .leftOuterJoin(mofImpreData, 1000)
      .map{case(key, (creative3, mofImpreOpt)) =>
        val mofFeaArr =
          if(mofImpreOpt == None){
            ("0" +: (0 until 9).map{i => "none"}).toArray
          }else{
            val Array(mofData, isMof, mofType, parentUnitId, isFixedUnit, camId) = mofImpreOpt.get.split("\002")
            val mofDataJson =
              try{
                JSON.parseObject(
                  mofData
                    .replaceAll("%257B", "{")
                    .replaceAll("%2522", "\"")
                    .replaceAll("%253A", ":")
                    .replaceAll("%252C", ",")
                    .replaceAll("%257D", "}")
                    .replaceAll("%7B", "{")
                    .replaceAll("%22", "\"")
                    .replaceAll("%3A", ":")
                    .replaceAll("%2C", ",")
                    .replaceAll("%7D", "}")
                )
              }catch {
                case e1: JSONException => new JSONObject()
              }

            val parentCrtCid =
              try{
                val parentCrtCidRaw = mofDataJson.getOrDefault("crt_cid", "none").toString.toLong
                if(parentCrtCidRaw > 5000000000L)
                  "none"
                else
                  parentCrtCidRaw.toString
              }catch {
                case e: Exception => "none"
              }

//            val crtRid = mofDataJson.getOrDefault("crt_rid", "none").toString

            val parentRvTidRaw = mofDataJson.getOrDefault("rv_tid", "none").toString
            val parentRvTid =
              if(parentRvTidRaw.length < 1)
                "none"
              else
                parentRvTidRaw

            val parentEcIdRaw = mofDataJson.getOrDefault("ec_id", "none").toString
            val parentEcId =
              if(parentEcIdRaw.length < 1)
                "none"
              else if(Array("4", "7").contains(parentEcIdRaw))
                "null"
              else
                parentEcIdRaw

            val parentTplGroupRaw = mofDataJson.getOrDefault("tplgp", "none").toString
            val parentTplGroup =
              if(parentTplGroupRaw.length < 1)
                "none"
              else
                parentTplGroupRaw

            val parentVfMd5Raw = mofDataJson.getOrDefault("v_fmd5", "none").toString
            val parentVfMd5 =
              if(parentVfMd5Raw.length < 10)
                "none"
              else
                parentVfMd5Raw

            val parentIfMd5Raw = mofDataJson.getOrDefault("i_fmd5", "none").toString
            val parentIfMd5 =
              if(parentIfMd5Raw.length < 10)
                "none"
              else
                parentIfMd5Raw

            Array(isMof, mofType, parentUnitId, isFixedUnit, parentCrtCid, parentRvTid, parentEcId, parentTplGroup, parentVfMd5, parentIfMd5)
          }

          Array(creative3, mofFeaArr.mkString("\002")).mkString("\002")
      }
      .repartition(1000)
      .saveAsTextFile(outputPath)

    ()
  }

}
