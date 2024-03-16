package com.mobvista.data.more_offer.exp1.hour.v2

import com.alibaba.fastjson.{JSON, JSONException, JSONObject}
import org.apache.spark.SparkContext

class MofRequestToImpression {

  def generate(sc: SparkContext, dateHour: String): Unit = {

    val year = dateHour.substring(0, 4)
    val month = dateHour.substring(4, 6)
    val day = dateHour.substring(6, 8)
    val hour = dateHour.substring(8, 10)
    val date = dateHour.substring(0, 8)

    val requestPath = "s3://mob-ad/adn/tracking-v3/request/%s/%s/%s/*/%s".format(year, month, day, hour)
    val impressionPath = "s3://mob-ad/adn/tracking-v3/impression/%s/%s/%s/*/%s".format(year, month, day, hour)
    val creative3Path = "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly/%s/%s".format(date, dateHour)

    val outputPath = "s3://mob-emr-test/baihai/m_sys_model/more_offer/exp1/train_data_hourly_v2/%s/%s".format(date, dateHour)

    println("requestPath: %s".format(requestPath))
    println("impressionPath: %s".format(impressionPath))
    println("creative3Path: %s".format(creative3Path))
    println("outputPath: %s".format(outputPath))

    val requestMofData =
      sc.textFile(requestPath)
        .filter{request =>
          try{
            val reqSplit = request.trim.split("\t")
            val extData = reqSplit(108)

            reqSplit.length >= 133 && extData.contains("parent_id")
          }catch {
            case e1: ArrayIndexOutOfBoundsException =>
              false
          }
        }
        .map{request =>
          val reqSplit = request.trim.split("\t")
          val requestId = reqSplit(27)
          val mofData = reqSplit(132)

          (requestId, mofData)
        }
        .repartition(100)

    val impressionData =
      sc.textFile(impressionPath)
        .filter{impression =>
          try{
            val impressionSplit = impression.trim.split("\t")
            val extData2 = impressionSplit(122)

            extData2.contains("parent_id")
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
            }else if(Array("51304", "117363").contains(unitId))
              "pl"
            else if(Array("51309", "117361").contains(unitId))
              "ec"
            else
              "none"
          val parentId = extData2.getString("parent_id")
          val camId = impressionSplit(7)

          (requestId, Array(isMof, mofType, parentId, camId).mkString("\002"))
        }
        .repartition(100)

    val mofImpreData =
      requestMofData.join(impressionData)
        .map{case(requestId, (mofData, impreFea)) =>
          val impreFeaSplit = impreFea.split("\002")
          val camId = impreFeaSplit(3)
          val key = requestId + "_" + camId

          (key, Array(mofData, impreFea).mkString("\002"))
        }

    val creative3Data =
      sc.textFile(creative3Path)
        .map{line =>
          val lineSplit = line.trim.split("\002")
          val requestId = lineSplit(2).split("\\|")(0)
          val camId = lineSplit(20)
          val key = requestId + "_" + camId

          (key, line.trim)
        }

    creative3Data
      .leftOuterJoin(mofImpreData, 100)
      .map{case(key, (creative3, mofImpreOpt)) =>
        val mofFeaArr =
          if(mofImpreOpt == None){
            ("0" +: (0 until 8).map{i => "none"}).toArray
          }else{
            val Array(mofData, isMof, mofType, parentUnitId, camId) = mofImpreOpt.get.split("\002")
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

            Array(isMof, mofType, parentUnitId, parentCrtCid, parentRvTid, parentEcId, parentTplGroup, parentVfMd5, parentIfMd5)
          }

        val creativeSplit = creative3.split("\002")
        if(mofFeaArr(1) == "pl" && !Array("51304", "95474", "112223").contains(creativeSplit(9))){
          creativeSplit(9) = "51304"
          creativeSplit(8) = "92763"
          creativeSplit(7) = "none"
        }
        if(mofFeaArr(1) == "ec" && !Array("51309").contains(creativeSplit(9))){
          creativeSplit(9) = "51309"
          creativeSplit(8) = "92763"
          creativeSplit(7) = "none"
        }

        Array(creativeSplit.mkString("\002"), mofFeaArr.mkString("\002")).mkString("\002")
      }
      .repartition(100)
      .saveAsTextFile(outputPath)

    ()
  }

}
