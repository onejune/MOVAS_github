package com.mobvista.data.more_offer.exp1.day.v1

import org.apache.spark.SparkContext

class DataCheck {

  def check(sc: SparkContext, date: String): Unit = {

    val inputPath = "s3://mob-emr-test/baihai/m_sys_model/more_offer/exp1/train_data_daily/%s".format(date)
    val outputPath = "s3://mob-emr-test/baihai/test/more_offer_check/%s".format(date)

    sc.textFile(inputPath)
      .flatMap{line =>
        val lineSplit = line.trim.split("\002")

        val mofFeaList = lineSplit.slice(340, 350)
        val feaNameList = Array("isMof", "mofType", "parentUnitId", "isFixedUnit", "parentCrtCid", "parentRvTid", "parentEcId", "parentTplGroup", "parentVfMd5", "parentIfMd5")

        feaNameList.zip(mofFeaList).map{case(feaName, feaValue) => (feaName + "_" + feaValue, 1)}
      }
      .reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}
      .map{case(feaNameValue, cnt) => "%s\t%d".format(feaNameValue, cnt)}
      .repartition(10)
      .saveAsTextFile(outputPath)

    /////////////

    /*val illegalData =
      sc.textFile("s3://mob-ad/adn/nginxlog/tracking/%s/%s/%s".format("2019", "08", "01")).filter{row =>
        row.contains("idfa=") && row.contains("unit_id=") && row.contains("mof_data=") && row.contains("/openapi/ad/v3")
      }.map{nginx =>
        val line = nginx.trim
        val url = line.split(" ").filter(_.startsWith("/openapi/ad/v3"))(0)
        val urlMap =
          url.split("\\?", 2)(1).split("&").map{seg =>
            val segSplit = seg.split("=", 2)
            try{
              (segSplit(0), segSplit(1))
            }catch {
              case e1: ArrayIndexOutOfBoundsException =>
                ("key", "value")
            }
          }.toMap
        val mofData = urlMap("mof_data")
        val mofDataJson =
          try{
            JSON.parseObject(
              mofData.replaceAll("%257B", "{").replaceAll("%2522", "\"").replaceAll("%253A", ":").replaceAll("%252C", ",").replaceAll("%257D", "}").replaceAll("%7B", "{").replaceAll("%22", "\"").replaceAll("%3A", ":").replaceAll("%2C", ",").replaceAll("%7D", "}")
            )
          }catch {
            case e1: JSONException => new JSONObject()
          }
        val crtCid = mofDataJson.getOrDefault("crt_cid", "none").toString
        val crtRid = mofDataJson.getOrDefault("crt_rid", "none").toString
        val rvTid = mofDataJson.getOrDefault("rv_tid", "none").toString
        val ecId = mofDataJson.getOrDefault("ec_id", "none").toString
        val tplGroup = mofDataJson.getOrDefault("tplgp", "none").toString
        val vfMd5Raw = mofDataJson.getOrDefault("v_fmd5", "none").toString
        val vfMd5 =
          if(vfMd5Raw.length < 10)
            "none"
          else
            vfMd5Raw
        val ifMd5Raw = mofDataJson.getOrDefault("i_fmd5", "none").toString
        val ifMd5 =
          if(ifMd5Raw.length < 10)
            "none"
          else
            ifMd5Raw
        (line, mofData, rvTid, ecId, tplGroup)
      }.filter{case(line, mofData, rvTid, ecId, tplGroup) =>
        rvTid.length < 1 || ecId == "4" || ecId == "7" || ecId.length < 1 || tplGroup.length < 1
      }
    illegalData.filter{case(line, mofData, rvTid, ecId, tplGroup) =>
        rvTid.length < 1
      }.map{case(line, mofData, rvTid, ecId, tplGroup) =>
        "%s\t%s".format(mofData, line)
      }.repartition(10).saveAsTextFile("s3://mob-emr-test/baihai/test/more_offer_check/illegal_rv_tid_empty/20190801")
    illegalData.filter{case(line, mofData, rvTid, ecId, tplGroup) =>
        ecId == "4"
      }.map{case(line, mofData, rvTid, ecId, tplGroup) =>
      "%s\t%s".format(mofData, line)
      }.repartition(10).saveAsTextFile("s3://mob-emr-test/baihai/test/more_offer_check/illegal_ec_id_4/20190801")
    illegalData.filter{case(line, mofData, rvTid, ecId, tplGroup) =>
        ecId == "7"
      }.map{case(line, mofData, rvTid, ecId, tplGroup) =>
      "%s\t%s".format(mofData, line)
      }.repartition(10).saveAsTextFile("s3://mob-emr-test/baihai/test/more_offer_check/illegal_ec_id_7/20190801")
    illegalData.filter{case(line, mofData, rvTid, ecId, tplGroup) =>
        ecId.length < 1
      }.map{case(line, mofData, rvTid, ecId, tplGroup) =>
      "%s\t%s".format(mofData, line)
      }.repartition(10).saveAsTextFile("s3://mob-emr-test/baihai/test/more_offer_check/illegal_ec_id_empty/20190801")
    illegalData.filter{case(line, mofData, rvTid, ecId, tplGroup) =>
        tplGroup.length < 1
      }.map{case(line, mofData, rvTid, ecId, tplGroup) =>
      "%s\t%s".format(mofData, line)
      }.repartition(10).saveAsTextFile("s3://mob-emr-test/baihai/test/more_offer_check/illegal_tplgp_empty/20190801")



    val allData = sc.textFile("s3://mob-ad/adn/ranker_service/ranker_service.ranker/2019/08/02").filter{line => line.contains("parent_unit:61286")}
    val allCnt = allData.count()
    println("allCnt: %d".format(allCnt))
    val fixUnitData = allData.filter{line => line.contains("real_unit:51304")}
    val fixCnt = fixUnitData.count()
    println("fixCnt: %d".format(fixCnt))
    fixUnitData.repartition(1).saveAsTextFile("s3://mob-emr-test/baihai/test/more_offer_61286_51304")
    val splitUnitData = allData.filter{line => line.contains("real_unit:98854")}
    val splitCnt = splitUnitData.count()
    println("splitCnt: %d".format(splitCnt))
    splitUnitData.repartition(1).saveAsTextFile("s3://mob-emr-test/baihai/test/more_offer_61286_98854")*/

    ///////


    ()
  }

}
