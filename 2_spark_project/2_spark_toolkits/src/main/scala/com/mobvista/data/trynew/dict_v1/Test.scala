package com.mobvista.data.trynew.dict_v1

import java.net.URI
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.TimeZone

import com.alibaba.fastjson.{JSON, JSONException, JSONObject}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

class Test {

  def test(sc: SparkContext): Unit = {

    //////////////////
    val infoArr = new ArrayBuffer[String]()

    val allData =
      sc.textFile("s3://mob-ad/adn/ranker_service/ranker_service.ranker/2019/08/09/*/15,s3://mob-ad/adn/ranker_service/ranker_service.ranker/2019/08/09/*/16")
        .filter{line =>
          try{
            val lineSplit = line.trim.split("\t")
            val unitId = lineSplit.filter{seg => seg.startsWith("key:")}(0).split("#")(3)

            Array("61286", "61287", "63733").contains(unitId)
          }catch {
            case e1: ArrayIndexOutOfBoundsException => false
          }
        }
        .repartition(100)
    val allCnt = allData.count()
    val allInfo = "all_request_cnt\t%d".format(allCnt)
    infoArr.append(allInfo)

    val tryNewData =
      allData.filter{line =>
        val lineSplit = line.trim.split(" ")
        lineSplit.filter{seg => seg.startsWith("unit_try_new:")}.length > 0
      }
    val tryNewCnt = tryNewData.count()
    val tryNewInfo = "recall_new_request_cnt\t%d".format(tryNewCnt)
    infoArr.append(tryNewInfo)

    val camRecallInfo =
      tryNewData.flatMap{line =>line.trim.split(" ").filter{seg =>seg.startsWith("unit_try_new:")}.map{seg => (seg.split(":")(1), 1)}}
        .reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}
        .map{case(cam, cnt) => "recall\t%s\t%d".format(cam, cnt)}
        .collect()
    infoArr.appendAll(camRecallInfo)

    val hitInfo =
      tryNewData.filter{line =>
        val lineSplit = line.trim.split("\t")
        lineSplit.filter{seg => seg.startsWith("hitted_trynew_offer:")}.length > 0
      }
        .map{line =>
          val lineSplit = line.trim.split("\t")
          (lineSplit.filter{seg => seg.startsWith("hitted_trynew_offer")}(0).split(":")(1), 1)
        }
        .reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}
        .map{case(cam, cnt) => "hit\t%s\t%d".format(cam, cnt)}
        .collect()
    infoArr.appendAll(hitInfo)

    sc.parallelize(infoArr, 1).saveAsTextFile("s3://mob-emr-test/baihai/test/tryNew/61286_61287_63733_2019080915_2019080916")

    /////////////////////////////////////////

    sc.textFile("s3://mob-ad/adn/ranker_service/ranker_service.ranker/2019/08/14/*/06")
      .filter{line  => line.contains("unit_auto_try_new")}
      .repartition(1)
      .saveAsTextFile("s3://mob-emr-test/baihai/test/tryNew/auto_try_new_example_2019081406")

    /////////////////////////////////////

    sc.textFile("s3://mob-ad/adn/ranker_service/ranker_service.ranker/2019/09/03")
        .filter{line =>
          try{
            val lineSplit = line.trim.split("\t")
            val keySplit = lineSplit.filter{seg => seg.startsWith("key:")}(0).split("#")
            val cc = keySplit(2)
            val unit = keySplit(3)
            val strategy = keySplit.last.split("\\|")(0)

            cc == "cn" && unit == "112223" && strategy == "1_more_offer_base"
          }catch {
            case e: Exception => false
          }
        }
        .repartition(10)
        .saveAsTextFile("s3://mob-emr-test/baihai/test/more_offer_base_cn_112223_20190903")

    sc.textFile("s3://mob-ad/adn/ranker_service/ranker_service.ranker/2019/09/03")
      .filter{line =>
        try{
          val lineSplit = line.trim.split("\t")
          val keySplit = lineSplit.filter{seg => seg.startsWith("key:")}(0).split("#")
          val cc = keySplit(2)
          val unit = keySplit(3)
          val strategy = keySplit.last.split("\\|")(0)

          cc == "cn" && unit == "112223" && strategy == "1_more_offer_v2"
        }catch {
          case e: Exception => false
        }
      }
      .repartition(10)
      .saveAsTextFile("s3://mob-emr-test/baihai/test/more_offer_v2_cn_112223")






    /////////////////////////////////

    sc.textFile("s3://mob-emr-test/baihai/test/more_offer_exp_perf_ios_20190831_20190902").map{line =>
        val lineSplit = line.split("\t")
        val key = lineSplit.slice(0, 3).mkString("-")
        val impre = lineSplit(3).toLong
        val click = lineSplit(4).toLong
        val install = lineSplit(5).toLong
        val rev = lineSplit(6).toDouble
        (key, (impre, click, install, rev))
      }.reduceByKey{case((impre1, click1, install1, rev1), (impre2, click2, install2, rev2)) =>
        (impre1+impre2, click1+click2, install1+install2, rev1+rev2)
      }.map{case(key, (impre, click, install, rev)) =>
          "%s\t%d\t%d\t%d\t%f".format(key.split("-").mkString("\t"), impre, click, install, rev)
      }.saveAsTextFile("s3://mob-emr-test/baihai/test/more_offer_exp_perf_ios_20190831_20190902_merge")


    ///////////////////////////

    sc.textFile("s3://mob-ad/adn/nginxlog/tracking/2019/08/07/*/20").filter{row =>
        row.contains("idfa=") && row.contains("unit_id=") && row.contains("mof_data=") && row.contains("/openapi/ad/v3")
      }.filter{nginx =>
        val line = nginx.trim
        val url = line.split(" ").filter(_.contains("/openapi/ad/v3"))(0).replaceAll("\"", "")
        val urlMap = url.split("\\?", 2)(1).split("&").map{seg =>
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
        val parentCrtCid = mofDataJson.getOrDefault("crt_cid", "none").toString
        parentCrtCid.length > 10
      }.repartition(10).saveAsTextFile("s3://mob-emr-test/baihai/test/illegal_crt_cid_2019080720")



    ()
  }

  def test1(sc: SparkContext): Unit = {

    val allData = sc.textFile("s3://mob-ad/adn/ranker_service/ranker_service.ranker/2019/09/26/*/1[1-7]").filter{line =>

      try{
        Array("65775", "63733", "61291").contains(
          line.trim.split("\t")
            .filter{seg => seg.startsWith("unit_id=") && seg.split(":")(0).split("\001").length == 1}(0)
            .split(":")(0)
            .split("=")(1)
        ) &&
          !line.contains("mooc_exp1_new")
      }catch {
        case e: Exception => false
      }


    }.repartition(100)
    val unitAllRequestCnt = allData
      .map{line =>
        val unitId = line.trim.split("\t")
          .filter{seg => seg.startsWith("unit_id=") && seg.split(":")(0).split("\001").length == 1}(0)
          .split(":")(0)
          .split("=")(1)
        (unitId, 1)
      }.reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}
    val unitRecallNewCnt = allData.filter{line =>
      line.contains(" unit_try_new:") || line.contains(" unit_auto_try_new:")
    }.map{line =>
      val unitId = line.trim.split("\t")
        .filter{seg => seg.startsWith("unit_id=") && seg.split(":")(0).split("\001").length == 1}(0)
        .split(":")(0)
        .split("=")(1)
      (unitId, 1)
    }.reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}
    val unitRecallManualNewCnt = allData.filter{line =>
      line.contains(" unit_try_new:")
    }.map{line =>
      val unitId = line.trim.split("\t")
        .filter{seg => seg.startsWith("unit_id=") && seg.split(":")(0).split("\001").length == 1}(0)
        .split(":")(0)
        .split("=")(1)
      (unitId, 1)
    }.reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}

    val unitRecallAutoNewCnt = allData.filter{line =>
      line.contains(" unit_auto_try_new:")
    }.map{line =>
      val unitId = line.trim.split("\t")
        .filter{seg => seg.startsWith("unit_id=") && seg.split(":")(0).split("\001").length == 1}(0)
        .split(":")(0)
        .split("=")(1)
      (unitId, 1)
    }.reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}

    val allHitData = allData.filter{line =>
      line.split("\t").filter{seg => seg.startsWith("try_new_type:1")}.length > 0 ||
        line.split("\t").filter{seg => seg.startsWith("try_new_type:2")}.length > 0
    }
    val unitAllHitCnt = allHitData.map{line =>
      val unitId = line.trim.split("\t")
        .filter{seg => seg.startsWith("unit_id=") && seg.split(":")(0).split("\001").length == 1}(0)
        .split(":")(0)
        .split("=")(1)
      (unitId, 1)
    }.reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}

    val unitHitManualCnt = allHitData.filter{line =>
      val lineSplit = line.trim.split("\t")
      lineSplit.filter{seg => seg == "try_new_type:1"}.length > 0
    }.map{line =>
      val unitId = line.trim.split("\t")
        .filter{seg => seg.startsWith("unit_id=") && seg.split(":")(0).split("\001").length == 1}(0)
        .split(":")(0)
        .split("=")(1)
      (unitId, 1)
    }.reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}

    val unitHitAutoCnt = allHitData.filter{line =>
      val lineSplit = line.trim.split("\t")
      lineSplit.filter{seg => seg == "try_new_type:2"}.length > 0
    }.map{line =>
      val unitId = line.trim.split("\t")
        .filter{seg => seg.startsWith("unit_id=") && seg.split(":")(0).split("\001").length == 1}(0)
        .split(":")(0)
        .split("=")(1)
      (unitId, 1)
    }.reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}

    Array(unitAllRequestCnt, unitRecallNewCnt, unitRecallManualNewCnt, unitRecallAutoNewCnt, unitAllHitCnt, unitHitManualCnt, unitHitAutoCnt)
      .map{rdd =>
        rdd.map{case(key, cnt) => (key, Array(cnt))}
      }
      .reduceLeft{(rdd1: RDD[(String, Array[Int])], rdd2: RDD[(String, Array[Int])]) =>
        rdd1.leftOuterJoin(rdd2)
          .map{case(unit, (cnt1, cnt2Opt)) =>
            val cnt2 =
              cnt2Opt match {
                case Some(arr) => arr
                case None => Array(0)
              }
            (unit, cnt1 ++ cnt2)
          }
      }
      .map{case(unit, cntArr) =>

        val Array(allRequestCnt, recallNewCnt, recallManualCnt, recallAutoCnt, allHitCnt, hitManualCnt, hitAutoCnt) = cntArr

        "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d".format(unit, allRequestCnt, recallNewCnt, recallManualCnt, recallAutoCnt, allHitCnt, hitManualCnt, hitAutoCnt)
      }
      .repartition(1)
      .saveAsTextFile("s3://mob-emr-test/baihai/test/tryNew/unitAllData_2019092611_2019092617")

    val unitCamManualRecallCnt = allData.filter{line =>
      line.contains(" unit_try_new:")
    }.flatMap{line =>
      val unitId = line.trim.split("\t")
        .filter{seg => seg.startsWith("unit_id=") && seg.split(":")(0).split("\001").length == 1}(0)
        .split(":")(0)
        .split("=")(1)
      line.trim.split(" ")
        .filter{seg => seg.startsWith("unit_try_new:")}
        .map{seg =>
          (unitId+"_"+seg.split(":")(1), 1)
        }
    }.reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}
    val unitCamAutoRecallCnt = allData.filter{line =>
      line.contains(" unit_auto_try_new:")
    }.flatMap{line =>
      val unitId = line.trim.split("\t")
        .filter{seg => seg.startsWith("unit_id=") && seg.split(":")(0).split("\001").length == 1}(0)
        .split(":")(0)
        .split("=")(1)
      line.trim.split(" ")
        .filter{seg => seg.startsWith("unit_auto_try_new:")}
        .map{seg =>
          (unitId+"_"+seg.split(":")(1), 1)
        }
    }.reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}
    val unitCamHitManualCnt = allData.filter{line =>
      val lineSplit = line.trim.split("\t")
      lineSplit.filter{seg => seg == "try_new_type:1"}.length > 0
    }.map{line =>
      val lineSplit = line.trim.split("\t")
      val unitId = line.trim.split("\t")
        .filter{seg => seg.startsWith("unit_id=") && seg.split(":")(0).split("\001").length == 1}(0)
        .split(":")(0)
        .split("=")(1)
      val hitCam = lineSplit.filter{seg => seg.startsWith("hitted_trynew_offer")}(0).split(":")(1)
      (unitId +"_" + hitCam, 1)
    }.reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}
    val unitCamHitAutoCnt = allData.filter{line =>
      val lineSplit = line.trim.split("\t")
      lineSplit.filter{seg => seg == "try_new_type:2"}.length > 0
    }.map{line =>
      val lineSplit = line.trim.split("\t")
      val unitId = line.trim.split("\t")
        .filter{seg => seg.startsWith("unit_id=") && seg.split(":")(0).split("\001").length == 1}(0)
        .split(":")(0)
        .split("=")(1)
      val hitCam = lineSplit.filter{seg => seg.startsWith("hitted_trynew_offer")}(0).split(":")(1)
      (unitId +"_" + hitCam, 1)
    }.reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}
    val unitCamManualRecallHitCnt = unitCamManualRecallCnt.leftOuterJoin(unitCamHitManualCnt).map{case(unitCam, (recallCnt, hitCntOpt)) =>
      val hitCnt =
        hitCntOpt match {
          case Some(cnt) => cnt
          case None => 0
        }
      (unitCam, Array(recallCnt, hitCnt))
    }
    val unitCamAutoRecallHitCnt = unitCamAutoRecallCnt.leftOuterJoin(unitCamHitAutoCnt).map{case(unitCam, (recallCnt, hitCntOpt)) =>
      val hitCnt =
        hitCntOpt match {
          case Some(cnt) => cnt
          case None => 0
        }
      (unitCam, Array(recallCnt, hitCnt))
    }
    unitCamManualRecallHitCnt.fullOuterJoin(unitCamAutoRecallHitCnt).map{case(unitCam, (manOpt, autoOpt)) =>
      val Array(manRecallCnt, manHitCnt) =
        manOpt match{
          case Some(arr) => arr
          case None => Array(0, 0)
        }
      val Array(autoRecallCnt, autoHitCnt) =
        autoOpt match{
          case Some(arr1) => arr1
          case None => Array(0, 0)
        }
      val Array(unit, cam) = unitCam.split("_")
      "%s\t%s\t%d\t%d\t%d\t%d".format(unit, cam, manRecallCnt, manHitCnt, autoRecallCnt, autoHitCnt)
    }.repartition(1).saveAsTextFile("s3://mob-emr-test/baihai/test/tryNew/unitCamAllData_2019092611_2019092617")


    //////////////////////

    sc.textFile("s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly/20190814/2019081413")
      .map{line =>
        val lineSplit = line.trim.split("\002")
        val adtype = lineSplit(19)

        (adtype, 1)
      }
      .reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}
      .map{case(adtype, cnt) => "%s\t%d".format(adtype, cnt)}
      .collect()
      .foreach(println)


    ///////////////////////////////////
    sc.textFile("s3://mob-ad/adn/ranker_service/ranker_service.ranker/2019/08/16")
      .filter{line =>
        val lineSplit = line.trim.split("\t")

        try{
          val keySplit = lineSplit.filter{seg => seg.startsWith("key:")}(0).split("#")
          val unitId = keySplit(3)

          Array("51304", "51309", "112223").contains(unitId)
        }catch {
          case e: Exception => false
        }
      }
      .repartition(1)
      .saveAsTextFile("s3://mob-emr-test/baihai/test/51304_51309_112223")

    //////////////////////////////////////////

    val cnt = sc.textFile("s3://mob-ad/adn/tracking-v3/request/2019/08/18/*/20").filter{line =>
        val lineSplit = line.trim.split("\t")
        lineSplit.length > 108 && lineSplit(108).contains("parent_id") && lineSplit.length < 133
      }.count()

    val allMofData = sc.textFile("s3://mob-ad/adn/tracking-v3/request/2019/08/18/*/20").filter{line =>
      try{
        val lineSplit = line.trim.split("\t")
        val extData = lineSplit(108)
        extData.contains("parent_id") && lineSplit.length > 132
      }catch {
        case e: Exception => false
      }
    }.map{line =>
      val lineSplit = line.trim.split("\t")
      val unitId = lineSplit(5)
      val extData = lineSplit(108)
      val json = JSON.parseObject(extData)
      val ht =
        if(json.containsKey("h5_t"))
          json.getString("h5_t")
        else if(unitId == "51309")
          "1"
        else if(unitId == "51304")
          "2"
        else
          "none"
      val mofData = lineSplit(132)
      (unitId + "_" + ht, mofData)
    }
    allMofData.filter{case(unitHt, mofData) => unitHt.startsWith("51304")}.repartition(10).saveAsTextFile("s3://mob-emr-test/baihai/test/request_pl_fixed")
    allMofData.filter{case(unitHt, mofData) =>
        val split = unitHt.split("_")
        split(1) == "2" && split(0) != "51304"
    }.repartition(10).saveAsTextFile("s3://mob-emr-test/baihai/test/request_pl_split")
    allMofData.filter{case(unitHt, mofData) => unitHt.startsWith("51309")}.repartition(10).saveAsTextFile("s3://mob-emr-test/baihai/test/request_ec_fixed")
    allMofData.filter{case(unitHt, mofData) =>
      val split = unitHt.split("_")
      split(1) == "1" && split(0) != "51309"
    }.repartition(10).saveAsTextFile("s3://mob-emr-test/baihai/test/request_ec_split")

    ////////////////////////////////////////////
    val mofData = sc.textFile("s3://mob-ad/adn/ranker_service/ranker_service.ranker/2019/08/29/*/08").filter{line =>
          try{
            val unitId = line.trim.split("\t").filter{seg => seg.startsWith("key:")}(0).split("#")(3)
            Array("51304", "51309", "117363", "117361").contains(unitId)
          }catch {
            case e: Exception => false
          }
        }
    mofData.filter{line =>
        line.trim.split("\t").filter{seg => seg.startsWith("key:")}(0).split("#")(3) == "51304"
      }.repartition(1).saveAsTextFile("s3://mob-emr-test/baihai/test/mof/51304_2019082908")
    mofData.filter{line =>
        line.trim.split("\t").filter{seg => seg.startsWith("key:")}(0).split("#")(3) == "51309"
      }.repartition(1).saveAsTextFile("s3://mob-emr-test/baihai/test/mof/51309_2019082908")
    mofData.filter{line =>
        line.trim.split("\t").filter{seg => seg.startsWith("key:")}(0).split("#")(3) == "117363"
      }.repartition(1).saveAsTextFile("s3://mob-emr-test/baihai/test/mof/117363_2019082908")
    mofData.filter{line =>
      line.trim.split("\t").filter{seg => seg.startsWith("key:")}(0).split("#")(3) == "117361"
    }.repartition(1).saveAsTextFile("s3://mob-emr-test/baihai/test/mof/117361_2019082908")


    /////////////////

    val allReqData =
      sc.textFile("s3://mob-ad/adn/tracking-v3/request/2019/08/29/*/08")
        .filter{line =>
          try{
            val lineSplit = line.trim.split("\t")
            val unitId = lineSplit(5)

            Array("51304", "51309", "117363", "117361").contains(unitId) && lineSplit.length >= 133
          }catch {
            case e: Exception => false
          }
        }
    allReqData.filter{line =>
      val lineSplit = line.trim.split("\t")
      lineSplit(5) == "51304"
    }.map{line =>
      val lineSplit = line.trim.split("\t")
      "%s\t%s".format("51304", lineSplit(132))
    }.repartition(1)
      .saveAsTextFile("s3://mob-emr-test/baihai/test/51304_req_mofdata")

    allReqData.filter{line =>
      val lineSplit = line.trim.split("\t")
      lineSplit(5) == "51309"
    }.map{line =>
      val lineSplit = line.trim.split("\t")
      "%s\t%s".format("51309", lineSplit(132))
    }.repartition(1)
      .saveAsTextFile("s3://mob-emr-test/baihai/test/51309_req_mofdata")

    allReqData.filter{line =>
      val lineSplit = line.trim.split("\t")
      lineSplit(5) == "117363"
    }.map{line =>
      val lineSplit = line.trim.split("\t")
      "%s\t%s".format("117363", lineSplit(132))
    }.repartition(1)
      .saveAsTextFile("s3://mob-emr-test/baihai/test/117363_req_mofdata")

    allReqData.filter{line =>
      val lineSplit = line.trim.split("\t")
      lineSplit(5) == "117361"
    }.map{line =>
      val lineSplit = line.trim.split("\t")
      "%s\t%s".format("117361", lineSplit(132))
    }.repartition(1)
      .saveAsTextFile("s3://mob-emr-test/baihai/test/117361_req_mofdata")


    ///////////////////////////////////

    val testData =
      sc.textFile("s3://mob-ad/adn/ranker_service/ranker_service.ranker/2019/09/01/*/1[0-2]")
        .filter{line =>
          try{
            val lineSplit = line.trim.split("\t")
            val strategy = lineSplit.filter{seg => seg.startsWith("key:")}(0).split("#")(4).split("\\|")(0)
            Array("1_more_offer_base", "1_more_offer_v2").contains(strategy)
          }catch {
            case e: Exception => false
          }
        }

    testData.filter{line =>
      val lineSplit = line.trim.split("\t")
      val strategy = lineSplit.filter{seg => seg.startsWith("key:")}(0).split("#")(4).split("\\|")(0)
      strategy == "1_more_offer_base"
    }.map{line =>
      val lineSplitIdx = line.trim.split("\t").zipWithIndex
      val startIdx = lineSplitIdx.filter{case(seg, idx) => seg.contains("detailed_features")}(0)._2 +1
      val endIdx = lineSplitIdx.filter{case(seg, idx) => seg.startsWith("score:")}(0)._2
      Array(
        "############################################",
        lineSplitIdx.slice(startIdx, endIdx)
          .map{case(seg, idx) =>
            seg.split(":")(0).split("\001").map{fea => fea.split("=")(0)}.mkString("#")
          }
          .sorted
          .mkString("\n"),
        "##########################################"
      ).mkString("\n")
    }
      .repartition(10)
      .saveAsTextFile("s3://mob-emr-test/baihai/test/1_more_offer_base_combine_schema_2019090110_2019090112")

    testData.filter{line =>
      val lineSplit = line.trim.split("\t")
      val strategy = lineSplit.filter{seg => seg.startsWith("key:")}(0).split("#")(4).split("\\|")(0)
      strategy == "1_more_offer_v2"
    }.map{line =>
      val lineSplitIdx = line.trim.split("\t").zipWithIndex
      val startIdx = lineSplitIdx.filter{case(seg, idx) => seg.contains("detailed_features")}(0)._2 +1
      val endIdx = lineSplitIdx.filter{case(seg, idx) => seg.startsWith("score:")}(0)._2
      Array(
        "############################################",
        lineSplitIdx.slice(startIdx, endIdx)
          .map{case(seg, idx) =>
            seg.split(":")(0).split("\001").map{fea => fea.split("=")(0)}.mkString("#")
          }
          .sorted
          .mkString("\n"),
        "##########################################"
      ).mkString("\n")
    }
      .repartition(10)
      .saveAsTextFile("s3://mob-emr-test/baihai/test/1_more_offer_v2_combine_schema_2019090110_2019090112")



    /////////////////////////////////////////////////
    val strategyData = sc.textFile("s3://mob-ad/adn/ranker_service/ranker_service.ranker/2019/08/29/*/20")
      .filter{line =>
        try{
          val lineSplit = line.trim.split("\t")
          val strategy = lineSplit.filter{seg => seg.startsWith("key:")}(0).split("#")(4).split("\\|")(0)
          Array("1_more_offer_base", "1_more_offer_v2").contains(strategy)
        }catch {
          case e: Exception => false
        }
      }
    strategyData.filter{line =>
      val lineSplit = line.trim.split("\t")
      val strategy = lineSplit.filter{seg => seg.startsWith("key:")}(0).split("#")(4).split("\\|")(0)
      strategy == "1_more_offer_base"
    }.repartition(5).saveAsTextFile("s3://mob-emr-test/baihai/test/more_offer_base")
    strategyData.filter{line =>
      val lineSplit = line.trim.split("\t")
      val strategy = lineSplit.filter{seg => seg.startsWith("key:")}(0).split("#")(4).split("\\|")(0)
      strategy == "1_more_offer_v2"
    }.repartition(5).saveAsTextFile("s3://mob-emr-test/baihai/test/more_offer_v2")

    ////////////////////////////////////////////////////////////////////////////

    ()
  }

  def test(spark: SparkSession, sc: SparkContext): Unit = {

    sc.textFile("s3://mob-ad/adn/ranker_service/ranker_service.ranker/2019/09/04")
      .filter{line =>
        try{
          val lineSplit = line.trim.split("\t")
          val keySplit = lineSplit.filter{seg => seg.trim.startsWith("key:")}(0).split("#")
          val cc = keySplit(2)
          val unit = keySplit(3)
          val strategy = keySplit.last.split("\\|")(0)

          strategy == "1_more_offer_v2" && cc == "us" && unit == "51304" && line.contains("campaign_id:295655656")
        }catch {
          case e: Exception => false
        }
      }
      .repartition(10)
      .saveAsTextFile("s3://mob-emr-test/baihai/test/more_offer_v2_us_51304_295655656")

    sc.textFile("s3://mob-ad/adn/ranker_service/ranker_service.ranker/2019/09/04")
      .filter{line =>
        try{
          val lineSplit = line.trim.split("\t")
          val keySplit = lineSplit.filter{seg => seg.trim.startsWith("key:")}(0).split("#")
          val cc = keySplit(2)
          val unit = keySplit(3)
          val strategy = keySplit.last.split("\\|")(0)

          strategy == "1_more_offer_base" && cc == "us" && unit == "51304" && line.contains("campaign_id:295655656")
        }catch {
          case e: Exception => false
        }
      }
      .repartition(10)
      .saveAsTextFile("s3://mob-emr-test/baihai/test/more_offer_base_us_51304_295655656")

    ////////////////////////////////////////////////

    sc.textFile("s3://mob-emr-test/baihai/m_sys_model/mvp_mining/exp1/user_feature_daily/libsvm_test/20190925").count()
    sc.textFile("s3://mob-emr-test/baihai/test/devFea_test/20190925").count()


    sc.textFile("s3://mob-emr-test/baihai/m_sys_model/mvp_mining/exp1/user_feature_daily/libsvm_test/20190925")
      .map{line =>
        val lineSplit = line.trim.split("\t", 2)
        (lineSplit(0), lineSplit(1))
      }
      .join(
        sc.textFile("s3://mob-emr-test/baihai/test/devFea_test/20190925")
          .map{line =>
            val lineSplit = line.trim.split("\t", 2)
            (lineSplit(0), lineSplit(1))
          }
      )
      .map{case(devId, (fea1, fea2)) =>
          Array(
            "##############################",
            devId,
            fea1,
            fea2,
            "##############################"
          ).mkString("\n")
      }
      .repartition(800)
      .saveAsTextFile("s3://mob-emr-test/baihai/test/devFeaValid/20190925")

    ////////////////////////

    sc.textFile("s3://mob-emr-test/baihai/m_sys_model/more_offer/exp1/more_offer_model/filt/20191022/2019102201").map{line =>
        val lineSplit = line.trim.split("\t");
        (lineSplit(0), lineSplit(1).toDouble)
    }.sortBy({case(fea, w) => w}, false).take(1000).foreach(println)



    //////////////////////////////////////////////////

    val df = spark.read.format("libsvm").load("s3://mob-emr-test/baihai/test/testLibsvm")
    df.show()

    /////////////////////////////

    val data = sc.textFile("s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly/20191022")
    Array("17123", "146172", "112222", "59430", "124385", "153610", "78286").foreach{unitId =>
      data.filter{line => line.trim.split("\002")(9) == unitId}.repartition(10).saveAsTextFile("s3://mob-emr-test/baihai/test/rs1_rs2/%s".format(unitId))
    }

    ///////////////////////////


    sc.textFile("s3://mob-ad/adn/ranker_service/ranker_service.ranker/2019/10/20").filter{line =>line.contains("video_endcard=null")}.repartition(200).saveAsTextFile("s3://mob-emr-test/baihai/test/old_rs_log_video_1")

    sc.textFile("s3://mob-ad/adn/ranker_service/ranker_service.ranker/2019/10/20").filter{line =>line.contains("ext_playable=null")}.repartition(100).saveAsTextFile("s3://mob-emr-test/baihai/test/old_rs_log_playable_1")



    ()
  }

}
