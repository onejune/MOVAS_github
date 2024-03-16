package com.mobvista.data.creative3.check

import org.apache.spark.SparkContext

import scala.collection.mutable

class PredictionCheck {

  def extract(sc: SparkContext, date: String): Unit = {

    val year = date.substring(0, 4)
    val month = date.substring(4, 6)
    val day = date.substring(6, 8)
    val onlinePath = "s3://mob-ad/adn/ranker_service/ranker_service.ranker/%s/%s/%s".format(year, month, day)
    val offlinePath = "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly/%s".format(date)

    val diffOutPath = "s3://mob-emr-test/baihai/PredictionCheck/diff/%s".format(date)
    val infoOutPath = "s3://mob-emr-test/baihai/PredictionCheck/info/%s".format(date)

    val columnNamePath = "s3://mob-emr-test/baihai/test/column_name"
    val columnNameBC =
      sc.broadcast(
        sc.textFile(columnNamePath)
          .collect()
          .map{line =>
            val lineSplit = line.split(" ")
            (lineSplit(0).toInt, lineSplit(1))
          }
          .sortBy{case(idx, feat) => idx}
          .map{case(idx, feat) => feat}
      )

    val onlineData =
    sc.textFile(onlinePath)
      .filter{line =>
        try{
          val lineSplit = line.trim.split("\t")
          val strategy = lineSplit.filter{seg => seg.startsWith("key:")}(0).split("#").last.split("\\|")(0).split("_", 2)(1)
          val unitId = lineSplit.filter{seg => seg.startsWith("key:")}(0).split("#")(3)

          strategy == "mooc_exp1" &&
          Set("4595", "14419", "48285", "114467").contains(unitId)
        }catch {
          case e: Exception => false
        }
      }
      .map{line =>
        try{
          val lineSplit = line.trim.split("\t")
          val reqId =
            lineSplit
              .filter{seg => seg.startsWith("key:")}(0)
              .split(":")(1)
              .split("#")(0)
          val camId =
            lineSplit
              .filter{seg => seg.startsWith("[algo_info]")}(0)
              .split(":")(1)
              .split(",")(0)

          (reqId + "_" + camId, line.trim)
        }catch {
          case e: Exception => ("null", "null")
        }
      }
      .filter{case(key, line) => key != "null"}
    val onlineCnt = onlineData.count()
    println("date: %s, onlineCnt: %d".format(date, onlineCnt))

    val offlineData =
    sc.textFile(offlinePath)
      .filter{line =>
        val lineSplit = line.trim.split("\002")
        val unitId = lineSplit(9)

        Set("4595", "14419", "48285", "114467").contains(unitId)
      }
      .map{line =>
        val lineSplit = line.trim.split("\002")
        val reqId = lineSplit(2).split("\\|")(0)
        val camId = lineSplit(20)
        (reqId + "_" + camId, line.trim)
      }
    val offlineCnt = offlineData.count()
    println("date: %s, offlineCnt: %d".format(date, offlineCnt))

    val allJoinData =
    onlineData.join(offlineData)
      .map { case (key, (online, offline)) =>
        try{
          val onlineSplit = online.split("\t")
          val onlineAlgo = onlineSplit.filter{seg => seg.startsWith("[algo_info]")}(0).split(":")(1).split(";")(0).split(",")
          val modelVersion = onlineAlgo(7)
          val ivr = onlineAlgo(3)

          (key, (online, Array(offline, modelVersion, ivr).mkString("\002")))
        }catch {
          case e1: ArrayIndexOutOfBoundsException =>
            ("null", ("null", "null"))
        }
      }
      .filter{case(key, (online, offline)) => key != "null"}
    val allJoinCnt = allJoinData.count()
    println("date: %s, allJoinCnt: %d".format(date, allJoinCnt))

    val validData =
    allJoinData
      .mapPartitions{partition =>

        val columnName = columnNameBC.value

        partition.map{case(key, (online, offline)) =>

          val onlineSplitIdx = online.split("\t").zipWithIndex
          val onlineStartIdx = onlineSplitIdx.filter{case(seg, idx) => seg.contains("detailed_features")}(0)._2
          val onlineEndIdx =
            try{
              onlineSplitIdx.filter{case(seg, idx) => seg.startsWith("score:")}(0)._2
            }catch {
              case e1: ArrayIndexOutOfBoundsException =>
                -1
            }
          val onlineFeaMap =
            if(onlineEndIdx == -1)
              Map[String, Set[String]]()
            else{
              onlineSplitIdx.slice(onlineStartIdx + 1, onlineEndIdx)
                .map{case(seg, idx) =>
                  seg.split(":")(0)
                    .split("\001")
                    .map{fea =>
                      val feaSplit = fea.split("=")
                      (feaSplit(0),
                        try{
                          Set(feaSplit(1))
                        }catch {
                          case e: Exception => Set[String]()
                        }
                      )
                    }
                    .toMap
                }
                .reduceLeft{(map1: Map[String, Set[String]], map2: Map[String, Set[String]]) =>
                  val map = new mutable.HashMap[String, Set[String]]()
                  map1.foreach{case(fea, valSet) =>
                    map(fea) = valSet
                  }
                  map2.foreach{case(fea, valSet) =>
                    map(fea) = map.getOrElse(fea, Set()) ++ valSet
                  }
                  map.toMap
                }
            }

          val offlineSplit = offline.split("\002")
          val offlineFeaMap =
            columnName
              .zip(offlineSplit.slice(0, offlineSplit.length-2))
              .map{case(fea, value) => (fea, value.split("\001").toSet)}
              .toMap

          val tagMap =
            onlineFeaMap.map{case(fea, onValSet) =>
              val offValSet = offlineFeaMap.getOrElse(fea, Set())
              if(offValSet.size == 0)
                (fea, true)
              else{
                if(onValSet.diff(offValSet).size == 0 && offValSet.diff(onValSet).size == 0)
                  (fea, true)
                else
                  (fea, false)
              }
            }
          (key, (online, onlineFeaMap, tagMap, offline, offlineFeaMap))
        }
      }
      .filter{case(key, (online, onlineFeaMap, tagMap, offline, offlineFeaMap)) =>
        try{
          val onlineImpreCamId = online.split("\t")
            .filter{seg => seg.startsWith("[algo_info]")}(0)
            .split(":")(1)
            .split(",")(0)
          val onlinePrintCamId = onlineFeaMap("campaign_id").head

          onlineImpreCamId == onlinePrintCamId
        }catch {
          case e1: NoSuchElementException =>
            false
        }
      }
    val validCnt = validData.count()
    println("date: %s, validCnt: %d".format(date, validCnt))

    val diffData =
      validData
        .filter{case(key, (online, onlineFeaMap, tagMap, offline, offlineFeaMap)) =>
          !tagMap.values.reduce{(bool1: Boolean, bool2: Boolean) => bool1 && bool2}
        }
    val diffCnt = diffData.count()
    println("date: %s, diffCnt: %d".format(date, diffCnt))

    val diffRate = diffCnt.toDouble / validCnt
    println("date: %s, diffRate: %f".format(date, diffRate))

    val diffOutData =
      diffData
        .map{case(key, (online, onlineFeaMap, tagMap, offline, offlineFeaMap)) =>
          Array(
            "########################\t" + key + "\t###########################",
            tagMap.map{case(fea, tag) =>
              if(tag)
                ""
              else{
                "%s\tonline_value: %s\toffline_value: %s".format(fea, onlineFeaMap(fea).mkString(","), offlineFeaMap(fea).mkString(","))
              }
            }
              .filter{r => r != ""}
              .mkString("\n"),
            Array(key, offline).mkString("\t"),
            Array(key, online).mkString("\t"),
            "##################################################################################################"
          ).mkString("\n")
        }
    diffOutData.repartition(diffCnt.toInt / 5000 + 1 ).saveAsTextFile(diffOutPath)

    val feaDiffCntRatio =
      diffData
        .flatMap{ case (key, (online, onlineFeaMap, tagMap, offline, offlineFeaMap)) =>
          tagMap
            .toArray
            .filter{case(fea, is_consistent) => !is_consistent}
            .map{case(fea, is_consistent) => (fea, 1)}
        }
        .reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}
        .map{case(feature, diffCnt) =>
          "%s\tinconsistent_cnt:%d\tcoverage:%f".format(feature, diffCnt, diffCnt.toDouble / validCnt)
        }
    feaDiffCntRatio.repartition(1).saveAsTextFile(infoOutPath)


    ()
  }

}
