package com.mobvista.data.creative3.check

import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer

class CheckValyria {

  def check(sc: SparkContext, dateHour: String): Unit = {

    val date = dateHour.substring(0, 8)
    val valyPath = "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly_valyria/%s/%s".format(date, dateHour)
    val creative3Path = "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly/%s/%s".format(date, dateHour)
    val diffPath = "s3://mob-emr-test/baihai/test/check_valyria_v2/diff/%s/%s".format(date, dateHour)
    val statsPath = "s3://mob-emr-test/baihai/test/check_valyria_v2/stats/%s/%s".format(date, dateHour)
    val columnPath = "s3://mob-emr-test/baihai/test/column_name"
    println("valyPath: %s".format(valyPath))
    println("creative3Path: %s".format(creative3Path))
    println("diffPath: %s".format(diffPath))
    println("statsPath: %s".format(statsPath))
    println("columnPath: %s".format(columnPath))

    val infoArray = new ArrayBuffer[String]()

    val columnNameBC =
      sc.broadcast(
        sc.textFile(columnPath)
          .collect()
          .map{line =>
            val lineSplit = line.split(" ")
            (lineSplit(0).toInt, lineSplit(1))
          }
          .sortBy{case(idx, feat) => idx}
          .map{case(idx, feat) => feat}
      )


    val valyData =
      sc.textFile(valyPath)
        .map{line =>
          val lineSplit = line.trim.split("\002")
          val extra5 = lineSplit(2).split("\\|")(0)
          val camId = lineSplit(20)

          (extra5+"_"+camId, line.trim)
        }
    val valyCnt = valyData.count()
    val valyInfo = "valyCnt: %d".format(valyCnt)
    println(valyInfo)
    infoArray.append(valyInfo)

    val creative3Data =
      sc.textFile(creative3Path)
        .map{line =>
          val lineSplit = line.trim.split("\002")
          val extra5 = lineSplit(2).split("\\|")(0)
          val camId = lineSplit(20)

          (extra5 + "_" + camId, line.trim)
        }
    val creative3Cnt = creative3Data.count()
    val creative3Info = "creative3Cnt: %d".format(creative3Cnt)
    println(creative3Info)
    infoArray.append(creative3Info)

    val joinData = valyData.join(creative3Data)
    val joinCnt = joinData.count()
    val joinInfo = "joinCnt: %d".format(joinCnt)
    println(joinInfo)
    infoArray.append(joinInfo)

    val diffData =
      joinData.mapPartitions{part =>

        val columnName = columnNameBC.value
        part.map{case(key, (valy, creative3)) =>

          val valyMap =
            columnName.zip(valy.split("\002"))
              .map{case(fea, value) => (fea, value.split("\001").toSet)}
              .toMap

          val creative3Map =
            columnName.zip(creative3.split("\002"))
              .map{case(fea, value) => (fea, value.split("\001").toSet)}
              .toMap

          val tagMap =
            columnName
              .map{fea =>
                val valySet = valyMap.getOrElse(fea, Set())
                val creative3Set = creative3Map.getOrElse(fea, Set())
                if(valySet.diff(creative3Set).size == 0 && creative3Set.diff(valySet).size == 0)
                  (fea, true)
                else
                  (fea, false)
              }.toMap

          (key, (valy, valyMap, tagMap, creative3, creative3Map))
        }
      }
        .filter{case(key, (valy, valyMap, tagMap, creative3, creative3Map)) =>
          !tagMap.values.reduce{(bool1: Boolean, bool2: Boolean) => bool1 && bool2}
        }
    val diffCnt = diffData.count()
    val diffInfo = "diffCnt: %d".format(diffCnt)
    println(diffInfo)
    infoArray.append(diffInfo)

    val diffRate = diffCnt.toDouble / joinCnt
    val rateInfo = "diffRate: %f".format(diffRate)
    println(rateInfo)
    infoArray.append(rateInfo)

    val diffOutData =
      diffData
        .map{case(key, (valy, valyMap, tagMap, creative3, creative3Map)) =>
          Array(
            "########################\t" + key + "\t###########################",
            tagMap.map{case(fea, tag) =>
              if(tag)
                ""
              else
                "%s\tvaly_value: %s\tcreative3_value: %s".format(fea, valyMap.getOrElse(fea, Set()).mkString(","), creative3Map.getOrElse(fea, Set()).mkString(","))
            }
              .filter{r => r != ""}
              .mkString("\n"),
            valy,
            creative3,
            "##################################################################################################"
          ).mkString("\n")
        }
    diffOutData.repartition(diffCnt.toInt / 200000 + 1).saveAsTextFile(diffPath)

    val feaDiffCnt =
      diffData
        .flatMap{case(key, (valy, valyMap, tagMap, creative3, creative3Map)) =>
          tagMap
            .toArray
            .filter{case(fea, is_consistent) => !is_consistent}
            .map{case(fea, is_consistent) => (fea, 1)}
        }
        .reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}
        .map{case(fea, diffCnt) =>
          "%s\t%d\t%f".format(fea, diffCnt, diffCnt.toDouble / joinCnt)
        }
        .collect()
    infoArray.appendAll(feaDiffCnt)

    sc.parallelize(infoArray, 1).saveAsTextFile(statsPath)

    ()
  }

}
