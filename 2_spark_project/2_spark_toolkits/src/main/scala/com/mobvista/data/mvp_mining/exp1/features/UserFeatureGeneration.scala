package com.mobvista.data.mvp_mining.exp1.features

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

class UserFeatureGeneration {

  def generate(spark: SparkSession, date: String): Unit = {

    val sc = spark.sparkContext
    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val dateTime = sdf.parse(date).getTime
    val daySpan = 7
    val inputPath =
      (0 until daySpan).map{i =>
        "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly/%s".format(sdf.format(new Date(dateTime - i * 86400 * 1000)))
      }
        .mkString(",")
    val feaIdxPath = "s3://mob-emr-test/baihai/m_sys_model/mvp_mining/exp1/user_feature/feature_index/%s".format(date)
    val featureContentPath = "s3://mob-emr-test/baihai/m_sys_model/mvp_mining/exp1/user_feature/feature_content/%s".format(date)

    println("inputPath: %s".format(inputPath))
    println("feaIdxPath: %s".format(feaIdxPath))
    println("featureContentPath: %s".format(featureContentPath))

    val feaNameArr = Array("language", "country_code", "ext_brand", "ext_model", "platform", "network_type", "os_version", "sdk_version", "idfa", "gaid", "dev_install_pkg", "screen_ratio", "screen_size")

    val devFeaSetArr =
      sc.textFile(inputPath)
        .map{line =>
          val lineSplit = line.trim.split("\002")

          val dev_id = lineSplit(38)

          val feaSetArr =
            Array(
              Set(lineSplit(5)), // language
              Set(lineSplit(6)), // cc
              Set(lineSplit(338)), // extBrand
              Set(lineSplit(339)), // extModel
              Set(lineSplit(13)), // platform
              Set(lineSplit(16)), // networkType
              Set(lineSplit(17)), // osVersion
              Set(lineSplit(18)), // sdkVersion
              Set(lineSplit(300)), // idfa
              Set(lineSplit(301)), // gaid
              lineSplit(302).split("\001").toSet, // devInstallPkg
              Set(lineSplit(304)), // screenRatio
              Set(lineSplit(314)) // screenSize
            )

          (dev_id, feaSetArr)
        }
        .reduceByKey{(feaSetArr1: Array[Set[String]], feaSetArr2: Array[Set[String]]) =>
          (0 until feaSetArr1.length).map{i => feaSetArr1(i) ++ feaSetArr2(i)}.toArray
        }
        .mapValues{feaSetArr =>

          val Array(lang, cc, extBrand, extModel, platform, networkType, osVersion, sdkVersion, idfa, gaid, devInstallPkg, screenRatio, screenSize) = feaSetArr

          val lang2 =
            if(lang.size > 1 && lang.contains("0"))
              lang - "0"
            else if(lang.size == 1 && lang.contains("0"))
              Set("none")
            else
              lang
          val cc2 =
            if(cc.size > 1 && cc.contains("0"))
              cc - "0"
            else if(cc.size == 1 && cc.contains("0"))
              Set("none")
            else
              cc
          val extBrand2 =
            if(extBrand.size > 1 && extBrand.contains("0"))
              extBrand - "0"
            else if(extBrand.size == 1 && extBrand.contains("0"))
              Set("none")
            else
              extBrand
          val extModel2 =
            if(extModel.size > 1 && extModel.contains("0"))
              extModel - "0"
            else if(extModel.size == 1 && extModel.contains("0"))
              Set("none")
            else
              extModel
          val devInstallPkg2 =
            if(devInstallPkg.size > 1 && devInstallPkg.contains("0"))
              devInstallPkg - "0"
            else if(devInstallPkg.size == 1 && devInstallPkg.contains("0"))
              Set("none")
            else
              devInstallPkg
          val screenRatio2 =
            if(screenRatio.size > 1 && screenRatio.contains("0"))
              screenRatio - "0"
            else if(screenRatio.size == 1 && screenRatio.contains("0"))
              Set("none")
            else
              screenRatio
          val screenSize2 =
            if(screenSize.size > 1 && screenSize.contains("0x0"))
              screenSize - "0x0"
            else if(screenSize.size == 1 && screenSize.contains("0x0"))
              Set("none")
            else
              screenSize

          Array(lang2, cc2, extBrand2, extModel2, platform, networkType, osVersion, sdkVersion, idfa, gaid, devInstallPkg2, screenRatio2, screenSize2)
            .map{feaSet =>
              if(feaSet.size > 1 && feaSet.contains("none"))
                feaSet - "none"
              else
                feaSet
            }
        }
        .filter{case(dev_id, feaSetArr) =>
          val sizeThresArray = Array(5, 10, 10, 10, 1, 5, 5, 10, 1, 1, 90, 4, 10)
          feaSetArr.zip(sizeThresArray)
            .map{case(feaSet, thres) => feaSet.size <= thres}
            .reduce{(bool1: Boolean, bool2: Boolean) => bool1 && bool2}
        }


    val devFeaArr =
      devFeaSetArr.mapValues{feaSetArr =>
        feaNameArr
          .zip(feaSetArr)
          .map{case(feaName, feaSet) => feaSet.toArray.map{fea => feaName + "_" + fea}}
          .reduceLeft{(arr1: Array[String], arr2: Array[String]) => arr1 ++ arr2}
      }
        .repartition(2000)
    // debug
    /*devFeaArr.map{case(devId, feaArr) =>
        devId +
        "\t" +
        feaArr.mkString("\t")
    }
      .repartition(500)
      .saveAsTextFile("s3://mob-emr-test/baihai/test/devFea/%s".format(date))*/


    val feaNameValCnt =
      devFeaArr
        .flatMap{case(devId, feaArr) =>
          feaArr.map{fea => (fea, 1)}
        }
        .reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}
        .filter{case(feaNameVal, cnt) => cnt >= 50 * daySpan}
        .repartition(50)


    val (feaCntArr, feaIdxArr) =
      (0 until feaNameArr.length).map{i =>
        val feaName = feaNameArr(i)
        val targetFeaNameVal =
          feaNameValCnt
            .filter{case(feaNameVal, cnt) => feaNameVal.startsWith(feaName + "_")}
            .zipWithIndex()
            .map{case((feaNameVal, cnt), idx) => (feaNameVal, cnt, idx)}
        val targetCnt = targetFeaNameVal.count()

        (targetCnt.toInt, targetFeaNameVal)
      }.unzip


    val offsetBuf = new ArrayBuffer[Int]()
    offsetBuf.append(0)
    (1 until feaNameArr.length).foreach{i =>
      offsetBuf.append(offsetBuf(i - 1) + feaCntArr(i - 1))
    }
    val feaOffsetArray = offsetBuf.toArray
    val feaNameValIdx =
      sc.union(
        (0 until feaNameArr.length).map{i =>
          val targetOffset = feaOffsetArray(i)
          feaIdxArr(i).map{case(feaNameVal, cnt, idx) => (feaNameVal, cnt, idx+targetOffset)}
        }
      )
//        .repartition(1)
        .sortBy({case(feaNameVal, cnt, idx) => cnt}, false)
        .map{case(feaNameVal, cnt, idx) => (feaNameVal, idx)}
    feaNameValIdx.sortBy({case(feaNameVal, idx) => idx}, true, 10).map{case(feaNameVal, idx) => "%s\t%d".format(feaNameVal, idx+1)}.saveAsTextFile(feaIdxPath)


    val topFeaIdxMapBC = sc.broadcast(feaNameValIdx.take(20000).toMap)
    val feaDevIdx =
      devFeaArr.mapPartitions{partition =>
        val topFeaIdxMap = topFeaIdxMapBC.value
        partition.flatMap{case(devId, feaArr) =>
          feaArr.map{fea =>
            (fea, (devId, topFeaIdxMap.getOrElse(fea, -1l)))
          }
        }
      }


    val feaDevIdxExist = feaDevIdx.filter{case(fea, (devId, idx)) => idx >= 0 }
    val feaDevIdxJoin =
      feaDevIdx.filter{case(fea, (devId, idx)) => idx == -1}
        .join(feaNameValIdx)
        .map{case(fea, ((devId, invalidIdx), idx)) =>
          (fea, (devId, idx))
        }
    sc.union(Array(feaDevIdxExist, feaDevIdxJoin))
      .map{case(fea, (devId, idx)) => (devId, Array((idx, fea)))}
      .reduceByKey{(idxFeaArr1: Array[(Long, String)], idxFeaArr2: Array[(Long, String)]) =>
        idxFeaArr1 ++ idxFeaArr2
      }
      .map{case(devId, idxFeaArr) =>
        devId +
          " " +
          idxFeaArr.sortWith{case((idx1, fea1), (idx2, fea2)) => idx1 < idx2}
            .map{case(idx, fea) => "%d:%d".format(idx + 1, 1)}
            .mkString(" ")
      }
      .repartition(200)
      .saveAsTextFile(featureContentPath)


    ()
  }

}
