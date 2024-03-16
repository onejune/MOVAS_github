package com.mobvista.data.mvp_mining.exp1.training_data

import org.apache.spark.SparkContext

class TrainDataGeneration {

  def generate(sc: SparkContext, date: String, pkgNameList: String): Unit = {

    val featurePath = "s3://mob-emr-test/baihai/m_sys_model/mvp_mining/exp1/user_feature/feature_content/%s".format(date)
    println("featurePath: %s".format(featurePath))

    val userFeature =
      sc.textFile(featurePath)
        .map{line =>
          val lineSplit = line.trim.split("\t", 2)
          (lineSplit(0), lineSplit(1))
        }

    pkgNameList.split(",").foreach{pkgName =>

      val mvpUserPath = "s3://mob-emr-test/baihai/m_sys_model/mvp_mining/exp1/%s/mvp_user_list".format(pkgName)
      val posSamplePath = "s3://mob-emr-test/baihai/m_sys_model/mvp_mining/exp1/%s/positive_sample/%s".format(pkgName, date)
      val negSamplePath = "s3://mob-emr-test/baihai/m_sys_model/mvp_mining/exp1/%s/negtive_sample/%s".format(pkgName, date)
      val mvpTrainDataPath = "s3://mob-emr-test/baihai/m_sys_model/mvp_mining/exp1/%s/train_data/%s".format(pkgName, date)
      println("%s mvpUserPath: %s".format(pkgName, mvpUserPath))
      println("%s posSamplePath: %s".format(pkgName, posSamplePath))
      println("%s negSamplePath: %s".format(pkgName, negSamplePath))
      println("%s trainDataPath: %s".format(pkgName, mvpTrainDataPath))

      val posSample =
        sc.textFile(mvpUserPath)
          .map{line => (line.trim.toLowerCase(), 1.0)}
          .join(userFeature)
          .map{case(devId, (label, feature)) => (devId, feature)}
      val posCnt = posSample.count()
      println("%s posCnt: %d".format(pkgName, posCnt))
      posSample.map{case(devId, feature) => Array(devId, feature).mkString("\t")}.repartition((posCnt / 10000).toInt + 1).saveAsTextFile(posSamplePath)

      val targetNegCnt = posCnt * 10
      val allNegSample = userFeature.subtractByKey(posSample)
      val allNegCnt = allNegSample.count()
      val sampleRatio = targetNegCnt.toDouble / allNegCnt
      val negSample = allNegSample.sample(false, sampleRatio)
      val actNegCnt = negSample.count()
      println("%s negCnt: %s".format(pkgName, actNegCnt))
      negSample.map{case(devId, feature) => Array(devId, feature).mkString("\t")}.repartition((actNegCnt / 10000).toInt + 1).saveAsTextFile(negSamplePath)

      sc.union(
        Array(
          posSample.map{case(devId, feature) => Array("1.0", feature).mkString("\t")},
          negSample.map{case(devId, feature) => Array("0.0", feature).mkString("\t")}
        )
      )
        .repartition(((posCnt + actNegCnt) / 50000).toInt + 1)
        .saveAsTextFile(mvpTrainDataPath)

    }

    ()
  }

}
