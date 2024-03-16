package com.mobvista.data.creative3.check

import org.apache.spark.SparkContext

class ConsistentCheck {

  def extractData(sc: SparkContext): Unit = {

    val inPath = "s3://mob-emr-test/guangxue/case"
    val outBasePath = "s3://mob-emr-test/baihai/case"

    val modelVersionData =
      sc.textFile(inPath)
        .filter{line =>

          val lineSplit = line.trim.split("\002")
          val strategy = lineSplit(2).split(";")(7).split("-")(0).split("_", 2)(1)

          strategy == "mooc_exp1"
        }
        .map{line =>
          val lineSplit = line.trim.split("\002")
          val extAlgo = lineSplit.last.split(";")(0).split(",")
          val modelVersion = extAlgo(7)
          val onlineIvr = extAlgo(29).split("\004")(1).toDouble / extAlgo(2).toDouble / 1000
          lineSplit(340) = modelVersion

          (modelVersion, Array(lineSplit.mkString("\002"), onlineIvr).mkString("\002"))
        }.cache()

    val modelVerCnt =
      modelVersionData.map{case(modelVersion, line) => (modelVersion, 1)}
        .reduceByKey{case(cnt1, cnt2) => cnt1 + cnt2}
        .collect()

    modelVerCnt.map{case(modelVersion, cnt) => "%s: %d".format(modelVersion, cnt)}.foreach(println)

    modelVerCnt.map{case(modelVersion, cnt) => modelVersion}
      .foreach{modelVer =>
        val outVerPath = "%s/%s".format(outBasePath, modelVer)
        modelVersionData
          .filter{case(modelVersion, line) => modelVersion == modelVer}
          .map{case(modelVersion, line) => line}
          .repartition(10)
          .saveAsTextFile(outVerPath)
      }

    ()
  }

}
