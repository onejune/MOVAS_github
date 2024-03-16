package com.mobvista.data.creative3.exp1.day

import org.apache.spark.SparkContext

class DataGeneration {

  def generate(sc: SparkContext, date: String): Unit = {

    val inputPath = "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly/%s".format(date)
    val allOutPath = "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_daily/%s".format(date)
    val appwallOutPath = "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/appwall_train_data_uniq_daily/%s".format(date)


    val allData = sc.textFile(inputPath)
    allData
      .repartition(1000)
      .saveAsTextFile(allOutPath)

    allData.filter{line =>
      val lineSplit = line.trim.split("\002")
      val adType = lineSplit(19)

      adType == "appwall"
    }
      .repartition(100)
      .saveAsTextFile(appwallOutPath)





    ////
//    val mofUnitBC = sc.broadcast(sc.textFile("s3://mob-emr-test/baihai/test/more_offer_id").map(_.trim).collect().toSet)
//    val timeList = Array("2019072316", "2019072315", "2019072314", "2019072313")
//    timeList.map{time =>
//      val date = time.substring(0, 8)
//      val path = "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/train_data_uniq_hourly/%s/%s".format(date, time)
//
//      val allData = sc.textFile(path)
//      val allCnt = allData.count()
//
//      val appwall = allData.filter{line =>
//        line.trim.split("\002")(19) == "appwall"
//      }
//      val appwallCnt = appwall.count()
//
//      val mof =
//        appwall.filter{line =>
//          val unitId = line.trim.split("\002")(9)
//          mofUnitBC.value.contains(unitId)
//        }
//      val mofCnt = mof.count()
//
//      val fixed_51304 = mof.filter{line => line.trim.split("\002")(9) == "51304"}
//      val fixedCnt_51304 = fixed_51304.count()
//
//      val fixed_51309 = mof.filter{line => line.trim.split("\002")(9) == "51309"}
//      val fixedCnt_51309 = fixed_51309.count()
//
//      val split = mof.filter{line => !Set("51304", "51309").contains(line.trim.split("\002")(9))}
//      val splitCnt = split.count()
//
//      "date: %s, allCnt: %d, appwallCnt: %d, mofCnt: %d, fixedCnt_51304: %d, fixedCnt_51309: %d, splitCnt: %d".format(date, allCnt, appwallCnt, mofCnt, fixedCnt_51304, fixedCnt_51309, splitCnt)
//    }
//      .foreach(println)






    ()
  }

}
