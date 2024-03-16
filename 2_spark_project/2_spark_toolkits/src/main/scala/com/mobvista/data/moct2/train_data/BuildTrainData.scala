package com.mobvista.data.moct2.train_data

import org.apache.spark.SparkContext

class BuildTrainData {
  def perform(sc: SparkContext, date: String): Unit = {
    val path = "s3://mob-emr-test/yangjingyun/m_model_online/neg_sampling/train_data_less_hourly/%s/*".format(date)
    val output = "s3://mob-emr-test/wanjun/m_sys_model/moct2/v1/train_data_daily_v1/%s".format(date)
    println(s"log path = " + path)
    println(s"output path = " + output)

    val tag_map = sc.textFile("s3://mob-emr-test/wanjun/m_sys_model/offline_data/m_ftrl_feature/m_ftrl_feature_new.dat")
      .map(line => {
        val arr = line.split("\t")
        if (arr.length != 2) {
          ("none", "none")
        }
        else {
          val key = arr(0)
          var value = arr(1).split("\\|")
          var tags = ""
          if (key.contains("campaign_id")) {
            var r = value.filter(_.contains("dmp_tag="))
            if (r.length == 0) {
              tags = ""
            }
            else {
              tags = r(0)
            }
          }
          else {
            var r = value.filter(_.contains("appid_category="))
            if (r.length == 0) {
              tags = ""
            }
            else {
              tags = r(0)
            }
          }
          var tag_names = ""
          var tag_arr = tags.split("#")
          for (d <- tag_arr) {
            var brr = d.split("\\=")
            if (brr.length == 2) {
              tag_names += brr(1) + "\001"
            }
          }
          if (tag_names.length > 1) {
            tag_names = tag_names.substring(0, tag_names.length - 1)
          }
          else {
            tag_names = ""
          }
          (key, tag_names)
        }
      })
      .filter(line => line._2 != "none")
      .collectAsMap()
    val pc = sc.broadcast(tag_map)

    val train_data = sc.textFile(path)
      .map(line => {
        val arr = line.split("\002")
        val app_id = arr(8)
        val campaign_id = arr(20)
        var dmp_tag = arr(26).split("\001").toSet
        val app_cate_name = pc.value.getOrElse("app_id\001" + app_id, "none")
        val dmptag_name = pc.value.getOrElse("campaign_id\001" + campaign_id, "none")
        if (app_cate_name.length > 1) {
          arr(7) = app_cate_name
        }
        if (dmptag_name.length > 1) {
          arr(26) = dmptag_name
        }
        val final_result = arr.mkString("\002")
        final_result
      })
      .repartition(400)
      .saveAsTextFile(output)
  }
}
