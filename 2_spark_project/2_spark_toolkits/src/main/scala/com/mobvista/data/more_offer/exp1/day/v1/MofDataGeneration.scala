package com.mobvista.data.more_offer.exp1.day.v1

import java.text.SimpleDateFormat
import java.util.{Locale, TimeZone}

import org.apache.spark.SparkContext

class MofDataGeneration {

  def generate(sc: SparkContext, date: String): Unit = {

    val year = date.substring(0, 4)
    val month = date.substring(4, 6)
    val day = date.substring(6, 8)
    val sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))

    val nginxPath = "s3://mob-ad/adn/nginxlog/tracking/%s/%s/%s".format(year, month, day)
    val requestPath = "s3://mob-ad/adn/tracking-v3/request/%s/%s/%s".format(year, month, day)
    val impressionPath = "s3://mob-ad/adn/tracking-v3/impression/%s/%s/%s".format(year, month, day)

    val nginxBeforeJoinPath = "s3://mob-emr-test/baihai/test/more_offer/nginxBeforeJoin/%s".format(date)
    val requestBeforJoinPath = "s3://mob-emr-test/baihai/test/more_offer/requestBeforeJoin/%s".format(date)
    val nginxNoJoinPath = "s3://mob-emr-test/baihai/test/more_offer/nginxNoJoin/%s".format(date)
    val nginxRequestJoinPath = "s3://mob-emr-test/baihai/test/more_offer/nginxRequestJoin/%s".format(date)
    val requestNoJoinPath = "s3://mob-emr-test/baihai/test/more_offer/requestNoJoin/%s".format(date)

    val impreBeforeJoinPath = "s3://mob-emr-test/baihai/test/more_offer/impreBeforeJoin/%s".format(date)
    val secondReqNoJoinPath = "s3://mob-emr-test/baihai/test/more_offer/secondReqNoJoin/%s".format(date)
    val secondJoinPath = "s3://mob-emr-test/baihai/test/more_offer/secondJoin/%s".format(date)
    val impreNoJoinPath = "s3://mob-emr-test/baihai/test/more_offer/impreNoJoin/%s".format(date)

    println("nginxPath: %s".format(nginxPath))
    println("requestPath: %s".format(requestPath))
    println("impressionPath: %s".format(impressionPath))
    println("nginxBeforeJoinPath: %s".format(nginxBeforeJoinPath))
    println("requestBeforJoinPath: %s".format(requestBeforJoinPath))
    println("nginxNoJoinPath: %s".format(nginxNoJoinPath))
    println("nginxRequestJoinPath: %s".format(nginxRequestJoinPath))
    println("requestNoJoinPath: %s".format(requestNoJoinPath))
    println("impreBeforeJoinPath: %s".format(impreBeforeJoinPath))
    println("secondReqNoJoinPath: %s".format(secondReqNoJoinPath))
    println("secondJoinPath: %s".format(secondJoinPath))
    println("impreNoJoinPath: %s".format(impreNoJoinPath))

    val nginxData =
      sc.textFile(nginxPath)
        .filter{row =>
          row.contains("idfa=") && row.contains("unit_id=") && row.contains("mof_data=") && row.contains("/openapi/ad/v3")
        }
        .map{nginx =>
          val line = nginx.trim
          val clientIp = line.substring(0, line.indexOf("-")).trim.split(",")(0)
          val timeStr = (sdf.parse(line.substring(line.indexOf("[")+1, line.indexOf("]")).split(" ")(0)).getTime / 1000).toString
          val url = line.split(" ").filter(_.startsWith("/openapi/ad/v3"))(0)
          val urlMap =
            url
              .split("\\?", 2)(1)
              .split("&")
              .map{seg =>
                val segSplit = seg.split("=", 2)
                try{
                  (segSplit(0), segSplit(1))
                }catch {
                  case e1: ArrayIndexOutOfBoundsException =>
                    ("key", "value")
                }
              }
              .toMap
          val idfa = urlMap("idfa")
          val unitId = urlMap("unit_id")
          val mofData = urlMap("mof_data")

          (Array(idfa, unitId, clientIp, timeStr).mkString("_"), (nginx, mofData))
        }
        .repartition(100)
    val nginxCnt = nginxData.count()
    println("date: %s, nginxBeforeJoinCnt: %d".format(date, nginxCnt))
    nginxData.take(10).foreach(println)
    nginxData.map{case(key, (nginx, mofData)) => "%s\t%s".format(key, nginx)}.saveAsTextFile(nginxBeforeJoinPath)

    val reqData =
      sc.textFile(requestPath)
        .filter{request =>
          try{
            val reqSplit = request.trim.split("\t")
            val extData = reqSplit(108)
            val platform = reqSplit(13)
            val unitId = reqSplit(5)
            val idfa = reqSplit(43)

            extData.contains("parent_id") && platform == "ios" && idfa != "0"
          }catch {
            case e1: ArrayIndexOutOfBoundsException =>
              false
          }
        }
        .map{request =>
          val reqSplit = request.trim.split("\t")
          val idfa = reqSplit(43)
          val unitId = reqSplit(5)
          val clientIp = reqSplit(34)
          val requestId = reqSplit(27)
          val time = java.lang.Long.valueOf(requestId.substring(0, 8), 16)

          ( Array(idfa, unitId, clientIp, time.toString).mkString("_"), requestId )
        }
        .repartition(100)
    val reqCnt = reqData.count()
    println("date: %s, reqBeforeJoinCnt: %d".format(date, reqCnt))
    reqData.map{case(key, req) => "%s\t%s".format(key, req)}.saveAsTextFile(requestBeforJoinPath)

    val nginxReqFullData =
      nginxData.fullOuterJoin(
        reqData.flatMap{case(key, req) =>
          val keySplit = key.split("_")
          val time = keySplit(3).toLong
          Array(
            (Array(keySplit(0), keySplit(1), keySplit(2), (time-3).toString).mkString("_"), (-3, req)),
            (Array(keySplit(0), keySplit(1), keySplit(2), (time-2).toString).mkString("_"), (-2, req)),
            (Array(keySplit(0), keySplit(1), keySplit(2), (time-1).toString).mkString("_"), (-1, req)),
            (Array(keySplit(0), keySplit(1), keySplit(2), time.toString).mkString("_"), (0, req)),
            (Array(keySplit(0), keySplit(1), keySplit(2), (time+1).toString).mkString("_"), (1, req)),
            (Array(keySplit(0), keySplit(1), keySplit(2), (time+2).toString).mkString("_"), (2, req)),
            (Array(keySplit(0), keySplit(1), keySplit(2), (time+3).toString).mkString("_"), (3, req))
          )
        }
      )
    nginxReqFullData.filter{case(key, (nginxOpt, reqOpt)) => nginxOpt != None && reqOpt == None}
      .map{case(key, (nginxOpt, reqOpt)) =>
          val (nginx, mofData) = nginxOpt.get
          "%s\t%s".format(key, nginx)
      }
      .saveAsTextFile(nginxNoJoinPath)
    val nginxReqJoinData =
      nginxReqFullData.filter{case(key, (nginxOpt, reqOpt)) => nginxOpt != None && reqOpt != None}
        .map{case(key, (nginxOpt, reqOpt)) =>
          val (nginx, mofData) = nginxOpt.get
          val (num, req) = reqOpt.get
          (req, (key, nginx, mofData))
        }
          .reduceByKey{case((key1, nginx1, mofData1), (key2, nginx2, mofData2)) => Array((key1, nginx2, mofData1), (key2, nginx2, mofData2)).apply(scala.util.Random.nextInt(2))}
          .map{case(req, (key, nginx, mofData)) => (key, req, nginx, mofData)}
    nginxReqJoinData.map{case(key, req, nginx, mofData) => "%s\t%s\t%s".format(key, req, nginx)}.saveAsTextFile(nginxRequestJoinPath)
    nginxReqFullData.filter{case(key, (nginxOpt, reqOpt)) => nginxOpt == None && reqOpt != None}
      .map{case(key, (nginxOpt, reqOpt)) =>
        val (num, req) = reqOpt.get
        (Array(key, req).mkString("_"), Set(num))
      }
      .reduceByKey{case(set1, set2) => set1 ++ set2}
      .filter{case(keyReq, set) =>
          set.diff(Set(-3, -2, -1, 0, 1, 2, 3)).size == 0 && Set(-3, -2, -1, 0, 1, 2, 3).diff(set).size == 0
      }
      .map{case(keyReq, set) => "%s\t%s".format(keyReq, set.mkString("_"))}
      .saveAsTextFile(requestNoJoinPath)


    val impressionData =
      sc.textFile(impressionPath)
        .filter{impression =>
          try{
            val impreSplit = impression.trim.split("\t")
            val extData2 = impreSplit(122)
            val platform = impreSplit(13)
            val unitId = impreSplit(5)

            extData2.contains("parent_id") && platform == "ios"
          }catch {
            case e1: ArrayIndexOutOfBoundsException =>
              false
          }
        }
        .map{impression =>

          val impressionSplit = impression.trim.split("\t")
          val requestId = impressionSplit(27)
          val unitId = impressionSplit(5)
          val camId = impressionSplit(7)

          ( requestId, Array(unitId, camId).mkString("_") )
        }
    impressionData.map{case(req, unitCam) => "%s\t%s".format(req, unitCam)}.saveAsTextFile(impreBeforeJoinPath)

    val secondFullJoinData =
      nginxReqJoinData.map{case(key, req, nginx, mofData) => (req, (key, mofData))}
        .fullOuterJoin(impressionData)

    secondFullJoinData.filter{case(req, (nginxReqOpt, impreOpt)) => nginxReqOpt != None && impreOpt == None}
      .map{case(req, (nginxReqOpt, impreOpt)) =>
          val (key, mofData) = nginxReqOpt.get
          "%s\t%s\t%s".format(req, key, mofData)
      }
      .saveAsTextFile(secondReqNoJoinPath)
    secondFullJoinData.filter{case(req, (nginxReqOpt, impreOpt)) => nginxReqOpt != None && impreOpt != None}
      .map{case(req, (nginxReqOpt, impreOpt)) =>
        val (key, mofData) = nginxReqOpt.get
        val unitCam = impreOpt.get
        "%s\t%s\t%s\t%s".format(req, unitCam, key, mofData)
      }
      .saveAsTextFile(secondJoinPath)
    secondFullJoinData.filter{case(req, (nginxReqOpt, impreOpt)) => nginxReqOpt == None && impreOpt != None}
      .map{case(req, (nginxReqOpt, impreOpt)) =>
          val unitCam = impreOpt.get
          "%s\t%s".format(req, unitCam)
      }
      .saveAsTextFile(impreNoJoinPath)

    ()
  }

}
