package com.mobvista.data.creative3.exp1.feature_postprocess

import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

class FeaturePostProcess {

  def postProcess(sc: SparkContext, dateHour: String): Unit = {

    val sdf = new SimpleDateFormat("yyyyMMddHH")
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val timeThres = sdf.parse(dateHour).getTime / 1000 - 86400
    val date = dateHour.substring(0, 8)
    val inputPath = "s3://mob-emr-test/dataplatform/DataWareHouse/offline/generate_train_data/m_dataflow_creative3_v1/%s/%s".format(date, dateHour)
    val outputPath = "s3://mob-emr-test/baihai/m_sys_model/creative3_v2/post_process/%s/%s".format(date, dateHour)

    println("inputPath: %s".format(inputPath))
    println("outputPath: %s".format(outputPath))

    val maxLimit9Index = new ArrayBuffer[Int]()
    val maxLimit7Index = new ArrayBuffer[Int]()
    val maxLimit2Index = new ArrayBuffer[Int]()

    (0 until 9).foreach{i =>
      maxLimit9Index.append(39 + i * 9)
      maxLimit9Index.append(39 + i * 9 + 3)
      maxLimit9Index.append(39 + i * 9 + 6)

      maxLimit7Index.append(39 + i * 9 + 1)
      maxLimit7Index.append(39 + i * 9 + 4)
      maxLimit7Index.append(39 + i * 9 + 7)

      maxLimit2Index.append(39 + i * 9 + 2)
      maxLimit2Index.append(39 + i * 9 + 5)
      maxLimit2Index.append(39 + i * 9 + 8)
    }

    (0 until 4 * 3 * 3).foreach{i =>
      maxLimit9Index.append(120 + 4 * i + 1)
      maxLimit7Index.append(120 + 4 * i + 2)
      maxLimit2Index.append(120 + 4 * i + 3)
    }

    (0 until 3 * 3).foreach{i =>
      maxLimit9Index.append(264 + i * 3 + 0)
      maxLimit7Index.append(264 + i * 3 + 1)
      maxLimit2Index.append(264 + i * 3 + 2)
    }

    val toLowerIndexBC = sc.broadcast(Array(5, 38))
    val maxLimit9IndexBC = sc.broadcast(maxLimit9Index)
    val maxLimit7IndexBC = sc.broadcast(maxLimit7Index)
    val maxLimit2IndexBC = sc.broadcast(maxLimit2Index)

    sc.textFile(inputPath)
      .mapPartitions{part =>

        val toLowerIndex = toLowerIndexBC.value
        val maxLimit9 = maxLimit9IndexBC.value
        val maxLimit7 = maxLimit7IndexBC.value
        val maxLimit2 = maxLimit2IndexBC.value

        part
          .filter{line =>
            val lineSplit = line.trim.split("\002")

            val timeFlag = try{java.lang.Long.valueOf(lineSplit(2).substring(0, 8), 16) >= timeThres}catch {case e1: NumberFormatException => false}

            val maxLimit9Flag = maxLimit9.map{idx =>if(lineSplit(idx) == "none") true else{try{lineSplit(idx).toInt;true}catch {case e1: NumberFormatException => false}}}.reduce{ (bool1: Boolean, bool2: Boolean) => bool1 && bool2}
            val maxLimit7Flag = maxLimit7.map{idx =>if(lineSplit(idx) == "none") true else{try{lineSplit(idx).toInt;true}catch {case e1: NumberFormatException => false}}}.reduce{ (bool1: Boolean, bool2: Boolean) =>bool1 && bool2}
            val maxLimit2Flag = maxLimit2.map{idx =>if(lineSplit(idx) == "none") true else{try{lineSplit(idx).toInt;true}catch {case e1: NumberFormatException => false}}}.reduce{ (bool1: Boolean, bool2: Boolean) => bool1 && bool2}

            val timeDiffFlag = if(Set("none", "null").contains(lineSplit(323))) true else{try{lineSplit(323).toLong;true}catch {case e1: NumberFormatException => false}}

            val pkgSizeFlag = if(lineSplit(23) == "none") true else{try{lineSplit(23).toLong;true}catch {case e1: NumberFormatException => false}}

            timeFlag &&
            maxLimit9Flag &&
            maxLimit7Flag &&
            maxLimit2Flag &&
            timeDiffFlag &&
            pkgSizeFlag
          }
          .map{line =>
            val lineSplit = line.trim.split("\002")

            toLowerIndex.foreach{idx =>lineSplit(idx) = lineSplit(idx).toLowerCase()}

            maxLimit9.foreach{idx =>if(lineSplit(idx) != "none") lineSplit(idx) = math.min(lineSplit(idx).toInt, 9).toString}
            maxLimit7.foreach{idx =>if(lineSplit(idx) != "none") lineSplit(idx) = math.min(lineSplit(idx).toInt, 7).toString}
            maxLimit2.foreach{idx =>if(lineSplit(idx) != "none") lineSplit(idx) = math.min(lineSplit(idx).toInt, 2).toString}

            if(lineSplit(323) != "none"){
              if(lineSplit(323) == "null"){
                lineSplit(323) = "-1"
              }else{
                val timeDiff = lineSplit(323).toLong
                lineSplit(323) =
                  (
                    if(timeDiff < 0)
                      -1
                    else if(timeDiff < 120)
                      1
                    else if(timeDiff < 180)
                      2
                    else if(timeDiff < 240)
                      3
                    else if(timeDiff < 300)
                      4
                    else if(timeDiff < 420)
                      5
                    else if(timeDiff < 660)
                      6
                    else if(timeDiff < 3600)
                      7
                    else if (timeDiff < 18000)
                      8
                    else
                      9
                    ).toString
              }
            }

            if(lineSplit(23) != "none"){
              val pkgSize = lineSplit(23).toLong
              lineSplit(23) =
                (
                  if(pkgSize <= 0)
                    -1
                  else
                    math.min(pkgSize / 50 + 1, 11)
                  ).toString
            }

            lineSplit.mkString("\002")
          }
      }
      .repartition(100)
      .saveAsTextFile(outputPath)

    ()
  }

}
