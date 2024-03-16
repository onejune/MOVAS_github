package com.mobvista.data.creative3.base_v1.test

import org.apache.spark.SparkContext

class DataSource(@transient sc: SparkContext,
                 columnNamePath: String,
                 combineSchemaPath: String,
                 removedFeaturePath: String) extends java.io.Serializable{

  val name2idx = parseColumnName()
  val names = name2idx.toSeq.sortBy{case(name, idx) => idx}.map{case(name, idx) => name}
  val labelIdx = name2idx("cvr_label")
  val extra5Idx = name2idx("extra5")
  val camIdx = name2idx("campaign_id")
  val adtypeIdx = name2idx("ad_type")
  val unitIdx = name2idx("unit_id")
  val appidIdx = name2idx("app_id")
  val sdkIdx = name2idx("sdk_version")
  val ccIdx = name2idx("country_code")
  val advIdx = name2idx("advertiser_id")


  val combineSchema = parseCombineSchema()

  val removedFeature = parseRemovedFearure()

  def parseColumnName(): collection.Map[String, Int] = {

    sc.textFile(columnNamePath).map{line =>
      val lineSplit = line.trim.split(" ")
      (lineSplit(1), lineSplit(0).toInt)
    }.collectAsMap()

  }

  def parseCombineSchema(): Array[Array[Int]] = {

    sc.textFile(combineSchemaPath).filter{line =>
      !line.startsWith("#")
    }.map{line =>
      line.trim.split("#").map{fea => name2idx(fea)}
    }.collect()

  }

  def parseRemovedFearure(): collection.Map[String, Array[String]] = {

    sc.textFile(removedFeaturePath).map{line =>
      val lineSplit = line.trim.split(" ")
      val key = lineSplit(0)
      val values = lineSplit(1).split(",")
      (key, values)
    }.collectAsMap()

  }

  def isWrongSample(lineSplit: Array[String]): Boolean ={

    lineSplit(adtypeIdx) == "0" ||
    lineSplit(adtypeIdx) == "appwall" ||
    removedFeature.getOrElse("unit_id", Array[String]()).contains(lineSplit(unitIdx)) ||
    removedFeature.getOrElse("app_id", Array[String]()).contains(lineSplit(appidIdx)) ||
    removedFeature.getOrElse("sdk_version", Array[String]()).contains(lineSplit(sdkIdx)) ||
    removedFeature.getOrElse("country", Array[String]()).contains(lineSplit(ccIdx)) ||
    removedFeature.getOrElse("advertiser_id", Array[String]()).contains(lineSplit(advIdx))

  }

  def generate(fieldArrArr: Array[Array[String]]): Array[Array[String]] = {

    if(fieldArrArr.length == 1){
      fieldArrArr(0).map{v => Array(v)}
    }else{
      val tailRes = generate(fieldArrArr.tail)
      fieldArrArr.head.map{headv =>
        tailRes.map{tailArr =>
          headv +: tailArr
        }
      }.reduceLeft{(first: Array[Array[String]], second: Array[Array[String]]) =>
        first ++ second
      }
    }
  }

  def extractFeature(line: String): (String, Array[String]) = {

    val lineSplit = line.trim.split("\002")

    val feaArray =
      combineSchema.map{schema =>
        val nameArr = schema.map{idx => names(idx)}
        val fieldArrArr = schema.map{idx =>
          lineSplit(idx).split("\001").filter{value => value != "none"}
        }
        val minLen = fieldArrArr.map{fieldArr => fieldArr.length}.min
        if(minLen <= 0){
          Array[String]()
        } else{
          generate(fieldArrArr).map{feaArr =>
            nameArr.zip(feaArr).map{case(name, value) => name + "=" + value}.mkString("\001") // one feature
          }
        }
      }.reduceLeft{(first: Array[String], second: Array[String]) => first ++ second}

    val label = lineSplit(labelIdx)

    (label, feaArray)
  }

}
