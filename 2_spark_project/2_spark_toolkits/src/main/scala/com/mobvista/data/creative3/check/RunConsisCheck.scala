package com.mobvista.data.creative3.check

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RunConsisCheck {

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis().toDouble

    val conf = new SparkConf()
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.network.timeout", "600s")
      .set("spark.executor.heartbeatInterval", "30s")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "256m")
      .set("spark.kryoserializer.buffer", "64m")
      .registerKryoClasses(Array(classOf[ConsistentCheck]))
    val spark = SparkSession
      .builder()
      .appName("bh_consistence_check")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext

    val consistentCheck = new ConsistentCheck()
    consistentCheck.extractData(sc)

    spark.close()

    val endTime = System.currentTimeMillis()
    println("Finished, time consumed: %.2f minutes".format((endTime - startTime)/(1000*60)))

    ()
  }

}
