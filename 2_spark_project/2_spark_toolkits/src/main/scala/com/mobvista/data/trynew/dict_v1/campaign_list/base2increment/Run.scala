package com.mobvista.data.trynew.dict_v1.campaign_list.base2increment

import java.sql.Date

import net.sourceforge.argparse4j.ArgumentParsers
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Run {

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis().toDouble

    val parser = ArgumentParsers.newFor("RUN").build()
    parser.addArgument("--date_hour").help("date hour: yyyyMMddHH")
    parser.addArgument("--sql_step_length").help("sql_step_length")

    try {
      val arg = parser.parseArgs(args)
      val date_hour = arg.getString("date_hour")
      val sql_step_length = arg.getString("sql_step_length").toInt
      println("date_hour is: %s".format(date_hour))
      println("sql_step_length is: %d".format(sql_step_length))

      val conf = new SparkConf()
        .set("spark.hadoop.validateOutputSpecs", "false")
        .set("spark.network.timeout", "600s")
        .set("spark.executor.heartbeatInterval", "30s")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer.max", "256m")
        .set("spark.kryoserializer.buffer", "64m")
        .registerKryoClasses(Array(classOf[DataGeneration]))
      val spark = SparkSession
        .builder()
        .appName("hercules trynew campaign_list base2incre")
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
      val sc = spark.sparkContext

      val dataGeneration = new DataGeneration()
      dataGeneration.generate(sc, date_hour, sql_step_length)

      spark.close()

    }catch {
      case e: Exception =>
        e.printStackTrace()
    }

    val endTime = System.currentTimeMillis()
    println("Finished, time consumed: %.2f minutes".format((endTime - startTime)/(1000*60)))

    ()
  }

}
