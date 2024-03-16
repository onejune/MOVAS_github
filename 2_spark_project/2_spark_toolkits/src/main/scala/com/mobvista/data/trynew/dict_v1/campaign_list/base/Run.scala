package com.mobvista.data.trynew.dict_v1.campaign_list.base

import net.sourceforge.argparse4j.ArgumentParsers
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Run {

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis().toDouble

    val parser = ArgumentParsers.newFor("RUN").build()
    parser.addArgument("--start_id").help("start id")
    parser.addArgument("--end_id").help("end id")
    parser.addArgument("--sql_step_length").help("sql_step_length")

    try {
      val arg = parser.parseArgs(args)
      val start_id = arg.getString("start_id").toLong
      val end_id = arg.getString("end_id").toLong
      val sql_step_length = arg.getString("sql_step_length").toInt
      println("start_id is: %d".format(start_id))
      println("end_id is: %d".format(end_id))
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
        .appName("hercules trynew campaign_list base")
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
      val sc = spark.sparkContext

      val dataGeneration = new DataGeneration()
      dataGeneration.generate(sc, start_id, end_id, sql_step_length)

      spark.close()

    }catch {
      case e: Exception => e.printStackTrace()
    }

    val endTime = System.currentTimeMillis()
    println("Finished, time consumed: %.2f minutes".format((endTime - startTime)/(1000*60)))

    ()
  }

}
