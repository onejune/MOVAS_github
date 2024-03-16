package com.mobvista.data.mvp_mining.exp1.training_data

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.TimeZone

import net.sourceforge.argparse4j.ArgumentParsers
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RunTrainDataGeneration {

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis().toDouble

    val parser = ArgumentParsers.newFor("RUN").build()
    parser.addArgument("--start_date").help("start date: yyyyMMdd")
    parser.addArgument("--end_date").help("end date: yyyyMMdd")
    parser.addArgument("--pkg_name_list").help("comma seperated pkg name list")

    try {
      val arg = parser.parseArgs(args)
      val start_date = arg.getString("start_date")
      val end_date = arg.getString("end_date")
      val pkg_name_list = arg.getString("pkg_name_list")
      println("start_date is: %s".format(start_date))
      println("end_date is: %s".format(end_date))
      println("pkg_name_list is: %s".format(pkg_name_list))
      val sdf = new SimpleDateFormat("yyyyMMdd")
      sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
      val day = ((sdf.parse(end_date).getTime - sdf.parse(start_date).getTime)/86400/1000 + 1).toInt
      println("total days: %d".format(day))

      val conf = new SparkConf()
        .set("spark.hadoop.validateOutputSpecs", "false")
        .set("spark.network.timeout", "600s")
        .set("spark.executor.heartbeatInterval", "30s")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryoserializer.buffer.max", "256m")
        .set("spark.kryoserializer.buffer", "64m")
        .registerKryoClasses(Array(classOf[TrainDataGeneration]))
      val spark = SparkSession
        .builder()
        .appName("bh: hercules mvp_mining train data generation")
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
      val sc = spark.sparkContext

      val trainDataGeneration = new TrainDataGeneration()
      (0 until day).foreach{i =>
        trainDataGeneration.generate(sc, sdf.format(new Date(sdf.parse(start_date).getTime + i * 86400 * 1000)), pkg_name_list)
      }

      spark.close()

    }catch {
      case e: Exception =>
        println(e.getMessage)
    }

    val endTime = System.currentTimeMillis()
    println("Finished, time consumed: %.2f minutes".format((endTime - startTime)/(1000*60)))

    ()
  }

}
