package com.mobvista.data.creative_model.exp1.hour

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.TimeZone

import com.mobvista.data.creative3.update_v2.hour.DataGenerationUniq
import net.sourceforge.argparse4j.ArgumentParsers
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Run {

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis().toDouble

    val parser = ArgumentParsers.newFor("RUN").build()
    parser.addArgument("--start_date_hour").help("start date: yyyyMMddHH")
    parser.addArgument("--end_date_hour").help("end date: yyyyMMddHH")

    try {
      val arg = parser.parseArgs(args)
      val start_date_hour = arg.getString("start_date_hour")
      val end_date_hour = arg.getString("end_date_hour")
      println("start_date_hour is: %s".format(start_date_hour))
      println("end_date_hour is: %s".format(end_date_hour))
      val sdf = new SimpleDateFormat("yyyyMMddHH")
      sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
      val hours = ((sdf.parse(end_date_hour).getTime - sdf.parse(start_date_hour).getTime)/3600/1000 + 1).toInt
      println("total hours: %d".format(hours))

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
        .appName("hercules creative_model hourly train data")
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
      val sc = spark.sparkContext

      val dataGeneration = new DataGeneration()
      (0 until hours).foreach{i =>
        dataGeneration.generate(sc, sdf.format(new Date(sdf.parse(start_date_hour).getTime + i * 3600 * 1000)))
      }

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
