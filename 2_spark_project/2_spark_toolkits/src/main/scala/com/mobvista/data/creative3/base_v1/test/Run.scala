package com.mobvista.data.creative3.base_v1.test

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.TimeZone

import com.mobvista.data.creative3.base_v1.hour.DataGeneration
import net.sourceforge.argparse4j.ArgumentParsers
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Run {

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis().toDouble

    val parser = ArgumentParsers.newFor("RUN").build()
    parser.addArgument("--start_date_hour").help("start date: yyyyMMddHH")
    parser.addArgument("--end_date_hour").help("end date: yyyyMMddHH")
    parser.addArgument("--column_name").help("column name file path in s3")
    parser.addArgument("--combine_schema").help("combine schema path in s3")
    parser.addArgument("--removed_feature").help("removed feature path in s3")

    try {
      val arg = parser.parseArgs(args)
      val start_date_hour = arg.getString("start_date_hour")
      val end_date_hour = arg.getString("end_date_hour")
      val columnNamePath = arg.getString("column_name")
      val combineSchemaPath = arg.getString("combine_schema")
      val removedFeaturePath = arg.getString("removed_feature")
      println("start_date_hour is: %s".format(start_date_hour))
      println("end_date_hour is: %s".format(end_date_hour))
      println("columnNamePath: %s".format(columnNamePath))
      println("combineSchemaPath: %s".format(combineSchemaPath))
      println("removedFeaturePath: %s".format(removedFeaturePath))
      val sdf = new SimpleDateFormat("yyyyMMddHH")
      sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
      val hous = ((sdf.parse(end_date_hour).getTime - sdf.parse(start_date_hour).getTime)/3600/1000 + 1).toInt
      println("total hours: %d".format(hous))


      val conf = new SparkConf()
        .set("spark.hadoop.validateOutputSpecs", "false")
        .set("spark.network.timeout", "600s")
        .set("spark.executor.heartbeatInterval", "30s")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(Array(classOf[DataGeneration]))
      val spark = SparkSession
        .builder()
        .appName("hercules base_v1 hour data")
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
      val sc = spark.sparkContext

      val dataGeneration = new DataGeneration()
      (0 until hous).foreach{i =>
        dataGeneration.generate(sc,
          columnNamePath,
          combineSchemaPath,
          removedFeaturePath,
          sdf.format(new Date(sdf.parse(start_date_hour).getTime + i * 3600 * 1000))
        )
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
