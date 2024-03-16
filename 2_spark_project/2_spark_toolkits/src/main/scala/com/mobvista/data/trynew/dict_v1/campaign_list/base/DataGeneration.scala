package com.mobvista.data.trynew.dict_v1.campaign_list.base

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.TimeZone

import com.alibaba.druid.pool.DruidDataSource
import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.dbutils.handlers.{ArrayListHandler, MapHandler}
import org.apache.spark.SparkContext

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

class DataGeneration {

  def generate(sc: SparkContext, startId: Long, endId: Long, sqlStepLength: Int = 100000): Unit = {

    val sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss")
    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))

    val outPath = "s3://mob-emr-test/baihai/m_sys_model/trynew/dict/campaign_list/base/id%d_id%d".format(startId, endId)
    println("outPath: %s".format(outPath))

    val host = "adn-mysql-externalforqa.mobvista.com"
    val port = "3306"
    val db = "mob_adn"
    val url = "jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=utf-8&useSSL=false".format(host, port, db)
    val username = "mob_adn_ro"
    val password = "blueriver123"
    val driver = "com.mysql.jdbc.Driver"
    println("host: %s".format(host))
    println("port: %s".format(port))
    println("db: %s".format(db))
    println("url: %s".format(url))
    println("username: %s".format(username))
    println("password: %s".format(password))
    println("driver: %s".format(driver))

    val druidDataSource = new DruidDataSource()
    druidDataSource.setUrl(url)
    druidDataSource.setUsername(username)
    druidDataSource.setPassword(password)
    druidDataSource.setDriverClassName(driver)
    druidDataSource.setInitialSize(5)
    druidDataSource.setMinIdle(1)
    druidDataSource.setMaxActive(10)
    druidDataSource.setMaxWait(5000)
    druidDataSource.setRemoveAbandoned(false)
//    druidDataSource.setRemoveAbandonedTimeout(1800)
    druidDataSource.setTestOnBorrow(true)
    druidDataSource.setValidationQuery("select 1")
    val conn = druidDataSource.getConnection
    val queryRunner = new QueryRunner()
    val arrayListHandler = new ArrayListHandler()

    val queryCnt = (endId - startId + 1) / sqlStepLength + 1
    println("queryCnt: %d".format(queryCnt))
    val timeLogArr = new ArrayBuffer[Long]()
    val camPkgTime =
      sc.union(
        (0 until queryCnt.toInt).map{i =>

          if(i % 50 == 0){
            val curTime = System.currentTimeMillis()
            timeLogArr.append(curTime)
            println("current time is : %s".format(sdf.format(new Date(System.currentTimeMillis()))))
            if(timeLogArr.length > 1){
              val readRate = 50 * sqlStepLength.toDouble * 1000 / (timeLogArr.last - timeLogArr(timeLogArr.length-2))
              println("readRate: %f records/second".format(readRate))
            }
          }

          val stepMinId = startId + i * sqlStepLength
          val stepMaxId = startId + (i + 1) * sqlStepLength

          val stepSql = "select id as campaign_id, advertiser_id, trace_app_id as package_name, timestamp as create_time, status from campaign_list where id >= %d and id < %d".format(stepMinId, stepMaxId)
          val stepRes = queryRunner.query(conn, stepSql, arrayListHandler)
          println("stepSql: %s\nstepCnt: %d".format(stepSql, stepRes.length))
          val stepData = stepRes.map{arr =>
            Array(arr(0), arr(1), arr(2), arr(3), arr(4)).mkString("\t")
          }
          sc.parallelize(stepData)
        }
      )
    val camPkgTimeCnt = camPkgTime.count()
    println("camPkgTimeCnt: %d".format(camPkgTimeCnt))

    conn.close()

    camPkgTime
      .repartition((camPkgTimeCnt/2000000).toInt + 1)
      .saveAsTextFile(outPath)

    ()
  }

}
