package com.mobvista.data.trynew.dict_v1.campaign_list.base2increment

import com.alibaba.druid.pool.DruidDataSource
import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.dbutils.handlers.{ArrayListHandler, MapHandler}
import org.apache.spark.SparkContext

import scala.collection.JavaConversions._

class DataGeneration {

  def generate(sc: SparkContext, dateHour: String, sqlStepLength: Int): Unit = {

    val date = dateHour.substring(0, 8)
    val basePath = "s3://mob-emr-test/baihai/m_sys_model/trynew/dict/campaign_list/base/"
    val outPath = "s3://mob-emr-test/baihai/m_sys_model/trynew/dict/campaign_list/increment/%s/%s".format(date, dateHour)
    println("basePath: %s".format(basePath))
    println("outPath: %s".format(outPath))

    val minId =
      sc.textFile(basePath)
        .map{line =>line.trim.split("\t")(0).toLong}
        .reduce{case(camId1, camId2) => math.max(camId1, camId2)} + 1

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
    val mapHandler = new MapHandler()
    val arrayListHandler = new ArrayListHandler()

    val sql1 = "select max(id) as max_id from campaign_list"
    val res1 = queryRunner.query(conn, sql1, mapHandler)
    val maxId = res1.get("max_id").toString.toLong
    println("minId: %d, maxId: %d".format(minId, maxId))

    val queryCnt = (maxId - minId + 1) / sqlStepLength + 1
    println("queryCnt: %d".format(queryCnt))
    val camPkgTime =
      sc.union(
        (0 until queryCnt.toInt).map{i =>

          val stepMinId = minId + i * sqlStepLength
          val stepMaxId = minId + (i + 1) * sqlStepLength

          val stepSql = "select id as campaign_id, advertiser_id, trace_app_id as package_name, timestamp as create_time, status from campaign_list where id >= %d and id < %d".format(stepMinId, stepMaxId)
          val stepRes = queryRunner.query(conn, stepSql, arrayListHandler)
          println("stepSql: %s\nstepCnt: %d".format(stepSql, stepRes.length))
          val stepData = stepRes.map{arr =>
            Array(arr(0), arr(1), arr(2), arr(3), arr(4)).mkString("\t")
          }
          sc.parallelize(stepData, stepRes.length / 100000 + 1)
        }
      )
    val camPkgTimeCnt = camPkgTime.count()
    println("camPkgTimeCnt: %d".format(camPkgTimeCnt))

    conn.close()

    sc.textFile(basePath)
      .union(camPkgTime)
      .distinct((maxId / 2000000).toInt + 1)
      .saveAsTextFile(outPath)

    ()
  }

}
