package com.mobvista.data.trynew.dict_v1.campaign_list.increment

import java.net.URI
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.TimeZone

import com.alibaba.druid.pool.DruidDataSource
import org.apache.commons.dbutils.QueryRunner
import org.apache.commons.dbutils.handlers.{ArrayListHandler, MapHandler}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

import scala.collection.JavaConversions._

class DataGeneration {

  def generate(sc: SparkContext, dateHour: String, sqlStepLength: Int): Unit = {

    val date = dateHour.substring(0, 8)
    val outPath = "s3://mob-emr-test/baihai/m_sys_model/trynew/dict/campaign_list/increment/%s/%s".format(date, dateHour)
    println("outPath: %s".format(outPath))

    val fs = FileSystem.get(new URI("s3://mob-emr-test"), sc.hadoopConfiguration)
    val sdfHour = new SimpleDateFormat("yyyyMMddHH")
    sdfHour.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val timeStamp = sdfHour.parse(dateHour).getTime
    val tryCnt = 60
    val lastHourPath =
      (1 to tryCnt)
        .map{ i =>
          val lastDateHour = sdfHour.format(new Date(timeStamp - i * 3600 * 1000L))
          val lastDate = lastDateHour.substring(0, 8)
          "s3://mob-emr-test/baihai/m_sys_model/trynew/dict/campaign_list/increment/%s/%s".format(lastDate, lastDateHour)
        }
        .filter{path =>fs.exists(new Path(path))}(0)
    val lastAllData = sc.textFile(lastHourPath)
    val minId =
      lastAllData
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

    // 从上次的最大id查到末尾
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

    // campaign_id, advertiser_id, package_name, create_time, status
    // 在已有记录中查询一天内的记录，进行更新，保证跟踪单子的状态变化
    val lastDayData = lastAllData
      .map{line =>
        val lineSplit = line.trim.split("\t")
        val camId = lineSplit(0).toLong
        val createTime = lineSplit(3).toLong
        (camId, createTime)
      }
      .filter{case(camId, createTime) =>
        createTime * 1000 >= timeStamp - 24 * 3600 * 1000
      }
    val lastDayCnt = lastDayData.count()
    val updateData =
      if(lastDayCnt > 0){

        val startId =
          lastDayData
            .reduce{case((camId1, createTime1), (camId2, createTime2)) =>
              if(createTime1 < createTime2)
                (camId1, createTime1)
              else
                (camId2, createTime2)
            }
            ._1

        val updateQueryCnt = (minId - startId) / sqlStepLength + 1
        println("updateQueryCnt: %d".format(updateQueryCnt))

        sc.union(
          (0 until updateQueryCnt.toInt).map{i =>

            val updateStepMinId = startId + i * sqlStepLength
            val updateStepMaxId = startId + (i + 1) * sqlStepLength

            val updateStepSql = "select id as campaign_id, advertiser_id, trace_app_id as package_name, timestamp as create_time, status from campaign_list where id >= %d and id < %d".format(updateStepMinId, updateStepMaxId)
            val updateStepRes = queryRunner.query(conn, updateStepSql, arrayListHandler)
            println("updateStepSql: %s\nupdateStepCnt: %d".format(updateStepSql, updateStepRes.length))
            val updateStepData = updateStepRes.map{arr =>
              Array(arr(0), arr(1), arr(2), arr(3), arr(4)).mkString("\t")
            }
            sc.parallelize(updateStepData, updateStepRes.length / 100000 + 1)
          }
        )
      }else{
        sc.emptyRDD[String]
      }
    val updateCnt = updateData.count()
    println("updateCnt: %d".format(updateCnt))

    conn.close()

    // campaign_id, advertiser_id, package_name, create_time, status
    lastAllData
      .union(camPkgTime)
      .map{line =>
        val lineSplit = line.trim.split("\t")
        (lineSplit(0), line)
      }
      .subtractByKey(
        updateData.map{line =>
          val lineSplit = line.trim.split("\t")
          (lineSplit(0), line)
        }
      )
      .map{case(camId, line) => line}
      .union(updateData)
      .distinct((maxId/6000000).toInt + 1)
      .saveAsTextFile(outPath)

    ()
  }

}
