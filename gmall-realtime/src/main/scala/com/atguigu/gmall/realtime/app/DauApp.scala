package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.StartupLog
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import com.atguigu.gmall0225.common.util.GmallConstant
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * Author: Wmd
  * Date: 2019/7/23 10:37
  */
object DauApp {

  def main(args: Array[String]): Unit = {
    //1.从kafka消费数据(启动日志)
    //ssc配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc = new StreamingContext(conf, Seconds(5))
    //从kafka消费启动日志信息
    val sourceStream: InputDStream[(String, String)] =
      MyKafkaUtil.getKafkaStream(ssc, GmallConstant.TOPIC_STARTUP)

    // 2 使用redis进行清洗
    //2.1 对数据进行封装
    val starupLogDSteam: DStream[StartupLog] = sourceStream.map {
      case (_, json) => JSON.parseObject(json, classOf[StartupLog])
    }
    //2.2写入之前先进行过滤
    starupLogDSteam.transform(rdd =>{
      val client: Jedis = RedisUtil.getJedisClient
      val uidSet: util.Set[String] = client.smembers(GmallConstant.REDIS_DAU_KEY + ":"+ new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
      val uidSetBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(uidSet)
      client.close()
      val distinctedRDD: RDD[StartupLog] = rdd.distinct
      distinctedRDD.filter(startupLog =>{
        val uids: util.Set[String] = uidSetBC.value
        // 返回没有写过的
        !uids.contains(startupLog.uid)
      })
    })
    // 2.3 保存到 redis
    starupLogDSteam.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        val client: Jedis = RedisUtil.getJedisClient
        it.foreach(startupLog => {
          // 存入到 Redis value 类型 set, 存储 uid

          client.sadd(GmallConstant.REDIS_DAU_KEY + ":"+startupLog.logDate, startupLog.uid)
        })
        client.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }


}
