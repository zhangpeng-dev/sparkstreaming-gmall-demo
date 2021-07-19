package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * @ClassName UserInfoApp
 * @Description TODO
 * @Date 2021/7/7 18:38
 * */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val userInfoKafkaDStream = MyKafkaUtil
      .getKafkaStream(GmallConstants.KAFKA_TOPIC_USER, ssc)

    val jsonDStream = userInfoKafkaDStream.map(_.value())

    jsonDStream.print()
    //将数据写入redis
    jsonDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        val jedis = new Jedis("hadoop102",6379)
        partition.foreach(str=>{
          //写入redis
          //转为样例类获取redis的key
          val userInfo = JSON.parseObject(str, classOf[UserInfo])
          val userInfoRedisKey = "userInfo:"+userInfo.id
          jedis.set(userInfoRedisKey,str)
        })
        jedis.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
