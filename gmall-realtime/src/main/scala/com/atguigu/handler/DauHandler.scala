package com.atguigu.handler

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat

/**
 * @ClassName DauHandler
 * @Description TODO
 * @Date 2021/7/2 16:22
 * */
object DauHandler {

  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]) = {
    filterByRedisDStream.map(log => ((log.mid, log.logDate), log))
      .groupByKey()
      .mapValues(iter => iter.toList.sortWith(_.ts < _.ts).take(1))
      .flatMap(_._2)
  }


  def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext) = {
    //方案一
    //    startUpLogDStream.filter(log=>{
    //      val jedis = new Jedis("hadoop102", 6379)
    //      val key = "DAU" + log.logDate
    //      val bool = !jedis.smembers(key).contains(log.mid)
    //      jedis.close()
    //      bool
    //    })
    //方案二
    startUpLogDStream.mapPartitions(partition => {
      val jedis = new Jedis("hadoop102", 6379)
      val value = partition.filter(log => {
        val key = "DAU" + log.logDate
        val bool = jedis.smembers(key).contains(log.mid)
        !bool
      })
      jedis.close()
      value
    })
    //方案三
    //    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    //    startUpLogDStream.transform(rdd=>{
    //      val jedis = new Jedis("hadoop102", 6379)
    //      val key = "DAU"+sdf.format(System.currentTimeMillis())
    //      val mids = jedis.smembers(key)
    //      val bdmids = sc.broadcast(mids)
    //      val value = rdd.filter(log => {
    //        !bdmids.value.contains(log.logDate)
    //      })
    //      jedis.close()
    //      value
    //    })
  }


  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val jedis = new Jedis("hadoop102", 6379)
        partition.foreach(log => {
          val key = "DAU" + log.logDate
          jedis.sadd(key, log.mid)
        })
        jedis.close()
      })
    })

  }
}
