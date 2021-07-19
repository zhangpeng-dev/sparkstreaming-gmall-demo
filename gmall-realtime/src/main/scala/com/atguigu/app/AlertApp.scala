package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util
import scala.util.control.Breaks._

/**
 * @ClassName AlertApp
 * @Description TODO
 * @Date 2021/7/7 10:48
 * */
object AlertApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val kafkaDStream = MyKafkaUtil
      .getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    val midToEventLogDStream = kafkaDStream.mapPartitions(partition => {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
      partition.map(record => {
        val eventLog = JSON.parseObject(record.value(), classOf[EventLog])
        val times = sdf.format(eventLog.ts)
        eventLog.logDate = times.split(" ")(0)
        eventLog.logHour = times.split(" ")(1)
        (eventLog.mid, eventLog)
      })
    })

    val midToEventLogWindowDStream = midToEventLogDStream.window(Minutes(5))

    val logWindowGroupDStream = midToEventLogWindowDStream.groupByKey()

    val boolToCouponAlertInfoDStream = logWindowGroupDStream
      .mapPartitions(partition => {
        partition.map { case (mid, iter) =>
          val uids = new util.HashSet[String]()
          val itemIds = new util.HashSet[String]()
          val events = new util.ArrayList[String]()
          var bool = true
          breakable(
            iter.foreach(log => {
              events.add(log.evid)
              if ("clickItem".equals(log.evid)) {
                bool = false
                break()
              } else if ("coupon".equals(log.evid)) {
                uids.add(log.uid)
                itemIds.add(log.itemid)
              }
            })
          )
          (uids.size() >= 3 && bool, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
        }
      })

    val couponAlertInfoDStream = boolToCouponAlertInfoDStream.filter(_._1).map(_._2)

    couponAlertInfoDStream.cache()
    couponAlertInfoDStream.print()

    couponAlertInfoDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val list = partition.toList.map(alert => {
          (alert.mid + alert.ts / 1000 / 60, alert)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_ALERT_INDEXNAME, list)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
