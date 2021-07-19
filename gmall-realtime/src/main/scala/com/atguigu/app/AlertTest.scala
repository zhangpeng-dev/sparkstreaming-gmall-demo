package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.EventLog
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util
import scala.util.control.Breaks.{break, breakable}

/**
 * @ClassName AlertTest
 * @Description TODO
 * @Date 2021/7/9 20:29
 * */
object AlertTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("AlertTest").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaDStream = MyKafkaUtil
      .getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    val midToEventLogDStream = kafkaDStream.mapPartitions(partition => {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
      partition.map(record => {
        val eventLog = JSON.parseObject(record.value(), classOf[EventLog])
        eventLog.logDate = sdf.format(eventLog.ts).split("")(0)
        eventLog.logHour = sdf.format(eventLog.ts).split("")(1)
        (eventLog.mid, eventLog)
      })
    })

    val windowDStream = midToEventLogDStream.window(Minutes(5))

    val groupDStream = windowDStream.groupByKey()

    groupDStream.mapPartitions(partition => {
      partition.map { case (mid, iter) =>
        val uids = new util.HashSet[String]()
        val itemIds = new util.HashSet[String]()
        val events = new util.ArrayList[String]()
        var bool = true
        breakable(
          iter.foreach(log => {
            if ("clickItem".equals(log.evid)) {
              bool=false
              break()
            }
          })
        )
      }

    })


    ssc.start()
    ssc.awaitTermination()
  }
}
