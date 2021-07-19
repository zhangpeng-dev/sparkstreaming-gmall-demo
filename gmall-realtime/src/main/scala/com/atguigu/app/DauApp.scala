package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream

import java.text.SimpleDateFormat
import org.apache.phoenix.spark._

object DauApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(
      GmallConstants.KAFKA_TOPIC_STARTUP, ssc
    )

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    val startUpLogDStream = kafkaDStream.mapPartitions(
      partitions => {
        partitions.map(record => {
          val startUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
          startUpLog.logDate = sdf.format(startUpLog.ts).split(" ")(0)
          startUpLog.logHour =sdf.format(startUpLog.ts).split(" ")(1)
          startUpLog
        })
      }
    )

    startUpLogDStream.cache()
    startUpLogDStream.count().print()

    val filterByRedisDStream = DauHandler.filterByRedis(startUpLogDStream, ssc.sparkContext)

    filterByRedisDStream.cache()
    filterByRedisDStream.count().print()

    val filterByGroupDStream = DauHandler.filterByGroup(filterByRedisDStream)

    filterByGroupDStream.cache()
    filterByGroupDStream.count().print()

    DauHandler.saveMidToRedis(filterByGroupDStream)

    filterByGroupDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL2021_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create,
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })



    //    kafkaDStream.foreachRDD(
    //      rdd => {
    //        rdd.foreachPartition(_.foreach(record => println(record.value())))
    //      }
    //    )

    ssc.start()
    ssc.awaitTermination()
  }
}
