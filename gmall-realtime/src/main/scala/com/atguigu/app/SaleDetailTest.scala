package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import java.util
import scala.collection.JavaConverters._

/**
 * @ClassName SaleDetailTest
 * @Description TODO
 * @Date 2021/7/8 16:35
 * */
object SaleDetailTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SaleDetailTest").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val orderInfoDStream = MyKafkaUtil
      .getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    val orderDetailDStream = MyKafkaUtil
      .getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    val orderIdToOrderInfoDStream = orderInfoDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        (orderInfo.id, orderInfo)
      })
    })

    val orderIdToOderDetailDStream = orderDetailDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })
    })

    val fullOuterDStream = orderIdToOrderInfoDStream
      .fullOuterJoin(orderIdToOderDetailDStream)

    val noUserDStream = fullOuterDStream.mapPartitions(partition => {
      implicit val formats = org.json4s.DefaultFormats
      val jedis = new Jedis("hadoop102", 6379)
      val details = new util.ArrayList[SaleDetail]()
      partition.foreach { case (order_id, (infoOpt, detailOpt)) =>
        val orderInfoRedisKey = "orderInfo:" + order_id
        val orderDetailRedisKey = "orderDetail:" + order_id
        if (infoOpt.isDefined) {
          val orderInfo = infoOpt.get
          if (detailOpt.isDefined) {
            val orderDetail = detailOpt.get
            details.add(new SaleDetail(orderInfo, orderDetail))
          }
          val orderInfoJsonStr = Serialization.write(orderInfo)
          jedis.set(orderInfoRedisKey, orderInfoJsonStr)
          jedis.expire(orderInfoRedisKey, 10)
          if (jedis.exists(orderDetailRedisKey)) {
            val orderDetailJsonStrSet = jedis.smembers(orderDetailRedisKey)
            for (elem <- orderDetailJsonStrSet.asScala) {
              val orderDetail = JSON.parseObject(elem, classOf[OrderDetail])
              details.add(new SaleDetail(orderInfo, orderDetail))
            }
          }
        } else {
          if (detailOpt.isDefined) {
            val orderDetail = detailOpt.get
            if (jedis.exists(orderInfoRedisKey)) {
              val orderInfoJsonStr = jedis.get(orderInfoRedisKey)
              val orderInfo = JSON.parseObject(orderInfoJsonStr, classOf[OrderInfo])
              details.add(new SaleDetail(orderInfo, orderDetail))
            } else {
              val orderDetailJsonStr = Serialization.write(orderDetail)
              jedis.sadd(orderDetailRedisKey, orderDetailJsonStr)
              jedis.expire(orderDetailRedisKey, 10)
            }
          }
        }
      }
      jedis.close()
      details.asScala.toIterator
    })

    val saleDetailDStream = noUserDStream.mapPartitions(partition => {
      val jedis = new Jedis("hadoop102", 6379)
      partition.map(saleDetail => {
        val userInfoRedisKey = "userInfo:" + saleDetail.user_id
        val userInfoJsonStr = jedis.get(userInfoRedisKey)
        val userInfo = JSON.parseObject(userInfoJsonStr, classOf[UserInfo])
        saleDetail.mergeUserInfo(userInfo)
        saleDetail
      })
      jedis.close()
      partition
    })

    saleDetailDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val list = partition.toList.map(saleDetail => {
          (saleDetail.order_detail_id, saleDetail)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_DETAIL_INDEXNAME + "-0225", list)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
