package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import collection.JavaConverters._

/**
 * @ClassName SaleDetailApp
 * @Description TODO
 * @Date 2021/7/7 18:37
 * */
object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val orderInfoKafkaDStream = MyKafkaUtil
      .getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, ssc)

    val orderDetailKafkaDStream = MyKafkaUtil
      .getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    val orderIdToOrderInfoDStream = orderInfoKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        (orderInfo.id, orderInfo)
      })
    })

    val orderIdToOrderDetailDStream = orderDetailKafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val orderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })
    })

    val fullOuterDStream = orderIdToOrderInfoDStream
      .fullOuterJoin(orderIdToOrderDetailDStream)

    val noUserDStream = fullOuterDStream.mapPartitions(partition => {
      //导入隐式转换，将样例类转为json
      implicit val formats = org.json4s.DefaultFormats

      //创建Redis连接
      val jedis = new Jedis("hadoop102", 6379)

      //创建结果集合存放收集的数据,每个分区一个结果集合
      val saleDetails = new util.ArrayList[SaleDetail]()

      partition.foreach { case (order_id, (orderOpt, detailOpt)) =>
        //设置order存放入redis的key
        val orderRedisKey = "orderInfo:" + order_id

        //设置orderDetail存入redis的key
        val detailRedisKey = "orderDetail:" + order_id

        //判断order是否存在
        if (orderOpt.isDefined) {
          //将存在的order接收
          val order = orderOpt.get

          //判断orderDetail是否存在
          if (detailOpt.isDefined) {

            //接收存在的orderDetail
            val orderDetail = detailOpt.get

            //将关联上的order和orderDetail存入结果集
            val saleDetail = new SaleDetail(order, orderDetail)
            saleDetails.add(saleDetail)
          }
          //只要order存在，无论orderDetail是否存在都将order写入redis，因为order可能和多个orderDetail关联
          //将样例类转换为jsonString
          val orderInfoJSON = Serialization.write(orderOpt)

          //以String类型保存入redis
          jedis.set(orderRedisKey, orderInfoJSON)

          //设置过期时间为10秒，不能一直等待orderDetail，给个时间界限
          jedis.expire(orderRedisKey, 10)

          //只要order存在就去redis的缓存区读取是否有orderDetail能关联
          //判断是否存在能关联的orderDetail
          if (jedis.exists(detailRedisKey)) {
            //读取关联上的orderDetail，可能有多个，所以用set存储和取出
            val details = jedis.smembers(detailRedisKey)

            //转为scala的set进行遍历
            for (elem <- details.asScala) {
              //将读取的String转为orderDetail样例类并存入结果集合
              val orderDetail = JSON.parseObject(elem, classOf[OrderDetail])
              saleDetails.add(new SaleDetail(order, orderDetail))
            }
          }
          // 如果order不存在则orderDetail存在
        } else {
          //获取orderDetail
          val detail = detailOpt.get

          //如果redis缓存中存在order
          if (jedis.exists(orderRedisKey)) {

            //读取order数据将其转为样例类
            val orderJSONStr = jedis.get(orderRedisKey)
            val orderInfo = JSON.parseObject(orderJSONStr, classOf[OrderInfo])

            //将其存入结果集合
            saleDetails.add(new SaleDetail(orderInfo, detail))

            //redis缓存中不存在order
          } else {
            //将样例类转为json
            val detailJSONStr = Serialization.write(detail)

            //缓存入redis等待order读取
            jedis.sadd(detailRedisKey, detailJSONStr)
            //设置过期时间
            jedis.expire(detailRedisKey, 10)
          }
        }
      }
      //关闭redis连接
      jedis.close()
      //转为scala迭代器返回
      saleDetails.asScala.toIterator
    })

    //从redis中获取userInfo的数据
    val saleDetailDStream = noUserDStream.mapPartitions(partition => {
      //创建redis连接
      val jedis = new Jedis("hadoop102", 6379)
      val details = partition.map(saleDetail => {
        val useInfoRedisKey = "userInfo:" + saleDetail.user_id

        //查找redis中userInfo的数据
        val userInfoJSONStr = jedis.get(useInfoRedisKey)
        //转为样例类
        val userInfo = JSON.parseObject(userInfoJSONStr, classOf[UserInfo])
        saleDetail.mergeUserInfo(userInfo)
        saleDetail
      })
      jedis.close()
      details
    })

    saleDetailDStream.print()

    //将数据写入ES
    saleDetailDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        //创建索引Index
        val indexName = GmallConstants.ES_DETAIL_INDEXNAME + "-" + sdf.format(new Date(System.currentTimeMillis()))

        val list = partition.toList.map(saleDetail => {
          (saleDetail.order_detail_id, saleDetail)
        })
        MyEsUtil.insertBulk(indexName, list)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}