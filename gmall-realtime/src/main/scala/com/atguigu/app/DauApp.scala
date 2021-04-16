package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DauApp {
  def main(args: Array[String]): Unit = {
   //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    //3.消费kafka中数据
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    //4.将json格式的数据转化为样例类,并补全logDate和logHour
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaStream.mapPartitions(partition => {
      partition.map(record => {
        //a.将数据转化为样例类
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        //b.补全logDate需要对时间戳做格式化
        //yyyy-MM-dd HH
        val times: String = sdf.format(new Date(startUpLog.ts))
        startUpLog.logDate = times.split(" ")(0)
        //c.补全logHour
        startUpLog.logHour = times.split(" ")(1)

        startUpLog
      })
    })


    //优化：因为多次使用，所以价加个缓存
    startUpLogDStream.cache()

   //5.跨批次去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream,ssc.sparkContext)

    filterByRedisDStream.cache()
    //原始数据条数
    startUpLogDStream.count().print()
    //经过跨批次去重后的数据条数
    filterByRedisDStream.count().print()

    //6.批次内去重
    val filterByMidDStream: DStream[StartUpLog] = DauHandler.filterByMid(filterByRedisDStream)
    //经过批次内去重后的一个数据条数
    filterByMidDStream.cache()
    filterByMidDStream.count().print()

    //7.将去重后的结果mid写入redis，方便下个批次的数据做去重
    DauHandler.saveMidToRedis(filterByMidDStream)

    //8.将去重后的数据写入Hbase
    filterByMidDStream.foreachRDD(rdd=>{
      rdd.saveToPhoenix(
      "GMALL1116_DAU",
      Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
      HBaseConfiguration.create,
      Some("hadoop102,hadoop103,hadoop104:2181"))
    })


//    kafkaStream.foreachRDD(rdd=>{
//      rdd.foreach(record=>{
//        println(record.value())
//      })
//    })

    ssc.start()
    ssc.awaitTermination()
  }
}
