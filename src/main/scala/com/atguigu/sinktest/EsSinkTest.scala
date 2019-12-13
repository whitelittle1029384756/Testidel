package com.atguigu.sinktest

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.http.HttpHost
import java.util

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.elasticsearch.client.Requests
import org.apache.flink.api.scala._

object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env =StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //定义es的httphost配置信息
    val httpHosts=new util.ArrayList[HttpHost]()

     httpHosts.add(new HttpHost("hadoop104",9200))

    //创建一个essink的builder
    val esSinkBuilder= new ElasticsearchSink.Builder[SensorReading](httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(element: SensorReading, runtimeContext: RuntimeContext, indexer: RequestIndexer): Unit = {
          //用hashMap作为插入es的数据类型
          val sourceData =new util.HashMap[String,String]()
          sourceData.put("sensor_id",element.id)
          sourceData.put("temperature",element.temperature.toString)
          sourceData.put("ts",element.timestamp.toString)
          //创建一个index request
          val indexRequest=Requests.indexRequest().index("sensor").`type`("readingData")
            .source(sourceData)
          //用indexer发送请求
          indexer.add(indexRequest)
          println(element + "saved successfully")

        }

      })
    val inputStream = env.readTextFile("C:\\Users\\lzp\\IdeaProjects\\myflinktutorial\\src\\main\\resources\\sensor.txt")

    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
    dataStream.addSink(esSinkBuilder.build())

    dataStream.print()
    env.execute("es sink test")

  }
}
