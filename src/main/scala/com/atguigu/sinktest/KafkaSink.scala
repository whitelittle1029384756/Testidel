package com.atguigu.sinktest

import java.util.Properties

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
object KafkaSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)

     val properties=new Properties()
     properties.setProperty("bootstrap.servers","hadoop104:9092,hadoop105:9092,hadoop106:9092")
    properties.setProperty("group.id","consumer-group")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserizlizer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","latest")


    val inputStream=env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
//     val inputStream=env.readTextFile("C:\\Users\\lzp\\IdeaProjects\\myflinktutorial\\src\\main\\resources\\sensor.txt")
    val dataStream=inputStream.map(
      data =>{
        val dataArray= data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString
      }
    )
    dataStream.addSink(new FlinkKafkaProducer011[String]("hadoop104:9092,hadoop105:9092,hadoop106:9092", "sinkTest", new SimpleStringSchema()))

      dataStream.print()




   env.execute("kafka sink test")

  }
}
