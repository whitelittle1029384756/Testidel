package com.atguigu.sinktest


import java.sql.Time
import java.util.Properties

import com.atguigu.apitest.SensorReading
import com.mysql.jdbc.TimeUtil
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend

object KafkasinkTest2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val checkpointPath="C:\\Users\\lzp\\IdeaProjects\\myflinktutorial\\src\\main\\resources\\checkpoint"
     val backend = new RocksDBStateBackend(checkpointPath)
     env.setStateBackend(backend)
    env.setStateBackend(new FsStateBackend("C:/tmp/checkpoints"))
  env.enableCheckpointing(600000)
    //配置重启策略
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60,Time.of(10,TimeUtil.SECONDS)))
    //    val inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop104:9092,hadoop105:9092,hadoop106:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString
      })

    dataStream.addSink(new FlinkKafkaProducer011[String]("hadoop104:9092,hadoop105:9092,hadoop106:9092", "sinkTest", new SimpleStringSchema()))

    dataStream.print()

    env.execute("kafka sink test")
  }
}
