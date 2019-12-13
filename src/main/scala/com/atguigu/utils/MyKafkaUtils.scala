package com.atguigu.utils

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object MyKafkaUtils {

   def getConsumer (topicid:String) ={
     val properties=new Properties()
     properties.setProperty("bootstrap.servers","hadoop104:9092,hadoop105:9092,hadoop106:9092")
     properties.setProperty("group.id","consumer-group")
     properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
     properties.setProperty("value.deserizlizer","org.apache.kafka.common.serialization.StringDeserializer")
     properties.setProperty("auto.offset.reset","latest")

   val mykafkaConsumer=  new FlinkKafkaConsumer011[String](topicid, new SimpleStringSchema(), properties)
     mykafkaConsumer
   }
}
