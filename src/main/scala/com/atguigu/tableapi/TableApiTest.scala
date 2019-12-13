package com.atguigu.tableapi

import com.alibaba.fastjson.JSON
import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import com.atguigu.utils._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment

object TableApiTest {
  def main(args: Array[String]): Unit = {

     val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
    val myKafkaConsumer = MyKafkaUtils.getConsumer("sensor")
     val inputstream=env.addSource(myKafkaConsumer)
     val tableEnv=TableEnvironment.getTableEnvironment(env)

      val sensorstream=inputstream.map({
         data =>{
             val array = data.split(",")
           SensorReading(array(0).trim(),array(1).trim().toLong,array(2).trim.toDouble)
         }
      })
     val temperatureTable =tableEnv.fromDataStream(sensorstream)

       val table=temperatureTable.select("id,temperature").filter("id='sensor_6'")
//    val midchDataStream:DataStream[(String,String)] = table.toAppendStream[(String,String)]
     val midchDataStream= tableEnv.toAppendStream[(String,Double)](table)
     midchDataStream.print()

      env.execute("table")

  }
}
