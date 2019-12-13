package com.atguigu.apitest

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

//    val inputStream = env.readTextFile("C:\\Users\\lzp\\IdeaProjects\\myflinktutorial\\src\\main\\resources\\sensor.txt")
       val inputStream= env.socketTextStream("localhost",7777)
    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
//.每个传感器每隔15秒输出这段时间内的最小值
    val minTempPerWindowStream=dataStream
        .keyBy(_.id)
        .timeWindow(Time.seconds(15))
      .minBy("temperature")
//      .reduce((x,y) => SensorReading(x.id,y.timestamp,x.temperature.min(y.temperature)))

      dataStream.print("data")
      minTempPerWindowStream.print("mintemperature")
    env.execute("window test")



  }
}
