package com.atguigu.apitest


import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
     //设置处理的时间为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
     //设置自动获取watermark 时间是系统的处理时间
    //系统时间不停的变化，每隔一段设置延迟生成一个watermark
    env.getConfig.setAutoWatermarkInterval(300L)
    env.enableCheckpointing(60000L)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    env.getCheckpointConfig.setCheckpointTimeout(30000L)
    //文件系统
//     env.setStateBackend(new FsStateBackend(""))

//    println(systemwatermark)
//    val inputStream = env.readTextFile("C:\\Users\\lzp\\IdeaProjects\\myflinktutorial\\src\\main\\resources\\sensor.txt")
       val inputStream= env.socketTextStream("localhost",7777)
    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
    //设置周期生成watermark   传入延迟参数，方法中返回一个时间戳
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1000)) {
      override def extractTimestamp(t: SensorReading): Long = {
           t.timestamp*1000L
      }
    })
//      .assignTimestampsAndWatermarks( new MyAssigner())
    val outputTag = new OutputTag[SensorReading]("side")
//.每个传感器每隔15秒输出这段时间内的最小值
    val minTempPerWindowStream=dataStream
        .keyBy(_.id)
      //时间是事件的时间
        .timeWindow(Time.seconds(15))
      //允许关窗时间延迟10s，时间太长会严重影响内存
      .allowedLateness(Time.seconds(10))
      .sideOutputLateData(outputTag)
//      .countWindow(2)
      .minBy("temperature")
//      .reduce((x,y) => SensorReading(x.id,y.timestamp,x.temperature.min(y.temperature)))

      dataStream.print("data")
      minTempPerWindowStream.print("mintemperature")
    minTempPerWindowStream.getSideOutput(outputTag).print("side")
    env.execute("window test")



  }
}


class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading]{
  //定义一个最大的延迟时间
  val bound = 1000L
  //定义一个当前最大的时间戳
  var maxTs= Long.MinValue
  override def getCurrentWatermark: Watermark = {
    val watermark=  new Watermark(maxTs - bound)
       println("当前watermark="+watermark.toString)
    watermark
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
     maxTs=maxTs.max(t.timestamp*1000L)
    t.timestamp*1000L
  }
}

//根据每个数据的时间戳生成watermark
class MyAssigner2 extends AssignerWithPunctuatedWatermarks[SensorReading]{
  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
      if(lastElement.id == "sersor_1"){
        new Watermark(extractedTimestamp)
      }else{
        null
      }
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    t.timestamp*1000L

  }
}