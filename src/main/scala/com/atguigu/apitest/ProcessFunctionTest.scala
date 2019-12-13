package com.atguigu.apitest

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object ProcessFunctionTest {

    def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      //设置处理的时间为事件时间
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      //设置自动获取watermark
      env.getConfig.setAutoWatermarkInterval(300L)
      //    val inputStream = env.readTextFile("C:\\Users\\lzp\\IdeaProjects\\myflinktutorial\\src\\main\\resources\\sensor.txt")
      val inputStream= env.socketTextStream("localhost",7777)
      val dataStream = inputStream
        .map(data => {
          val dataArray = data.split(",")
          SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
        })
        //设置周期生成watermark   传入延迟参数，方法中返回一个时间戳
      /*  .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1000)) {
        override def extractTimestamp(t: SensorReading): Long = {
          t.timestamp*1000L
        }
      })*/
      //      .assignTimestampsAndWatermarks( new MyAssigner())

      //温度持续上升报警
       val processedStream=dataStream
           .keyBy(_.id)
           .process( new TempIncreseWarning())

      //低温冰点报警
      val freezingMonitorStream=dataStream
          .process(new FreezingMonitor())
   //温度跳变报警
      val tempChangeStream=dataStream
          .keyBy(_.id)
//          .flatMap( new TempChangeWarning(10.0))
        //输入一个函数，在函数中定义逻辑
        //第一个类型为out输出类型，
          .flatMapWithState[(String,Double,Double,String),Double]({
        //如果状态为空，第一条数据
        case (in:SensorReading,None) => (List.empty,Some(in.temperature))
          //如果状态不为空，判断
        case (in:SensorReading,lastTemp:Some[Double])=>{
          val diff =(in.temperature - lastTemp.get).abs
          if(diff > 10.0){
            (List((in.id,lastTemp.get,in.temperature,"change too much")),Some(in.temperature))
          }else {
            (List.empty,Some(in.temperature))
          }
        }

      })

      dataStream.print("data")
//      processedStream.print("process")
//      freezingMonitorStream.print("healthy")
//      freezingMonitorStream.getSideOutput(new OutputTag[(String,String)]("freezing-warning")).print("freezing")

     tempChangeStream.print("change")
      env.execute("ProcessFunction test")



    }





}





class TempIncreseWarning() extends KeyedProcessFunction[String,SensorReading,String] {
  //定义一个转态，用户保存上一次的温度值
  lazy val lastTemp =getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp.state",Types.of[Double]) )
  //定义一个状态，用于保存定时器的时间戳
  lazy val currentTimer=getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer-state",Types.of[Long]) )

  override def processElement(value: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {

    //取出上一次的温度值
    val prevTemp =lastTemp.value()
    lastTemp.update(value.temperature)
    val curTimerTs=currentTimer.value()
    //如果温度上升，并且没有设置过定时器，就注册一个定时器
    //状态默认值为0
    if(value.temperature > prevTemp && curTimerTs == 0){
     val timeTs =  context.timerService().currentProcessingTime() + 10000L
      context.timerService().registerProcessingTimeTimer(timeTs)
   //保存时间戳到状态
      currentTimer.update(timeTs)


    }else if(value.temperature <= prevTemp){
      //温度下降，删除定时器
      context.timerService().deleteProcessingTimeTimer(curTimerTs)
      //记得清除当前时间戳
      currentTimer.clear()
    }


  }
  //触发定时器
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("sensor "+ctx.getCurrentKey + "温度10s持续上升")
    currentTimer.clear()
  }
}


class FreezingMonitor() extends ProcessFunction[SensorReading,(String,Double,String)] {
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, (String, Double, String)]#Context, out: Collector[(String, Double, String)]): Unit = {
    if(value.temperature < 32.0){
       ctx.output(new OutputTag[(String,String)]("freezing-warning"),(value.id,"freezing"))
    }else {
      out.collect(value.id,value.temperature,"healthy")
    }

  }
}

//自定义富函数
class TempChangeWarning(threshold: Double)extends  RichFlatMapFunction[SensorReading,(String,Double,Double,String)] {

  //定义状态 如何获取上次的值
  private var lastTempState :ValueState[Double]= _

  override def open(parameters: Configuration): Unit = {
    lastTempState=getRuntimeContext.getState( new ValueStateDescriptor[Double]("lastTemp-state",classOf[Double]))

  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double, String)]): Unit = {
//先取出上一次的温度
    val lastTemp= lastTempState.value()
    //根据温度值和上次的差值，判断是否输出报警
    val diff=(lastTemp-value.temperature).abs
    if(diff >= threshold){
      out.collect(value.id,value.temperature,lastTempState.value,"change too much")
    }
    //更新状态
     lastTempState.update(value.temperature)

  }


}