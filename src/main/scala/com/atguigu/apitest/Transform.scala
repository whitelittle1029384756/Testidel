package com.atguigu.apitest
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
object Transform {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
       env.setParallelism(1)
  //1.简单转换和滚动聚合算子测试
    val dataStream= env.readTextFile("C:\\Users\\lzp\\IdeaProjects\\myflinktutorial\\src\\main\\resources\\sensor.txt")
      .map(data=>{
        val dataArray=data.split(",")
        SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
      })

   val aggStream= dataStream .keyBy("id")
//      .max("temperature")
        .reduce((x,y)=>SensorReading(x.id,x.timestamp+1,y.temperature+10))

    //2.多流转换算子测试
    val splitStream=dataStream
        .split(sensorData =>{
          //根据温度值高低划分不同的流
          if(sensorData.temperature >30)Seq("high") else Seq("low")
        })

    val lowTempStream=splitStream.select("low")
    val highTempStream=splitStream.select("high")
    val allTempStream=splitStream.select("high","low")

    //3.合并两条流
    val warningStream=highTempStream.map(data => (data.id,data.temperature))
    val connectedSteams=warningStream.connect(lowTempStream)

    val coMapStream=connectedSteams.map(
      warningData => (warningData._1,warningData._2,"high temperature warning"),
      lowData =>(lowData.id,"healthy")
    )
    //类型必须一致
    val unionStream=highTempStream.union(lowTempStream,allTempStream)

   //4.UDF测试
    dataStream.filter(new MyFilter()).print("filter")
    dataStream.filter(_.id.startsWith("sensor_1"))






//    aggStream.print()
//    lowTempStream.print("low1")
//    highTempStream.print("high2")
//    allTempStream.print("all")
//    coMapStream.print("coMap stream")

    env.execute("transform")




  }
}


class MyFilter() extends FilterFunction[SensorReading]{
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith("sensor_")
  }
}

class MyMapper() extends RichMapFunction[SensorReading,Int] {
  override def map(in: SensorReading): Int = {
    0
  }

  override def open(parameters: Configuration): Unit = {
      super.open(parameters)
  }
}