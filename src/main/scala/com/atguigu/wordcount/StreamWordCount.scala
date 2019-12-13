package com.atguigu.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //利用传入参数
      val params=ParameterTool.fromArgs(args)
    val host= params.get("host")
    val port=params.getInt("port")
    //创建环境
       val env=StreamExecutionEnvironment.getExecutionEnvironment
    //接收socket 文本
    val textDStream=env.socketTextStream(host,port)

   //转换

    val dataStream= textDStream.flatMap(_.split("W+"))
      .filter(_.nonEmpty).map((_,1))
      .keyBy(0)
      .sum(1)

    dataStream.print()
      .setParallelism(1)

    //启动executor
    env.execute("my stream word count")

  }
}
