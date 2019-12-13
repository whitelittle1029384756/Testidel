package com.atguigu.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
object WordCount2 {
  def main(args: Array[String]): Unit = {
    val params=ParameterTool.fromArgs(args)
    val inpath= params.get("inpath")
    val outpath=params.get("outpath")
    //创建一个执行环境
    val env=ExecutionEnvironment.getExecutionEnvironment
    //从文件中读取数据

    val textDStream =env.readTextFile(inpath)
    import org.apache.flink.api.scala._
    val wordCount=textDStream.flatMap(_.split("W+"))
      .map((_,1)).groupBy(0).sum(1)
    //打印输出

    wordCount.writeAsText(outpath)

  }
}
