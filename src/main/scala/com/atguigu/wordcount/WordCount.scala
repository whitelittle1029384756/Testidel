package com.atguigu.wordcount

import org.apache.flink.api.scala.ExecutionEnvironment

object WordCount {
  def main(args: Array[String]): Unit = {
    //创建一个执行环境
      val env=ExecutionEnvironment.getExecutionEnvironment
    //从文件中读取数据
    val inpath="C:\\Users\\lzp\\IdeaProjects\\myflinktutorial\\src\\main\\resources\\hello.txt"
    val textDStream =env.readTextFile(inpath)
    import org.apache.flink.api.scala._
      val wordCount=textDStream.flatMap(_.split("W+"))
        .map((_,1)).groupBy(0).sum(1)
     //打印输出
      wordCount.print()

  }


}
