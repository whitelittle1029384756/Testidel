package com.atguigu.sinktest

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
object JDBCSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.readTextFile("C:\\Users\\lzp\\IdeaProjects\\myflinktutorial\\src\\main\\resources\\sensor.txt")

    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })

    dataStream.addSink(new MyjdbcSink())
    dataStream.print()
    env.execute("jdbc sink test")

  }
}




class MyjdbcSink() extends  RichSinkFunction[SensorReading]{
  //定义连接和预编译语句
  var conn:Connection = _
   var insertStmt:PreparedStatement = _
  var updataStmt:PreparedStatement = _

  //在open生命周期中创建连接和预编译语句
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://hadoop104:3306/db1","root","123456")
   insertStmt = conn.prepareStatement("insert into temperature (id,timestamp,temperature) values(?,?,?)")
   updataStmt = conn.prepareStatement("update temperature set timestamp = ? ,temperature=? where id = ?")
  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
     updataStmt.setLong(1,value.timestamp)
    updataStmt.setDouble(2,value.temperature)
    updataStmt.setString(3,value.id)
    updataStmt.execute()
    if(updataStmt.getUpdateCount == 0){
      insertStmt.setString(1,value.id)
      insertStmt.setLong(2,value.timestamp)
      insertStmt.setDouble(3,value.temperature)
      insertStmt.execute()
    }



  }

  override def close(): Unit = {
    insertStmt.close()
    updataStmt.close()
    conn.close()
  }
}