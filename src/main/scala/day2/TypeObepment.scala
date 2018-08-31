package day2

import day1.LoggerLevels
import org.apache.spark.{SparkConf, SparkContext}

object TypeObepment {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf=new SparkConf().setAppName("TypeObepment").setMaster("local")
    val sc=new SparkContext(conf)
    val dr=sc.textFile(args(0)).map(x=>{
      val array=x.split('|')
      //(10000.0,南京-鼓楼区,经验不限,学历不限,民营 Java开发)
      (array(array.length-7),array(array.length-6),array(array.length-5),array(array.length-4),array(array.length-2),array(array.length-1).trim)
    }).cache()


    val typemoney=dr.map(t=>{
     // val ar=t._2.split('-')
      (t._6,t._1.toDouble)
    }).reduceByKey(_+_)
    val counttype=dr.map(t=>{
      // val ar=t._2.split('-')
      (t._6,1)
    }).reduceByKey(_+_)
    val avgmony=typemoney.join(counttype).map(x=>{
      val avg=x._2._1/x._2._2
      (x._1,avg)
    }).sortBy(_._2,false)
    val shendu=avgmony.filter(_._1=="深度学习")
//计算前端工资
    val rdd=dr.filter(_._6=="Web前端")
    val  qianduanaremoney=rdd.map(t=>{
      val ar=t._2.split('-')
      (ar(0),t._1.toDouble)
    }).reduceByKey(_+_)
    val  qianduanare=rdd.map(t=>{
      val ar=t._2.split('-')
      (ar(0),1)
    }).reduceByKey(_+_)
    val qianduanmony= qianduanare.join( qianduanaremoney).map(x=>{
      val avg=x._2._2/x._2._1
      (x._1,avg)
    }).sortBy(_._2,false).filter(_._1=="成都")
    println(qianduanmony.collect().toBuffer)
  }
}
