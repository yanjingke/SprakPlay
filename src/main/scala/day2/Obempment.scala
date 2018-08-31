package day2

import day1.LoggerLevels
import org.apache.spark.{SparkConf, SparkContext}

object Obempment {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf=new SparkConf().setAppName("Obempment").setMaster("local")
    val sc=new SparkContext(conf)
    val dr=sc.textFile(args(0)).map(x=>{
      val array=x.split('|')
      //(10000.0,南京-鼓楼区,经验不限,学历不限,民营 )
      (array(1),array(2),array(3),array(4),array(6))
    }).cache()
    val aremoney=dr.map(t=>{
      val ar=t._2.split('-')
      (ar(0),t._1.toDouble)
    }).reduceByKey(_+_)
    val are=dr.map(t=>{
      val ar=t._2.split('-')
      (ar(0),1)
    }).reduceByKey(_+_)
    val areavag=are.join(aremoney).map(x=>{
      val avg=x._2._2/x._2._1
      (x._1,avg)
    }).sortBy(_._2,false)
    val topareavag=are.join(aremoney).map(x=>{
      val avg=x._2._2/x._2._1
      (x._1,avg)
    }).sortBy(_._2,false).take(10)
    val chengduavg=areavag.filter(_._1=="成都")
    //println(chengduavg.collect().toBuffer)
    println(topareavag.toBuffer)
    sc.stop()
  }

}
