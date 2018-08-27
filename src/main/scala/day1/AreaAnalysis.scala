package day1

import org.apache.spark.{SparkConf, SparkContext}

object AreaAnalysis {

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf= new SparkConf().setMaster("local").setAppName("AreaAnalysis")
    val sc=new SparkContext(conf)

    val dr=sc.textFile(args(0)).map(x=>{
      val arrs=x.split('|')
     arrs(2)=arrs(2).substring(0,arrs(2).length-2)
      //新双楠area,2室1厅,69.86平米,总价:137w,单价：19611平方米
      (arrs(arrs.length-3),(arrs(1),arrs(2),arrs(arrs.length-2).toDouble,arrs(arrs.length-1).toDouble))
    }).cache()
// areatotalhome((猛追湾,200), (东湖,400)
    val areatotalhome=dr.map(x=>{
      (x._1,1)
    }).reduceByKey(_+_)
    //(猛追湾,(2929000.0)), (东湖,(1.23808E7)), (成外,(2812000.0)), (东升镇,(2252800.0))
    val areatotalavg=dr.map(x=>{
      (x._1,x._2._4)
    }).reduceByKey(_+_)
   // (猛追湾,(2929000.0,200)), (东湖,(1.23808E7,400)), (成外,(2812000.0,200)), (东升镇,(2252800.0,200))
    val rdd=areatotalavg.join(areatotalhome)
    //地区平均房价
    val avghome=rdd.map(x=>{
     val avg= x._2._1/x._2._2
      (x._1,avg)
    })
    val sort=avghome.sortBy(_._2)
    val sig_city=avghome.filter(_._1=="犀浦")
    val totalm=dr.map(x=>{
      ("total",x._2._4)
    }).reduceByKey(_+_)
    val total=dr.map(x=>{
      ("total",1)
    }).reduceByKey(_+_)

    val jointo=total.join(totalm)
    val all_avg=jointo.map(x=>{
      ("avg",(x._2._2/x._2._1))
    })
    print( all_avg.collect().toBuffer)
    sc.stop()
  }
}



//case class HomeDr(room:String,ara:Double,adress:String,totalPra:Double,price:Double)