object Test {
  def main(args: Array[String]): Unit = {
   val arr="中铁骑士公馆套二   有装修     户型方正|2室1厅|69.86平米|东|精装|有电梯|新双楠|137|19611"
    val a=arr.split('|')
    print(a(0))
  }
}
