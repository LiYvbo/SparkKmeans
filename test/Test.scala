/**
 * Created by liyubo on 2016/6/17.
 */
object Test {
 def main (args: Array[String]) {
   val WIDTH = math.sqrt(1)/3
   val intval_0 = Map[String,Double]("min"->0.0,"max"->WIDTH)
   val intval_1 = Map[String,Double]("min"->WIDTH,"max"->WIDTH*2)
   val intval_2 = Map[String,Double]("min"->WIDTH*2,"max"->WIDTH*3)

   println(intval_0("min"))
   println(intval_0("max"))
   println(intval_1("min"))
   println(intval_1("max"))
   println(intval_2("min"))
   println(intval_2("max"))
  }
}
