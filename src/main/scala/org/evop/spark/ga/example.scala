import org.apache.spark._
import pGA._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration


object prog1 {
  
  def main(  args:  Array[String]  )  {
  
    
    println("NFC          "+TestFunctions.NFC)
    val ri=new RandomDoubleInitializer(1500, 1000,  TestFunctions.SphereBound ,  TestFunctions.SphereFunc  )
    println("NFC          "+TestFunctions.NFC)
    val parGA=new GA(  TestFunctions.SphereFunc,  ri,  "ROULETTE",  "INTERCHANGE",  "WEAKPARENT",  "UNIFORM",  "MIN",  30  ,  5  ,  "MAX_GENS",  1500,  5 )
    println("NFC          "+TestFunctions.NFC)
       
  }
  
}
