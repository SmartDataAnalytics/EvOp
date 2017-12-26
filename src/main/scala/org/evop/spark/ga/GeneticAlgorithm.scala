package org.evop.spark.ga

object GeneticAlgorithm {
  def main(  args:  Array[String]  )  {
    var A  =  List(0,1,2,3,40,5,6)
    var B  =  A.zipWithIndex
    var C  =  B.filter(_._2>3)
    C.foreach(println)
    
    
    val dest= List(1,2,4,5,7,8,9)
val points = List(1,5,9)
val xs = points.toSet
val newl = dest.filter(!xs.contains(_))
newl.foreach(println)
  }
  
}