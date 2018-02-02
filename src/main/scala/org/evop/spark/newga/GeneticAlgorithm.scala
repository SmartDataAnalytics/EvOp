package org.evop.spark.newga

import scala.util.control.Breaks._
import java.util.Calendar


object GeneticAlgorithm {
  def main(  args:  Array[String]  )  {
    var A  =  List(0,1,2,3,40,5,6)
    var B  =  A.zipWithIndex
    var C  =  B.filter(_._2>3)
    C.foreach(println)
    
    println(Calendar.getInstance().getTime)
    
    A  =  A.filter(_==0)  
      println(A.length)
    var t:Int=0
    for(i <- 1 to 5){
      if(i==10)
        break
      t+=1
      println(i)
    }
    println("t =  "+t)
    
    var  myArray  =  Array(2,5,7,1,6,7,1,3,5) 
    println(  myArray.map(  x  =>  x  ).min  )
//    val dest= List(1,2,4,5,7,8,9)
//val points = List(1,5,9)
//val xs = points.toSet
//val newl = dest.filter(!xs.contains(_))
//newl.foreach(println)
    var Thresh:Double =  1.toDouble/100.toDouble
    println(Thresh)
  }
 
  
}