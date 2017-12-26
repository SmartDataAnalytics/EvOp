package org.evop.spark.ga

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.catalyst.expressions.Sqrt
import org.apache.commons.math3.analysis.function.Sqrt
import breeze.linalg.split
import java.io.File
import java.io.PrintWriter
import scala.io.Source
import scala.tools.cmd.Spec.Accumulator

case object TestFunctions {
  
  //value Bounds of Optimization Functions
  val AckleyBound  :  Array[Int]  =  Array  (  -33  ,  33  )
  val SphereBound  :  Array[Int]  =  Array  (  -6  ,  6  )
  val SumOfDiffPowersBound  :  Array[Int]  =  Array  (  -1  ,  1  )
  val GriewankBound  :  Array[Int]  =  Array  (  -600  ,  600  )
  val conf = new SparkConf().setAppName("Parallel GA").setMaster("spark://172.18.160.16:3077") 
    .setMaster("local[*]")
// local mode
//  .setMaster("local[*]")
//  .set("spark.executor.memory","7g")
//  .set("spark.driver.memory","3g")
//  .set("spark.driver.maxResultSize","0")
    
  //Create Spark Context
  val sc = new SparkContext(conf)
  
  var NFC = sc.longAccumulator("Number of Function Calls")
  //var NFC  = 0
  
  //Simple Test Function
  def FitnessFunc  (  Alleles:Array[Gene]  )  :  Double =  {
      var fitness:Double  =  0.0
      for(i <- 0 to Alleles.length-1)  {
        fitness  =  fitness  +  (  Alleles(i).Allele * Math.pow(-1, i)  )
      }
      fitness
      
      }
 
  
  /*
   * MANY LOCAL MINIMA FUNCTION
   */
  
  //  Ackley    -    Dimensions : d    -    GlobalMinima : f( 0, ... , 0 ) = 0
  def AckleyFunc  (  Alleles:Array[Gene]  )  :  Double =  {
    val a  =  20
    val b  =  0.2
    val c  =  2  *  3.1416
    val d  =  Alleles.length
    var sum  =  Ackley  (Alleles  ,  c  )
    val Result : Double  =  -a  *  math.exp( -b * math.sqrt (  sum._1 / d )  )  -  math.exp( sum._2 / d )  +  a  +  math.exp(1)
    Result
  }
  
  def Ackley  (  Alleles  :  Array[Gene]  ,  c  :  Double    )  :  (  Double  ,  Double  )   =  {
    if (  Alleles.length  ==  1  )  {
      var sum1  :  Double  =  Math.pow  (  Alleles(0).Allele  , 2)
      var sum2  :  Double  =  Math.cos  (  c  *    Alleles(0).Allele  )
      (  sum1  ,  sum2  )
    }
    else  {
      var sum1  =  (  0.0  ,  0.0  )
      var sum2  =  (  0.0  ,  0.0  )
      sum1  =  Ackley  (  Alleles.slice(  0  ,  Alleles.length/2  )  ,  c  )
      sum2  =  Ackley  (  Alleles.slice(  Alleles.length/2  ,  Alleles.length  )  ,  c  )
      (  sum1._1  +  sum2._1  ,  sum1._2  +  sum2._2    )
    }
  }
  
  
  //  Griewank    -    Dimensions:d    -    GlobalMinima : f( 0, ... , 0 ) = 0
  def GriewankFunc  (  Alleles:Array[Gene]  )  :  Double =  {
    val  Result  =  Griewank  (  Alleles  )
    Result._1  -  Result._2  +  1
  }
  
  def Griewank  (  Alleles:Array[Gene]  ,  baseIndex:Int  =  1  )  :  (  Double  ,  Double  ) =  {
    if  (  Alleles.length  ==  1  )  {
      (  Math.pow(  Alleles(0).Allele  ,  2  )  /  4000  ,  Math.cos(  Alleles(0).Allele  /  Math.sqrt(  baseIndex  )  )  )
    }
    else  {
      val  R1  =  Griewank  (  Alleles.slice(  0  ,  Alleles.length/2)  ,  baseIndex  )
      val  R2  =  Griewank  (  Alleles.slice(  Alleles.length/2  ,  Alleles.length  )  ,  baseIndex  +  Alleles.length/2  )
      (  R1._1  +  R2._1  ,  R1._2  +  R2._2  )
    }
  }
  
  
  
  //  Rastrigin    -    Dimensions:d    -    GlobalMinima : f( 0, ... , 0 ) = 0
  def RastriginFunc  (  Alleles:Array[Gene]  )  :  Double =  {
    (  10  *  Alleles.length.toDouble  )  +  Rastrigin  (  Alleles  )
  }
  
  def Rastrigin  (  Alleles:Array[Gene]  )  :  Double =  {
    if  (  Alleles.length  ==  1  )  {
      Math.pow(  Alleles(0).Allele  ,  2  )  -  10  *  Math.cos  (  2  *  3.1416  *  Alleles(0).Allele  )
    }
    else  {
      Rastrigin  (  Alleles.slice  (  0  ,  Alleles.length/2  )  )    +  Rastrigin  (  Alleles.slice  (  Alleles.length/2  ,  Alleles.length  )  )
    }
  }
  
  //  Bukin    -    Dimensions : 2    -    GlobalMinima : f( -10  ,  1 ) = 0
  def BukinFunc  (  Alleles:Array[Gene]  )  :  Double =  {
    val  f  :  Double  =  100  *  Math.sqrt( Math.abs(  Alleles(1).Allele  -  (Alleles(0).Allele * 0.01)  ) )  +  0.01  *  Math.abs(  Alleles(0).Allele  +  10  )
    f
  }
  
  
  //  Cross-In-Tray    -    Dimensions : 2    -    GlobalMinima : f( 1.3491 , -1.3491 )  =  f( 1.3491 , 1.3491 )  =  f( -1.3491 , 1.3491 )  =  f( -1.3491 , -1.3491 )= -2.06261
  def CrossInTrayFunc  (  Alleles:Array[Gene]  )  :  Double =  {
    val  f  :  Double  =  -0.0001  *  Math.pow  (  Math.abs(  Math.sin(Alleles(0).Allele)  +  Math.sin(Alleles(1).Allele)  *  Math.exp(100)  )  +  1  ,  0.1  )
    f
  }
  
  
  

  
  
  
  
  /*
   * BOWL SHAPED FUNCTIONS
   */
  
  //  Sphere    -    Dimensions : d    -    GlobalMinima : f( 0, ... , 0 ) = 0
  def SphereFunc  (  Alleles:Array[Gene]  )  :  Double =  {
    
   var fitness:Double  =  0.0
   if  (  Alleles.length  ==  1  )
     fitness  =  Math.pow(Alleles(0).Allele, 2)
   
   else
     fitness  =   SphereFunc  (   Alleles.slice(0, Alleles.length/2)  )  +  SphereFunc  (   Alleles.slice(Alleles.length/2,  Alleles.length  )  )
   fitness
  }
  
  
  
  //  Sum Of Different Powers    -    Dimensions:d    -    GlobalMinima : f( 0, ... , 0 ) = 0
  def SumOfDiffPowersFunc  (  Alleles:Array[Gene]  ,  baseIndex:Int  =  1  )  :  Double =  {
   var fitness:Double  =  0.0
   if  (  Alleles.length  ==  1  )
     fitness  =  Math.pow  (  Alleles(0).Allele  ,  baseIndex  +  1  )
   else
     fitness  =   SumOfDiffPowersFunc  (   Alleles.slice(0, Alleles.length/2)  ,  baseIndex  )  +  SumOfDiffPowersFunc  (   Alleles.slice(Alleles.length/2,  Alleles.length  )    ,  baseIndex + Alleles.length/2  )
   fitness
  }
  
  
  //  Sum Squares    -    Dimensions:d    -    GlobalMinima : f( 0, ... , 0 ) = 0
  def SumSquaresFunc  (  Alleles:Array[Gene]  ,  baseIndex:Int  =  1  )  :  Double =  {
   var fitness:Double  =  0.0
   if  (  Alleles.length  ==  1  )
     fitness  =  baseIndex  *  Math.pow  (  Alleles(0).Allele  ,  2  )
   else
     fitness  =   SumOfDiffPowersFunc  (   Alleles.slice(0, Alleles.length/2)  ,  baseIndex  )  +  SumOfDiffPowersFunc  (   Alleles.slice(Alleles.length/2,  Alleles.length  )    ,  baseIndex + Alleles.length/2  )
   fitness
  }
  
  
  
  
  /*
   * Plate Shaped Functions
   */
  
  
  //  Zakharov    -    Dimensions:d    -    GlobalMinima : f( 0, ... , 0 ) = 0
  def ZakharovFunc  (  Alleles:Array[Gene]  )  :  Double =  {
    val  sum  =  Zakharov  (  Alleles  )
    sum._1  +  Math.pow  (  sum._2  ,  2  )  +  Math.pow  (  sum._2  ,  2  )
    0.0
  }
  
  def Zakharov  (  Alleles  :  Array[Gene]  ,  baseIndex  :  Int  =  1  )  :  (  Double  ,  Double  )  =  {
    var  sum1  :  Double  =  0
    var  sum2  :  Double  =  0
    if  (  Alleles.length  ==  1  )  {
      sum1  =  Math.pow  (  Alleles(0).Allele  ,  2  )
      sum2  =  0.5  *  baseIndex  *  Alleles(0).Allele
    }    else  {
      var temp1  =  Zakharov(  Alleles.slice(  0  , Alleles.length/2  )  ,  baseIndex  )
      var temp2  =  Zakharov(  Alleles.slice(  0  , Alleles.length/2  )  ,  baseIndex  )
      sum1  =  temp1._1  +  temp2._1
      sum2  =  temp1._2  +  temp2._2
    }
    (  sum1  ,  sum2  )
  }
  
  
  
  
}