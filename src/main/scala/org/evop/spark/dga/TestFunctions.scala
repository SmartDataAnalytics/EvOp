package org.evop.spark.dga

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.catalyst.expressions.Sqrt
import org.apache.commons.math3.analysis.function.Sqrt
import breeze.linalg.split
//import java.io.File
//import java.io.PrintWriter
import scala.io.Source
import scala.tools.cmd.Spec.Accumulator
import scala.annotation.tailrec
import org.h2.index.BaseIndex

case object TestFunctions {
  
  //value Bounds of Optimization Functions
  val AckleyBound  :  Array[Int]  =  Array  (  -33  ,  33  )
  val SphereBound  :  Array[Int]  =  Array  (  -6  ,  6  )
  val SumOfDiffPowersBound  :  Array[Int]  =  Array  (  -1  ,  1  )
  val GriewankBound  :  Array[Int]  =  Array  (  -600  ,  600  )
  val RastriginBound  :  Array[Int]  =  Array  (  -5  ,  5  )
  val SumSquaresBound  :  Array[Int]  =  Array  (  -5  ,  5  )
  val ZakharovBound  :  Array[Int]  =  Array  (  -5  ,  10  )
  val SchwefelBound  :  Array[Int]  =  Array  (  -500  ,  500  )
  val RosenBound  :  Array[Int]  =  Array  (  -5  ,  10  )
  val DixonBound  :  Array[Int]  =  Array  (  -10  ,  10  )
  val RotatedBound  :  Array[Int]  =  Array  (  -66  ,  66  )
  //  Langermann''  Levy  Schwefel  Perm  Rotated-Hyper-Ellipsoid  Trid  Power-Sum
  //  Dixon-Price  RosenBrock  Michalewicz  Perm  Powell  Styblinski-Tang
  //var conf = new SparkConf().setAppName("Parallel GA")//.setMaster("spark://172.18.160.16:3077") 
 // .setMaster("local[*]")
// local mode
//  .setMaster("local[*]")
//  .set("spark.executor.memory","7g")
//  .set("spark.driver.memory","3g")
//  .set("spark.driver.maxResultSize","0")
    
  //Create Spark Context
//  var sc = new SparkContext(conf)
  
  var func  =  ""
//  var NFC = sc.accumulator(0)
  //var NFC  = 0
  
  //Simple Test Function
  def FitnessFunc  (  Alleles:Array[Gene]  )  :  Double =  {
      var fitness:Double  =  0.0
      for(i <- 0 to Alleles.length-1)  {
        fitness  =  fitness  +  (  Alleles(i).Allele  *  Math.pow(-1, i)  )
      }
      fitness
      
      }
 
  
  /*
   * MANY LOCAL MINIMA FUNCTION
   */
  
  //  Ackley    -    Dimensions : d    -    GlobalMinima : f( 0, ... , 0 ) = 0
  
  def AckleyFunc  (  Alleles:Array[Gene]  ,  accumulator:  Double  )  :  Double =  {
    val a  =  20
    val b  =  0.2
    val c  =  2  *  3.1416
    val d  =  Alleles.length
    var sum  =  Ackley  (Alleles.toList  ,  (  accumulator  ,  accumulator  )  )
    val Result : Double  =  -a  *  math.exp( -b * math.sqrt (  sum._1 / d )  )  -  math.exp( sum._2 / d )  +  a  +  math.exp(1)
    Result
  }
  
  @tailrec
  def Ackley  (  Alleles  :  List[Gene]  ,  accumulator  :  (  Double  ,  Double  )    )  :  (  Double  ,  Double  )   =  {
    val c  =  2  *  3.1416
    Alleles match  {
      case  Nil  =>  accumulator
      case  x::xs  =>  Ackley  (  Alleles.slice(  1  ,  Alleles.length  )  ,  
          (  accumulator._1  +  Math.pow  (  Alleles(0).Allele  , 2)  ,
              accumulator._2  +  Math.cos  (  c  *    Alleles(0).Allele  )  )  )
    }
    
  }
  
//  def AckleyFunc  (  Alleles:Array[Gene]  )  :  Double =  {
//    val a  =  20
//    val b  =  0.2
//    val c  =  2  *  3.1416
//    val d  =  Alleles.length
//    var sum  =  Ackley  (Alleles  ,  c  )
//    val Result : Double  =  -a  *  math.exp( -b * math.sqrt (  sum._1 / d )  )  -  math.exp( sum._2 / d )  +  a  +  math.exp(1)
//    Result
//  }
//  
//  def Ackley  (  Alleles  :  Array[Gene]  ,  c  :  Double    )  :  (  Double  ,  Double  )   =  {
//    if (  Alleles.length  ==  1  )  {
//      var sum1  :  Double  =  Math.pow  (  Alleles(0).Allele  , 2)
//      var sum2  :  Double  =  Math.cos  (  c  *    Alleles(0).Allele  )
//      (  sum1  ,  sum2  )
//    }
//    else  {
//      var sum1  =  (  0.0  ,  0.0  )
//      var sum2  =  (  0.0  ,  0.0  )
//      sum1  =  Ackley  (  Alleles.slice(  0  ,  Alleles.length/2  )  ,  c  )
//      sum2  =  Ackley  (  Alleles.slice(  Alleles.length/2  ,  Alleles.length  )  ,  c  )
//      (  sum1._1  +  sum2._1  ,  sum1._2  +  sum2._2    )
//    }
//  }
  
  
  //  Griewank    -    Dimensions:d    -    GlobalMinima : f( 0, ... , 0 ) = 0
  def GriewankFunc  (  Alleles:  Array[Gene]  ,  accumulator:  Double  )  :  Double =  {
    val  Result  =  Griewank  (  Alleles.toList  ,  (  accumulator  ,  accumulator  )  ,  1  )
    Result._1  -  Result._2  +  1
  }
  
  @tailrec
  def Griewank  (  Alleles:List[Gene]  ,  accumulator:  (Double,  Double)  ,  baseIndex:Int  =  1  )  :  (  Double  ,  Double  ) =  {
    Alleles  match  {
      case  Nil  =>  accumulator
      case x::xs  =>  Griewank  (  xs  ,  
          (  accumulator._1  +  Math.pow(  x.Allele  ,  2  )  /  4000  ,  accumulator._2  +  Math.cos(  x.Allele  /  Math.sqrt(  baseIndex  )  )  )  ,
          (  baseIndex  +  1  )  )
    }
    
  }
//  def GriewankFunc  (  Alleles:Array[Gene]  )  :  Double =  {
//    val  Result  =  Griewank  (  Alleles  )
//    Result._1  -  Result._2  +  1
//  }
//  
//  def Griewank  (  Alleles:Array[Gene]  ,  baseIndex:Int  =  1  )  :  (  Double  ,  Double  ) =  {
//    if  (  Alleles.length  ==  1  )  {
//      (  Math.pow(  Alleles(0).Allele  ,  2  )  /  4000  ,  Math.cos(  Alleles(0).Allele  /  Math.sqrt(  baseIndex  )  )  )
//    }
//    else  {
//      val  R1  =  Griewank  (  Alleles.slice(  0  ,  Alleles.length/2)  ,  baseIndex  )
//      val  R2  =  Griewank  (  Alleles.slice(  Alleles.length/2  ,  Alleles.length  )  ,  baseIndex  +  Alleles.length/2  )
//      (  R1._1  +  R2._1  ,  R1._2  +  R2._2  )
//    }
//  }
  
  
  
  //  Rastrigin    -    Dimensions:d    -    GlobalMinima : f( 0, ... , 0 ) = 0
  def RastriginFunc  (  Alleles:Array[Gene]  ,  accumulator:  Double  )  :  Double =  {
    (  10  *  Alleles.length.toDouble  )  +  Rastrigin  (  Alleles.toList  ,  accumulator  )
  }
  @tailrec
  def Rastrigin  (  Alleles:List[Gene]  ,  accumulator:  Double  )  :  Double =  {
    Alleles  match  {
      case  Nil  =>  accumulator
      case x::xs  =>  Rastrigin  (  xs  ,  accumulator  +  Math.pow(  x.Allele  ,  2  )  -  10  *  Math.cos  (  2  *  3.1416  *  x.Allele  )  )
    }    
  }
//  def RastriginFunc  (  Alleles:Array[Gene]  )  :  Double =  {
//    (  10  *  Alleles.length.toDouble  )  +  Rastrigin  (  Alleles  )
//  }
//  
//  def Rastrigin  (  Alleles:Array[Gene]  )  :  Double =  {
//    if  (  Alleles.length  ==  1  )  {
//      Math.pow(  Alleles(0).Allele  ,  2  )  -  10  *  Math.cos  (  2  *  3.1416  *  Alleles(0).Allele  )
//    }
//    else  {
//      Rastrigin  (  Alleles.slice  (  0  ,  Alleles.length/2  )  )    +  Rastrigin  (  Alleles.slice  (  Alleles.length/2  ,  Alleles.length  )  )
//    }
//  }
//  
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
  //  @tailrec
//  def SphereFunc  (  Alleles:Array[Gene]  )  :  Double =  {
//    
//   var fitness:Double  =  0.0
//   if  (  Alleles.length  ==  1  )
//     fitness  =  Math.pow(Alleles(0).Allele, 2)
//   
//   else
//     fitness  =   SphereFunc  (   Alleles.slice(0, Alleles.length/2)  )  +  SphereFunc  (   Alleles.slice(Alleles.length/2,  Alleles.length  )  )
//   fitness
//  }
  
  @tailrec
  def SphereFunc  (  Alleles:Array[Gene]  ,  accumulator:Double  )  :  Double =  Alleles.toList match  {
     case  Nil  =>  accumulator
     case  x  ::  xs  =>    SphereFunc  (  xs.toArray  ,  accumulator  +  Math.pow  (  x.Allele  , 2  )  ) 
   }
    
  //  Sum Of Different Powers    -    Dimensions:d    -    GlobalMinima : f( 0, ... , 0 ) = 0

   def SumOfDiffPowersFunc  (  Alleles:Array[Gene]  ,  accumulator:Double  )  :  Double  =  {
     SumOfDiffPowers  (  Alleles.toList  ,  0  ,  1  )
   }
  
  @tailrec
  def SumOfDiffPowers  (  Alleles:List[Gene]  ,  accumulator:Double  ,  baseIndex:Int  )  :  Double =  {
    Alleles match  {
     case  Nil  =>  accumulator
     case  x  ::  xs  =>    SumOfDiffPowers  (  xs  ,  accumulator  +  Math.pow  (  x.Allele  , baseIndex+1  )  ,  baseIndex+1  )
    }
   }
  
//  def SumOfDiffPowersFunc  (  Alleles:Array[Gene]  )  :  Double =  {
//    var baseIndex:Int  =  1
//   var fitness:Double  =  0.0
//   if  (  Alleles.length  ==  1  )
//     fitness  =  Math.pow  (  Alleles(0).Allele  ,  baseIndex  +  1  )
//   else
//     fitness  =   SumOfDiffPowersFunc  (   Alleles.slice(0, Alleles.length/2)  ,  baseIndex  )  +  SumOfDiffPowersFunc  (   Alleles.slice(Alleles.length/2,  Alleles.length  )    ,  baseIndex + Alleles.length/2  )
//   fitness
//  }
  
//  def SumOfDiffPowersFunc  (  Alleles:Array[Gene]  ,  baseIndex:Int  )  :  Double =  {
//   var fitness:Double  =  0.0
//   if  (  Alleles.length  ==  1  )
//     fitness  =  Math.pow  (  Alleles(0).Allele  ,  baseIndex  +  1  )
//   else
//     fitness  =   SumOfDiffPowersFunc  (   Alleles.slice(0, Alleles.length/2)  ,  baseIndex  )  +  SumOfDiffPowersFunc  (   Alleles.slice(Alleles.length/2,  Alleles.length  )    ,  baseIndex + Alleles.length/2  )
//   fitness
//  }
  
  
  
  //  Sum Squares    -    Dimensions:d    -    GlobalMinima : f( 0, ... , 0 ) = 0
  def SumSquaresFunc  (  Alleles:Array[Gene]  ,  accumulator:Double  )  :  Double  =  {
     SumOfDiffPowers  (  Alleles.toList  ,  0  ,  1  )
   }
  
  @tailrec
  def SumSquares (  Alleles:List[Gene]  ,  accumulator:Double  ,  baseIndex:Int  )  :  Double =  {
    Alleles match  {
     case  Nil  =>  accumulator
     case  x  ::  xs  =>    SumSquares  (  xs  ,  accumulator  +  (  baseIndex  *  Math.pow  (  x.Allele  , 2  )  )  ,  baseIndex+1  )
    }
   }
//  def SumSquaresFunc  (  Alleles:Array[Gene]  )  :  Double =  {
//    var baseIndex:Int  =  1
//   var fitness:Double  =  0.0
//   if  (  Alleles.length  ==  1  )
//     fitness  =  baseIndex  *  Math.pow  (  Alleles(0).Allele  ,  2  )
//   else
//     fitness  =   SumOfDiffPowersFunc  (   Alleles.slice(0, Alleles.length/2)  ,  baseIndex  )  +  SumOfDiffPowersFunc  (   Alleles.slice(Alleles.length/2,  Alleles.length  )    ,  baseIndex + Alleles.length/2  )
//   fitness
//  }
//  
//  def SumSquaresFunc  (  Alleles:Array[Gene]  ,  baseIndex:Int  )  :  Double =  {
//   var fitness:Double  =  0.0
//   if  (  Alleles.length  ==  1  )
//     fitness  =  baseIndex  *  Math.pow  (  Alleles(0).Allele  ,  2  )
//   else
//     fitness  =   SumOfDiffPowersFunc  (   Alleles.slice(0, Alleles.length/2)  ,  baseIndex  )  +  SumOfDiffPowersFunc  (   Alleles.slice(Alleles.length/2,  Alleles.length  )    ,  baseIndex + Alleles.length/2  )
//   fitness
//  }
//    
  /*
   * Plate Shaped Functions
   */
  
  
  //  Zakharov    -    Dimensions:d    -    GlobalMinima : f( 0, ... , 0 ) = 0
  def ZakharovFunc  (  Alleles:Array[Gene]  ,  accumulator:  Double  )  :  Double =  {
    val  sum  =  Zakharov  (  Alleles.toList  ,  (  0  ,  0)  ,  1  )
    sum._1  +  Math.pow  (  sum._2  ,  2  )  +  Math.pow  (  sum._2  ,  4  )
  }
  
  @tailrec
  def Zakharov  (  Alleles  :  List[Gene],  accumulator:  (Double,  Double)  ,  baseIndex:Int  =  1  )  :  (  Double  ,  Double  ) =  {
    Alleles  match  {
      case  Nil  =>  accumulator
      case x::xs  =>  Zakharov  (  xs  ,  
          (  accumulator._1  +  Math.pow(  x.Allele  ,  2  )  ,  accumulator._2  +  (  0.5  *  baseIndex  *  x.Allele  )  )  ,
          (  baseIndex  +  1  )  )
    }
  }
  
//  def ZakharovFunc  (  Alleles:Array[Gene]  )  :  Double =  {
//    val  sum  =  Zakharov  (  Alleles  ,  1  )
//    sum._1  +  Math.pow  (  sum._2  ,  2  )  +  Math.pow  (  sum._2  ,  2  )
//    0.0
//  }
//  
//  def Zakharov  (  Alleles  :  Array[Gene]  ,  baseIndex:Int  )  :  (  Double  ,  Double  )  =  {
//    var  sum1  :  Double  =  0
//    var  sum2  :  Double  =  0
//    if  (  Alleles.length  ==  1  )  {
//      sum1  =  Math.pow  (  Alleles(0).Allele  ,  2  )
//      sum2  =  0.5  *  baseIndex  *  Alleles(0).Allele
//    }    else  {
//      var temp1  =  Zakharov(  Alleles.slice(  0  , Alleles.length/2  )  ,  baseIndex  )
//      var temp2  =  Zakharov(  Alleles.slice(  0  , Alleles.length/2  )  ,  baseIndex  )
//      sum1  =  temp1._1  +  temp2._1
//      sum2  =  temp1._2  +  temp2._2
//    }
//    (  sum1  ,  sum2  )
//  }
  
  //  Styblinski    -    Dimensions:d    -    GlobalMinima : f( 0, ... , 0 ) = 0
   def StyblinskiFunc  (  Alleles:Array[Gene]  ,  accumulator:Double  )  :  Double  =  {
     0.5  *  Styblinski  (  Alleles.toList  ,  0  )
   }
  
  @tailrec
  def Styblinski (  Alleles:List[Gene]  ,  accumulator:Double )  :  Double =  {
    Alleles match  {
     case  Nil  =>  accumulator
     case  x  ::  xs  =>    Styblinski  (  xs  ,  accumulator  +  Math.pow  (  x.Allele  ,  4  )  -  16  *  Math.pow  (  x.Allele  ,  2  )  +  5  *  x.Allele  )
    }
   }
  
    //  Schwefel    -    Dimensions:d    -    GlobalMinima : f( 0, ... , 0 ) = 0
  def SchwefelFunc  (  Alleles:Array[Gene]  ,  accumulator:Double  )  :  Double  =  {
     418.9829  *  Alleles.length  -  Schwefel  (  Alleles.toList  ,  0  )
   }
  
  @tailrec
  def Schwefel (  Alleles:List[Gene]  ,  accumulator:Double )  :  Double =  {
    Alleles match  {
     case  Nil  =>  accumulator
     case  x  ::  xs  =>    Schwefel  (  xs  ,  accumulator  +  (  x.Allele  *  Math.sin(  Math.pow  (  x.Allele.abs  ,  0.5  )  )  )  )
    }
   }
  
  
  
    //  Rosen    -    Dimensions:d    -    GlobalMinima : f( 0, ... , 0 ) = 0
  def RosenFunc  (  Alleles:Array[Gene]  ,  accumulator:Double  )  :  Double  =  {
     Rosen  (  Alleles.toList  ,  0  ,  1  )
   }
  
  @tailrec
  def Rosen (  Alleles:List[Gene]  ,  accumulator:Double  ,  baseIndex:Int  )  :  Double =  {
    Alleles match  {
     case  x  ::  Nil  =>  accumulator
     case  x  ::  y  ::  xs  =>    Rosen  (  y::xs  ,  accumulator  +  (  100  *  Math.pow(y.Allele - Math.pow(x.Allele,2),2)  +  Math.pow(x.Allele-1,2) )    ,  baseIndex+1  )
    }
   }
  
  
  //  Dixon    -    Dimensions:d    -    GlobalMinima : f( 0, ... , 0 ) = 0
  def DixonFunc  (  Alleles:Array[Gene]  ,  accumulator:Double  )  :  Double  =  {
    Alleles.toList match {
      case x::xs  =>  Math.pow(  x.Allele - 1  ,  2  )  +  Dixon  (  xs  ,  0  ,  2  ,  x.Allele  )
    }
   }
  
  @tailrec
  def Dixon (  Alleles:List[Gene]  ,  accumulator:Double  ,  baseIndex:Int  ,  prev:Double    )  :  Double =  {
    Alleles match  {
     case  Nil  =>  accumulator
     case  x  ::   xs  =>    Dixon  (  xs  ,  accumulator  +  baseIndex * Math.pow( 2*x.Allele*x.Allele - prev ,2 )     ,  baseIndex+1  ,  x.Allele  )
    }
   }

    //  Rotated Hyper Elipsoid    -    Dimensions:d    -    GlobalMinima : f( 0, ... , 0 ) = 0
  def RotatedFunc  (  Alleles:Array[Gene]  ,  accumulator:Double  )  :  Double  =  {
    RotatedHE_Outer  (  Alleles.toList  ,  accumulator)
   }
  
  
  def RotatedHE_Outer (  Alleles:List[Gene]  ,  accumulator:Double  )  :  Double =  {
    var res  =  0.0  
    for (  i  <- 0 to Alleles.length - 1  )
      res  +=  RotatedHE_Inner  (  Alleles.slice(0 , i+1)  ,  0  )
      res
   }
  
  @tailrec
  def RotatedHE_Inner (  Alleles:List[Gene]  ,  accumulator:Double  )  :  Double =  {
    Alleles match  {
     case  Nil  =>  accumulator
     case  x  ::   xs  =>    RotatedHE_Inner  (  xs  ,  accumulator  +  Math.pow( x.Allele,2 )  )
    }
   }
  
  
}
  