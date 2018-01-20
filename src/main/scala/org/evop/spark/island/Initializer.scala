package org.evop.spark.island

import scala.io.Source

//import scala.collection.parallel.ParIterableLike.Foreach


abstract class Initializer {
  
  //Initialize population size, Default is zero
  var Populaion:Int = 0
  var Dimensions:Int = 0
  
  //Type of Initializer, Random or File
  var InitType:Int=0
  
  //Chromosome Length
  var chromoLength=0;
  var chromoList:List[(Double, Chromosome)]=List()
  
  
}
class RandomInitializer(  P  :  Int  , CL:  Int  , valBound:  Array[Int]  , f  :  Array[Gene]  =>  Double  ) extends Initializer {
  Populaion = P
  Dimensions  =  CL
  InitType=1
  val r = scala.util.Random
  valBound.length match  {
  
    case 2  =>      for(i <-  Populaion to 1 by -1)  {
                    val temp  =  List.fill(CL)( valBound(0) + r.nextInt( valBound(1) - valBound(0) +1  ) )  // Initialize the chromosomes with random values within specified range
                    val randomAlleles  =  temp.map(  x => new Gene(x)  ).toArray
                    val newchromo  =  new Chromosome(i, randomAlleles,  f  )  // Create a chromosome from Randomly created Alleles
                    chromoList = (i.asInstanceOf[Double], newchromo) :: chromoList
                    println(newchromo)
                    
                    }
  
    case _  =>      for(i <-  Populaion to 1 by -1)  {
                    var temp:List[Int]  =  List.range(  1  ,  CL+1  )
                    temp.foreach(println)
                        temp  =  temp.map(  i  =>  (    valBound(i-1) + r.nextInt( valBound(CL+i-1)  -  valBound(i-1) +1 ) )  )
                    //var temp  =  List.fill(CL)( minval + r.nextInt( maxval-minval ) )  // Initialize the chromosomes with random values within specified range
                    val randomAlleles  =  temp.map(  x => new Gene(x)  ).toArray
                    val newchromo  =  new Chromosome(i, randomAlleles,  f  )  // Create a chromosome from Randomly created Alleles
                    chromoList = (i.asInstanceOf[Double], newchromo) :: chromoList
                    println(newchromo)
                    }
  }
}
class RandomDoubleInitializer(  P  :  Int  , CL:  Int  , valBound:  Array[Int]  , f  :  Array[Gene]  =>  Double  ) extends Initializer {
  Populaion = P
  Dimensions  =  CL
  InitType=2
  
  val r = scala.util.Random
  valBound.length match  {
  
    case 2  =>      for(i <-  Populaion to 1 by -1)  {
                    var temp:List[Double]  =  List()  
                    
                    for (k <- 1 to Dimensions){
                      //println((r.nextInt( 100 ).toDouble/100).toDouble)
                      var toadd:Double  =  valBound(0) + (  (r.nextInt( 100 ).toDouble/100).toDouble  *  (valBound(1)-valBound(0) ).toDouble  ).toDouble 
                      var frac  =  toadd - toadd.toInt
                      frac  =  frac*100
                      frac  = frac.toInt
                      frac = frac/100
                      toadd = toadd.toInt+frac
                      temp  =  toadd  ::  temp
                    }
                    
                    val randomAlleles  =  temp.map(  x => new Gene(x)  ).toArray
                    val newchromo  =  new Chromosome(i, randomAlleles,  f  )  // Create a chromosome from Randomly created Alleles
                    chromoList = (i.asInstanceOf[Double], newchromo) :: chromoList
                    
                    println(newchromo)
                    }
  
    case _  =>      for(i <-  Populaion to 1 by -1)  {
                    var temp:List[Int]  =  List.range(  1  ,  CL+1  )
                    temp.foreach(println)
                        temp  =  temp.map(  i  =>  (    valBound(i-1) + (r.nextInt( 100 )/100)  *  ( valBound(CL+i-1)  -  valBound(i-1) +1 )     )  )
                    //var temp  =  List.fill(CL)( minval + r.nextInt( maxval-minval ) )  // Initialize the chromosomes with random values within specified range
                    val randomAlleles  =  temp.map(  x => new Gene(x)  ).toArray
                    val newchromo  =  new Chromosome(i, randomAlleles,  f  )  // Create a chromosome from Randomly created Alleles
                    chromoList = (i.asInstanceOf[Double], newchromo) :: chromoList
                    println(newchromo)
                    }
  }
}
/*
class RandomInitializer(P:Int, CL:Int, minval:Int=0, maxval:Int=1000, f:Array[Gene]=>Double) extends Initializer {
  Populaion = P
  InitType=1
 
  val r = scala.util.Random
    println("I am in  "+r)
  for(i <-  Populaion to 1 by -1)  {
      val temp  =  List.fill(CL)( minval + r.nextInt( maxval-minval ) )  // Initialize the chromosomes with random values within specified range
      val randomAlleles  =  temp.map(  x => new Gene(x)  ).toArray
      val newchromo  =  new Chromosome(i, randomAlleles,  f  )  // Create a chromosome from Randomly created Alleles
      chromoList = (i.asInstanceOf[Double], newchromo) :: chromoList
      println(newchromo)
  }
}*/
//class RandomInitializer(P:Int, CL:Int, valBounds:Array[Int], f:Array[Gene]=>Double) extends Initializer 
  

class FileInitializer(P:Int, CL:Int, fname:String) extends Initializer {
  Populaion = P
  InitType = 2
  
  val listOfLines = Source.fromFile(fname).getLines.toList
}