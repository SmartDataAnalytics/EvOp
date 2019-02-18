package dsgd
import org.apache.spark.mllib.linalg.{Vector, Vectors}

class pointInitializer (  P  :  Int  , CL:  Int  , valBound:  Array[Int]  )  {
  
  
  var Populaion = P
  var Dimensions  =  CL
  var pointList  :  List[Vector]  =  List()
  
  val r = scala.util.Random
  
  //  Sphere    -    Dimensions : d    -    GlobalMinima : f( 0, ... , 0 ) = 0
  def SphereFunc  (  Alleles:Array[Double]  )  :  Double =  {
    
   var fitness:Double  =  0.0
   if  (  Alleles.length  ==  1  )
     fitness  =  Math.pow(Alleles(0), 2)
   
   else
     fitness  =   SphereFunc  (   Alleles.slice(0, Alleles.length/2)  )  +  SphereFunc  (   Alleles.slice(Alleles.length/2,  Alleles.length  )  )
   fitness
  }
  
  
  for(i <-  Populaion to 1 by -1)  {
    var temp:List[Double]  =  List()
    for (k <- 1 to Dimensions){
      var toadd:Double  =  valBound(0) + (  (r.nextInt( 100 ).toDouble/100).toDouble  *  (valBound(1)-valBound(0) ).toDouble  ).toDouble
      var frac  =  toadd - toadd.toInt
      frac  =  frac*100
      frac  = frac.toInt
      frac = frac/100
      toadd = toadd.toInt+frac
      temp  =  toadd  ::  temp
      }
    var fitness  =  SphereFunc(  temp.toArray  )
    temp  =  temp  :::  List  (  fitness  )
    val tempVec  =  Vectors.dense  (  temp.toArray  )                  //  (  new Array[Double](3)  )
    pointList = tempVec :: pointList
    //println(pointList)
    }
}