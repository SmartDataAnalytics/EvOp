package dsgd

import org.apache.spark.mllib.optimization
import org.apache.spark.mllib.optimization.GradientDescent
import org.apache.spark.mllib.optimization.Gradient
import org.apache.spark.mllib.linalg.{Vector, Vectors}

class SphereGradient extends Gradient  {
  def dot  (  data  :  Vector  ,  weights  :  Vector  )  :  Double  =  {
    var d  =  data.toArray
    var w  =  weights.toArray
    //println("	Data Size is	"  +  d.length)
    //println("	Weights Size is	"  +  w.length)
    var  dotProduct  :  Double  =  0
    for  (  i  <-  0  to d.length  -  1  )  {
      dotProduct  +=  d  (  i  )  *  w  (  i  )
    }
    
    dotProduct
  }
  
  override def compute(data: Vector, label: Double, weights: Vector): (Vector, Double) = {
    
    val  diff  =  dot  (  data  ,  weights  )  //  -  label
    
    val  loss  =  diff  *  diff  /  2.0
    //val gradient = data.copy
    
    var  Arr  =  Array.fill  (  data.size  )  (  2.0  )
    val  gradient  =  Vectors.dense  (  Arr  )
    //println("GRADIENT IS")
    //gradient.toArray.foreach(println)
    //println("LOSS IS	"+loss)
    
    //scal(diff, gradient)
    (  gradient  ,  loss  )
  }

  override def compute(
      data: Vector,
      label: Double,
      weights: Vector,
      cumGradient: Vector): Double = {
    
    val diff = dot(data, weights) - label
    //println("	DIFF IS	"+diff)
    //  ppkaxpy(diff, data, cumGradient)
    diff * diff / 2.0
  }
  
}