package dsgd

import org.apache.spark.mllib.optimization
import org.apache.spark.mllib.optimization.GradientDescent
import org.apache.spark.mllib.optimization.Gradient
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}

//import cern.colt.matrix.linalg.Blas
import org.apache.spark.mllib.linalg.BLAS
//import org.apache.spark.ml.linalg.BLAS

import com.github.fommil.netlib.BLAS


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
    
    //var  Arr  =  Array.fill  (  data.size  )  (  2.0  )
    var Arr  =  data.copy.toArray
    println("Arr")
    Arr.foreach(println)
    Arr  .  map  (  x  =>  x  *  2  )
    var  gradient  =  Vectors.dense  (  Arr  )
    
    println("GRADIENT IS")
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
    
    var B  =  BLAS.getInstance
    
//    println("Label is "+label)
//    println("Data is ")
//    println(  data.toArray.mkString("  &  ")  )
    var  Arr  =  Array.fill  (  data.size  )  (  2.0  )
    var cumGradientArr  =  Vectors.dense(  Arr  )
    val diff = dot(data, weights) - label
    //println("	DIFF IS	"+diff * diff / 2.0)
    var data1  =  data.toArray.map(x=>x*2)
    var data2  =  Vectors.dense(data1)
    //B.daxpy(data2.size, 1, data2.toDense.values, 1, cumGradient.toDense.values, 1)
//    println("cumGradient is ")
//    println(  cumGradient.toArray.mkString(" ,")  )
//    println("---------------")
    //axpy(diff, data, cumGradientArr)
    //  B.
    //B.axpy(diff, data, cumGradient)
    //println(  weights.toArray.mkString(" # ")  )
    diff * diff / 2.0
  
  }
  
}