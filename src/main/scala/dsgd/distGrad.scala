package dsgd
/*
 * 
 * Using ML Distributed Stochastic Gradient Class for parameter optimization of N dimensions.
 */
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.optimization
import org.apache.spark.mllib.optimization.GradientDescent
import org.apache.spark.mllib.optimization.Gradient
import org.apache.spark.mllib.optimization.Updater
import org.evop.spark.newga.RandomDoubleInitializer
import org.evop.spark.newga.TestFunctions
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.optimization.LogisticGradient
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.util.Random

object distGrad {
  def main(  args:  Array[String]   )  {
    
      val  d  =  100    //  args(0).toInt
      val  p  =  100    //  args(1).toInt
      //var  o  =  args  (  7  )
      
      var  ri  =  new pointInitializer  (  p  ,  d  ,  TestFunctions.SphereBound  )
      
    //Create Spark Context
    val conf = new SparkConf().setAppName("StochasticGD").setMaster("local[*]")
    //val conf = new SparkConf().setAppName("Parallel GA").setMaster(configs)
    val sc  =  new SparkContext  (  conf  )
    
    //var rawdata =  List(  (  0.0  ,  Vector( 1,2,3 )  )  ,  (  0.1  ,  Vector( 7,2,3 )  )  ) 
    var rawData  =  ri.pointList.map  (  a  =>  (  0.0  ,  a  )  )
    var data  =  sc.parallelize  (  rawData  )  
    
    //  var gradient  :  Gradient  =  new Gradient()

    var m=3
    var n=3
    val points = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => (1.0, Vectors.dense(Array.fill(n)(random.nextDouble()))))
    }.cache()
    
    
    val (weights, loss) =  GradientDescent.runMiniBatchSGD  (  
        points, 
        new SphereGradient  ,
        new SquaredL2Updater  ,
        0.1  ,                      //  StepSize
        10000  ,                    //  Iterations 
        0.0  ,                    //  Set Regularization Parameter 
        1.0  ,                   //  MiniBatchFraction 
        Vectors.dense(  Array.fill  [  Double  ]  (  d  )  (  scala.util.Random.nextInt(100).toDouble  /  100.00  )  )  ,
        (1.0/d.toDouble).toDouble      // COnvergenceTol
        )
      weights.toArray.foreach  (  println  )
        //println("w:"  + weights(0))
      println("loss:" + loss(0))
      sc.stop()
      
      
      //  runMiniBatchSGD(data, gradient, updater, stepSize, numIterations, regParam, miniBatchFraction, initialWeights)
                    
  }
  
}