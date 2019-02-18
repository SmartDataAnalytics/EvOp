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
import org.apache.spark.mllib.optimization.{L1Updater, SimpleUpdater, SquaredL2Updater, Updater}
import org.apache.spark.mllib.optimization.LogisticGradient
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.util.Random
import scala.io.Source
import java.io._
import java.util.Calendar
import org.apache.spark.storage.StorageLevel

object distGrad {
  def main(  args:  Array[String]   )  {
    
      val  d  =  args(0).toInt
      val  p  =  args(1).toInt
      val  g  =  args(2).toInt
      var  m  =  args(3).toInt  //  Partitions
      var  step  =  args(4).toDouble  //  StepSize
      var  reg  =  args(5).toDouble  //  Regularization Parameter
      var  minBatch  =  args(6).toDouble  //  Mini Batch Fraction
      var configs  =  	args(7)  //  Cluster Master IP   "local[*]"
      var func  =  "Sphere"
      var min  =  -6
      var max  =  6
      //var  o  =  args  (  7  )
      
      var  ri  =  new pointInitializer  (  p  ,  d  ,  TestFunctions.SphereBound  )
      
    //Create Spark Context
    val conf = new SparkConf().setAppName("StochasticGD").setMaster(configs)
    //  val conf = new SparkConf().setAppName("StochasticGD").setMaster("local[*]")
    
    val sc  =  new SparkContext  (  conf  )
    
    //var rawdata =  List(  (  0.0  ,  Vector( 1,2,3 )  )  ,  (  0.1  ,  Vector( 7,2,3 )  )  ) 
    var rawData  =  ri.pointList.map  (  a  =>  (  0.0  ,  a  )  )
    var data  =  sc.parallelize  (  rawData  )  
    
    //  var gradient  :  Gradient  =  new Gradient()
    
      def SphereFunc  (  Alleles:Array[Double]  )  :  Double =  {
        var fitness:Double  =  0.0
        if  (  Alleles.length  ==  1  )
          fitness  =  Math.pow(Alleles(0), 2)
        else
          fitness  =   SphereFunc  (   Alleles.slice(0, Alleles.length/2)  )  +  SphereFunc  (   Alleles.slice(Alleles.length/2,  Alleles.length  )  )
        fitness
  }

    
    var n=d  //3
    val points = sc.parallelize(0 until p, m).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(  idx  )
      var tempData  =  iter.map(i => Array.fill(d)(  random.nextDouble()*(max-min)+min  ))
      var Data  =  tempData.map(i => (  SphereFunc  (  i  )  , Vectors.dense(  i  )))
      //  iter.map(i => (1.0, Vectors.dense(Array.fill(d)(  random.nextDouble()*(max-min)+min  ))))
      Data.toIterator
    }.persist(StorageLevel.MEMORY_AND_DISK)
    
    val random = new Random()
    var initWeights  =  Vectors.dense(  Array.fill  [  Double  ]  (  d  )  (  random.nextDouble()*(max-min)+min  )  )
    val t0  =  System.nanoTime()
     
    
    val (weights, loss) =  GradientDescent.runMiniBatchSGD  (  
        points, 
        new SphereGradient  ,
        new SimpleUpdater  ,    //  //new SquaredL2Updater()  ,
        step  ,                      //  StepSize 0.1
        g  ,                    //  Iterations 
        reg  ,                    //  Set Regularization Parameter 0.1 
        minBatch  ,                   //  MiniBatchFraction 1
        initWeights  ,
        scala.math.pow  (1e1  ,   -(scala.math.log10(d)))      // ConvergenceTol
        )
     val t1  =  System.nanoTime()
      //weights.toArray.foreach  (  println  )
        //println("w:"  + weights(0))
      println("Initial loss:" + loss(0))
      println(  "Final Loss: @"  +  loss.length  +" iteration is "+  loss  (  loss.length-1 )  )
      //loss.foreach(println)
      //println("Initial Weights")
      //initWeights.toArray.foreach(println)
      //println("Final Weights")
      //weights.toArray.foreach(println)
      sc.stop()
      
      
      //  runMiniBatchSGD(data, gradient, updater, stepSize, numIterations, regParam, miniBatchFraction, initialWeights)
      var fOutput   = "\n"+Calendar.getInstance().getTime+"\n"+"Dimensions= "+d+",	Func= "+func+",	Population= "+p+",	Partis= "  +  m  +
      ", Fitness= "+loss  (  loss.length-1 )+", Iterations= "  +  loss.length  +", StepSize= "  +  step  +", RegParam= "  +  reg  +", MiniBatchFrac= "  +  minBatch  +
      ",	Time= "+  (  (t1-t0).toDouble  /  1000000000  ).toDouble  +"\n"
    //val outRDD  =  sc.parallelize(fOutput)
    //outRDD.saveAsTextFile("hdfs://172.18.160.17:54310/FahadMaqbool/DatasetX/"+gens+"-"+init.Dimensions+"-"+bdCastStrategy+"-"+bdCastSize+"-"+scala.util.Random.nextInt(1000)+".txt")
  val fw = new FileWriter("/data/home/FahadMaqbool/sgd/newResults.txt", true)
  fw.write(fOutput)
  fw.close()
  
  fOutput  +=  "\n\n\n"
  val fw2 = new FileWriter("/data/home/FahadMaqbool/sgd/newDetails.txt", true)
  fw2.write(fOutput)
  fw2.write(  loss.mkString(  "\n"  )  )
  fw2.write("------------------------------------------------------------------------")
  fw2.close()
  }
}