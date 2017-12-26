package org.evop.spark.ga

import org.apache.spark._
import org.apache.spark.rdd.RDD
import java.io.File
//import java.io.PrintWriter
//import scala.io.Source
//import java.io._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import scala.tools.ant.sabbus.Break
import scala.util.control.Breaks._
import scala.io.Source
import java.io.FileSystem

class GA(  f:  Array[Gene]  =>  Double  , init:Initializer  ,
    SelectorType:String  ,  MutatorType:String  ,  Replacement:String  ,  crossType:String  ,direction:String  ,
    CrossOverProb:Double  ,  MutationProb:Double  ,
    stopper_Type:String  ,  Stopper_Threshold:Long  ,  PartitionsCount:Int    ,  GenGap:Int,  
    bdCastStrategy:String  ,  bdCastSize:Int  )   extends  Serializable  {
  
  
  //Create Spark Configurations
  val conf = new SparkConf().setAppName("Parallel GA")//.setMaster("spark://172.18.160.16:3077") 
    .setMaster("local[*]")
// local mode
//  .setMaster("local[*]")
//  .set("spark.executor.memory","7g")
//  .set("spark.driver.memory","3g")
//  .set("spark.driver.maxResultSize","0")
    
  //Create Spark Context
  val sc = TestFunctions.sc
  //val ssc  =  new StreamingContext(conf, Seconds(1))
  
  //val a  =  sc.textFile("hdfs://172.18.160.17:54310/FahadMaqbool")
  
  //val accumNFC = sc.longAccumulator("NFC Accumulator")
  
  
  //  Parameters PreProcessing to appropriate objects
  var chromoList:List[(Double, Chromosome)]=List()
  init.InitType match{
      case 1 => {  chromoList  =  init.asInstanceOf[RandomInitializer].chromoList  }  //IntRDDCreation()
      case 2 => {  chromoList  =  init.asInstanceOf[RandomDoubleInitializer].chromoList  }  //FileRDDCreation()
  }
  
  
  
    
  val theSelector  =  SelectorType match{
    case "RANDOM"    =>  new RandomSelector(  sc  ,  MutatorType  ,  Replacement  ,  crossType,  direction  ,  CrossOverProb  ,  MutationProb  ,  bdCastStrategy  ,  bdCastSize  )
    case "ROULETTE"    =>  new RouletteSelector(  sc  ,  MutatorType  ,  Replacement  ,  crossType,  direction  ,  CrossOverProb  ,  MutationProb  ,  bdCastStrategy  ,  bdCastSize  )
  
  }
  //  End Parameters PreProcessing
  var optRecord  :  List[(  Int,  Long ,  Double  )]  =  List()
  //Intialize Generation Number to 1
  var gens:Int  =  1
  
  //RDD Creation from List
  val chromoRDD = sc.parallelize(chromoList)
  
  // Create Range Partitioner
  val TunedPartitioner  =  new RangePartitioner  (  PartitionsCount  ,  chromoRDD  )
  
  // Partition RDD according to Range partitioner
  val Partitioned  =  chromoRDD.partitionBy  (  TunedPartitioner  ).persist(  )
  Partitioned.collect()
  
  //Define the Gap after which Best Solution of a Partition will replace the weak solution of other partition
  var gap:Int  =  GenGap
  
  
  var condition  =  true
  var nextPartitions  =  Partitioned
    
  //println("Starting While")
  var BestofBest  =  (0.0  ,  new Chromosome(0.0,Array(new Gene(0))))
  val  theStopper:Stopper  =  new Stopper(stopper_Type,Stopper_Threshold)
  breakable{
  while(  !(  theStopper.stop(gens,0)  )  &&  condition  ==  true  )  {
    
    
//    nextPartitions.unpersist()
//    nextPartitions = newPartitions
//    nextPartitions.persist
//    newPartitions.unpersist()
    //nextPartitions.map(x=>x._2).foreach(println)
    //println("Before Selection")
    
    if(  gens  %  gap  ==  1  )  {
      nextPartitions   =  theSelector.selection(nextPartitions)
      BestofBest  =  theSelector.selectBest(nextPartitions)
      println("The Best Solution After Generation "+gens+" is "+BestofBest )
      optRecord  =  (  gens  ,  TestFunctions.NFC.value.toLong  ,  BestofBest._2.fitness  )  ::  optRecord
      if (BestofBest._2.fitness<=0){
        theStopper.forceStop()
        gens  -=  1
        condition  =  false
      }
      gens  +=  1
    }
    else  if(  gens  %  gap  ==  0)  {
      nextPartitions   =  theSelector.selection(nextPartitions)
      nextPartitions  =  theSelector.eliminateWeak(  nextPartitions  ,  PartitionsCount  )
      nextPartitions.collect()
      
      gens  +=  1
    }
    else if (gap>2){
      nextPartitions   =  theSelector.selection(nextPartitions,gap-2)
      gens  +=  gap-2
            
    }
    var temp:Int  =  (chromoList.length*CrossOverProb.toInt/100)  +  (chromoList.length*MutationProb.toInt/100)
    
  }
  }
  
  
  val fOutput  = "\n\n\nGens= "+ gens+",	Dimensions= "+init.Dimensions+",	Selector= "+SelectorType+",	Mutator= "+MutatorType+",	Replace= "+Replacement+",	Dir= "+direction+",	xovrP= "+
    CrossOverProb+",	MutP= "+MutationProb+",	Stoper= "+stopper_Type+",	xovr= "+crossType+",	Partis= "+PartitionsCount+",	bdStgy= "+bdCastStrategy+",	bdSize= "+bdCastSize+",	GenGap= "+GenGap+",	NFC= "+TestFunctions.NFC.value+",	Time= "+theStopper.timeDiff
    val outRDD  =  sc.parallelize(fOutput)
    outRDD.saveAsTextFile("hdfs://172.18.160.17:54310/FahadMaqbool/DatasetX/"+gens+"-"+init.Dimensions+"-"+bdCastStrategy+"-"+bdCastSize+"-"+scala.util.Random.nextInt(1000)+".txt")

    
}