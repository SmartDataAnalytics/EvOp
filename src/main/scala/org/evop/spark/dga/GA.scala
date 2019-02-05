package org.evop.spark.dga

import org.apache.spark._
import org.apache.spark.rdd.RDD
//import java.io.File
//import java.io.PrintWriter
//import scala.io.Source
//import java.io._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import scala.tools.ant.sabbus.Break
import scala.util.control.Breaks._
import scala.io.Source
//import java.io.FileSystem
import java.io._
import java.util.Calendar


class GA(  f:  Array[Gene]  =>  Double  , init:Initializer  ,
    SelectorType:String  ,  MutatorType:String  ,  Replacement:String  ,  crossType:String  ,direction:String  ,
    CrossOverProb:Double  ,  MutationProb:Double  ,
    stopper_Type:String  ,  Stopper_Threshold:Long  ,  PartitionsCount:Int    ,  GenGap:Int,  
    bdCastStrategy:String  ,  bdCastSize:Int  ,  configs:String)   extends  Serializable  {
  
  
  //Create Spark Configurations
  //val conf = new SparkConf().setAppName("Parallel GA").setMaster("spark://172.18.160.16:3077") 
  //  .setMaster("local[*]")
// local mode
//  .setMaster("local[*]")
//  .set("spark.executor.memory","7g")
//  .set("spark.driver.memory","3g")
//  .set("spark.driver.maxResultSize","0")
    
  //Create Spark Context
  //val conf = new SparkConf().setAppName("Parallel GA").setMaster("local[*]")
  val conf = new SparkConf().setAppName("Parallel GA").setMaster(configs)
  val sc  =  new SparkContext  (  conf  )
  
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
    case "RANDOM"    =>  new RandomSelector(  sc  ,  MutatorType  ,  Replacement  ,  crossType,  direction  ,  CrossOverProb  ,  MutationProb  ,  bdCastStrategy  ,  bdCastSize  ,  PartitionsCount  )
    case "ROULETTE"    =>  new RouletteSelector(  sc  ,  MutatorType  ,  Replacement  ,  crossType,  direction  ,  CrossOverProb  ,  MutationProb  ,  bdCastStrategy  ,  bdCastSize  ,  PartitionsCount  )
  
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
  var nextPartitions  =  sc.parallelize  (  chromoRDD.take(  bdCastSize  )  )
  theSelector.shareBest(chromoRDD.take(  bdCastSize  ) )
    
  //println("Starting While")
  var BestofBest  =  chromoList(0)._2  //new Chromosome(0.0,Array(new Gene(0)))
  val  theStopper:Stopper  =  new Stopper(stopper_Type,Stopper_Threshold)
  var Threshold:Double  =  1.toDouble  /  (chromoList(0)._2.Genes.length.toDouble  *  PartitionsCount.toDouble  )
  breakable{
      while(  !(  theStopper.stop(gens,0)  )  &&  condition  ==  true  )  {
        
        
          nextPartitions   =  theSelector.selection(Partitioned)
          var Results  =  nextPartitions.collect()
          Results  =  Results.sortWith(  _._2.fitness  <  _._2.fitness  )
          var prRes  =  Results.map(x=>x._2)
          prRes.foreach(println)
          BestofBest  =  Results(0)._2
          
          optRecord  =  (  gens  ,  0.toLong  ,  BestofBest.fitness  )  ::  optRecord
          if (BestofBest.fitness  <=  Threshold  ){
            theStopper.forceStop()
            gens  +=  gap
            condition  =  false
          }
          else {
            gens  +=  gap
            theSelector.shareBest(Results)
          }
          println("The Best Solution After Generation "+gens+" is "+BestofBest )
    
      }
  }
  
  
  
  val fOutput   = "\n"+Calendar.getInstance().getTime+"\n"+"Dimensions= "+init.Dimensions+",	Func= "+TestFunctions.func+",	Population= "+chromoList.length+",	Partis= "+PartitionsCount+
  ",	bdSize= "+bdCastSize+",	GenGap= "+GenGap+", VTR= "+Threshold+", Gens= "+ gens+", Fitness= "+BestofBest.fitness+",	Time= "+theStopper.timeDiff+",	bdStgy= "+bdCastStrategy+"\n"
    //val outRDD  =  sc.parallelize(fOutput)
    //outRDD.saveAsTextFile("hdfs://172.18.160.17:54310/FahadMaqbool/DatasetX/"+gens+"-"+init.Dimensions+"-"+bdCastStrategy+"-"+bdCastSize+"-"+scala.util.Random.nextInt(1000)+".txt")
  val fw = new FileWriter("/data/home/FahadMaqbool/PGA/newResults.txt", true)
  fw.write(fOutput)
  fw.close()
  
  val fDetail   = "\n\n\n"+Calendar.getInstance().getTime+"\nGens= "+ gens+",	Dimensions= "+init.Dimensions+",	Func= "+TestFunctions.func+",	Population= "+chromoList.length+",	Selector= "+SelectorType+",	Mutator= "+MutatorType+",	Replace= "+Replacement+",	Dir= "+direction+",	xovrP= "+
    CrossOverProb+",	MutP= "+MutationProb+",	Stoper= "+stopper_Type+",	xovr= "+crossType+",	Partis= "+PartitionsCount+",	bdStgy= "+bdCastStrategy+",	bdSize= "+bdCastSize+",	GenGap= "+GenGap+",	Time= "+theStopper.timeDiff+"\n"
    //val outRDD  =  sc.parallelize(fOutput)
    //outRDD.saveAsTextFile("hdfs://172.18.160.17:54310/FahadMaqbool/DatasetX/"+gens+"-"+init.Dimensions+"-"+bdCastStrategy+"-"+bdCastSize+"-"+scala.util.Random.nextInt(1000)+".txt")
  val fw2 = new FileWriter("/data/home/FahadMaqbool/PGA/newDetails.txt", true)
  fw2.write(fOutput)
  for (  u  <-  0 to optRecord.length-1  )
    fw2.write(optRecord(u)._1+"	,	"+optRecord(u)._3+"	,	"+optRecord(u)._2)
  fw2.write("			-------------------------------------------			")
  fw2.close()
    
}