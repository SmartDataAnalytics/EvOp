package pGA

import org.apache.spark._
import pGA._
import org.apache.spark.rdd.RDD
import java.io.File
//import java.io.PrintWriter
//import scala.io.Source
import java.io._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import scala.tools.ant.sabbus.Break
import scala.util.control.Breaks._
import scala.io.Source

class GA(  f:  Array[Gene]  =>  Double  , init:Initializer  ,
    SelectorType:String  ,  MutatorType:String  ,  Replacement:String  ,  crossType:String  ,direction:String  ,
    CrossOverProb:Double  ,  MutationProb:Double  ,
    stopper_Type:String  ,  Stopper_Threshold:Long  ,  PartitionsCount:Int    )   extends  Serializable  {
  
  
  //Create Spark Configurations
  val conf = new SparkConf().set("spark.executor.memory","7g")
  .set("spark.driver.memory","3g")
  .set("spark.driver.maxResultSize","0")
  .setAppName("Parallel GA").setMaster("local[*]")  // local mode
  
    
  //Create Spark Context
  val sc = new SparkContext(conf)
  //val ssc  =  new StreamingContext(conf, Seconds(1))
  
  //  Parameters PreProcessing to appropriate objects
  var chromoList:List[(Double, Chromosome)]=List()
  init.InitType match{
      case 1 => {  chromoList  =  init.asInstanceOf[RandomInitializer].chromoList  }  //IntRDDCreation()
      case 2 => {  chromoList  =  init.asInstanceOf[RandomDoubleInitializer].chromoList  }  //FileRDDCreation()
  }
  
    
  val theSelector  =  SelectorType match{
    case "RANDOM"    =>  new RandomSelector(  sc  ,  MutatorType  ,  Replacement  ,  crossType,  direction  ,  CrossOverProb  ,  MutationProb  )
    case "ROULETTE"    =>  new RouletteSelector(  sc  ,  MutatorType  ,  Replacement  ,  crossType,  direction  ,  CrossOverProb  ,  MutationProb  )
  
  }
  //  End Parameters PreProcessing
  var optRecord  :  List[(  Int,  Int,  Double  )]  =  List()
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
  var gap:Int  =  10
  
  
  var condition  =  true
  var nextPartitions  =  Partitioned
    
  //println("Starting While")
  var BestofBest  =  new Chromosome(0.0,Array(new Gene(0)))
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
      //nextPartitions  =  newPartitions
      //newPartitions.unpersist()
      nextPartitions.persist()
      //println("BroadCasting")
      BestofBest  =  theSelector.selectBest(nextPartitions)
      println("The Best Solution After Generation "+gens+" is "+BestofBest )
      optRecord  =  (  gens  ,  TestFunctions.NFC  ,  BestofBest.fitness  )  ::  optRecord
      if (BestofBest.fitness<=0){
        theStopper.forceStop()
        gens  -=  1
        condition  =  false
      }
      gens  +=  1
    }
    else  if(  gens  %  gap  ==  0)  {
      nextPartitions   =  theSelector.selection(nextPartitions)
      //nextPartitions  =  newPartitions
      //newPartitions.unpersist()
      nextPartitions.persist()
      //println("Recieving")
      nextPartitions  =  theSelector.eliminateWeak(nextPartitions)
      nextPartitions.collect()
      nextPartitions.persist()
      gens  +=  1
    }
    else if (gap>2){
      nextPartitions   =  theSelector.selection(nextPartitions,gap-2)
      //nextPartitions  =  newPartitions
      //newPartitions.unpersist()
      nextPartitions.persist()
      gens  +=  gap-2
            
    }
    
  }
  }
  
  val dir1:File  =  new File("./Results/"+SelectorType+"-"+Replacement+"-"+crossType+"-"+MutatorType)
  if(!dir1.exists())
    dir1.mkdir()
  val dir2:File  =  new File("./NFC/"+SelectorType+"-"+Replacement+"-"+crossType+"-"+MutatorType)
  if(!dir2.exists())
    dir2.mkdir()
  val fName  =  gens+"D"+init.Dimensions+"X"+CrossOverProb+"M"+MutationProb+"-"+scala.util.Random.nextInt(1000)
  val writer = new PrintWriter(new File("./Results/"+SelectorType+"-"+Replacement+"-"+crossType+"-"+MutatorType+"/"+fName+".txt")  )
 
  writer.write(Stopper_Threshold+", "+init.Dimensions+", "+SelectorType+", "+MutatorType+", "+Replacement+", "+direction+", "+
    CrossOverProb+", "+MutationProb+", "+stopper_Type+", "+PartitionsCount+", "+BestofBest.fitness+", "+theStopper.timeDiff+"\n")
  for (oRecord <- optRecord)
    writer.write(oRecord._1+","+oRecord._3+"\n")
  writer.close()
  
  val fw = new FileWriter("./Results/Results.txt", true)
  fw.write(fName+", "+gens+", "+init.Dimensions+", "+SelectorType+", "+MutatorType+", "+Replacement+", "+direction+", "+
    CrossOverProb+", "+MutationProb+", "+stopper_Type+", "+crossType+", "+PartitionsCount+", "+BestofBest.fitness+", "+TestFunctions.NFC+", "+gens+", "+theStopper.timeDiff)
  fw.write("\n")
  fw.close()
  
  
  val Nwriter = new PrintWriter(new File("./NFC/"+SelectorType+"-"+Replacement+"-"+crossType+"-"+MutatorType+"/"+fName+".txt")  )
  Nwriter.write(Stopper_Threshold+", "+init.Dimensions+", "+SelectorType+", "+MutatorType+", "+Replacement+", "+direction+", "+
    CrossOverProb+", "+MutationProb+", "+stopper_Type+", "+PartitionsCount+", "+BestofBest.fitness+", "+theStopper.timeDiff+"\n")
  for (oRecord <- optRecord)
    Nwriter.write(oRecord._1+","+oRecord._2+"\n")
  Nwriter.close()

}
