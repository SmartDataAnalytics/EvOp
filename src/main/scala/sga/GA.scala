package sga

import java.io.File
import java.io._
import scala.util.control.Breaks._

class GA(  f:  Array[Gene]  =>  Double  , init:Initializer  ,
    SelectorType:String  ,  MutatorType:String  ,  Replacement:String  ,  crossType:String  ,direction:String  ,
    CrossOverProb:Double  ,  MutationProb:Double  ,
    stopper_Type:String  ,  Stopper_Threshold:Long  ,  PartitionsCount:Int    )   extends  Serializable  {
  
  

  
  
  //  Parameters PreProcessing to appropriate objects
  var chromoList:List[(Double, Chromosome)]=List()
  init.InitType match{
      case 1 => {  chromoList  =  init.asInstanceOf[RandomInitializer].chromoList  }  //IntRDDCreation()
      case 2 => {  chromoList  =  init.asInstanceOf[RandomDoubleInitializer].chromoList  }  //FileRDDCreation()
  }
  
    
  val theSelector  =  SelectorType match{
    case "RANDOM"    =>  new RandomSelector(  MutatorType  ,  Replacement  ,  crossType,  direction  ,  CrossOverProb  ,  MutationProb  )
    case "ROULETTE"    =>  new RouletteSelector(  MutatorType  ,  Replacement  ,  crossType,  direction  ,  CrossOverProb  ,  MutationProb  )
  
  }
  //  End Parameters PreProcessing
  var optRecord  :  List[(  Int,  Int,  Double  )]  =  List()
  //Intialize Generation Number to 1
  var gens:Int  =  1
  
  //RDD Creation from List
  //val chromoRDD = sc.parallelize(chromoList)
  
  // Create Range Partitioner
  //val TunedPartitioner  =  new RangePartitioner  (  PartitionsCount  ,  chromoRDD  )
  
  // Partition RDD according to Range partitioner
  //val Partitioned  =  chromoRDD.partitionBy  (  TunedPartitioner  ).persist(  )
  //Partitioned.collect()
  
  //Define the Gap after which Best Solution of a Partition will replace the weak solution of other partition
  var gap:Int  =  10
  
  
  var condition  =  true
  var nextPartitions:List[(Double, Chromosome)]  =  init.chromoList
    
  //println("Starting While")
  var BestofBest  =  new Chromosome(0.0,Array(new Gene(100)))
  val  theStopper:Stopper  =  new Stopper(stopper_Type,Stopper_Threshold)
  
    while(  !(  theStopper.stop(gens,0)  )  &&  condition  ==  true  )  {
        
        nextPartitions   =  theSelector.selection(nextPartitions)
        
        
          BestofBest  =  theSelector.selectBest(nextPartitions)
          println("The Best Solution After Generation "+gens+" is "+BestofBest )
        
        
          optRecord  =  (  gens  ,  TestFunctions.NFC  ,  BestofBest.fitness  )  ::  optRecord
        if (  BestofBest.fitness.abs  <=  0.0005  ){
          println("Going to Stop")
          theStopper.forceStop()
          gens  -=  1
          condition  =  false
        }
          println("Generation is "+gens+"					Stopping Criteria is "+  !(theStopper.stop(gens,0))  )
          gens  +=  1
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