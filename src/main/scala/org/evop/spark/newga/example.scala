package org.evop.spark.newga

import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.util.Random
import org.apache.spark.storage.StorageLevel

object example extends Serializable {
  def main(  args:  Array[String]  )  {
    println("Dimensions ?	:")
    val  d  =  readInt()
    println("Population	? :")
    val  p  =  readInt()
    println("Generations	? :")
    val  g  =  readInt()
    println("Selection	? :	ROULETTE(R)	/	RANDOM(M)")
    var  s  =  readLine()
    println("Replacement	?	:	WEAKPARENT(W)	/	BOTHPARENT(B)")
    var  r  =  readLine()
    println("Crossover	?	:	UNIFORM(U)	/	SINGLE(S)	/	THREEPARENT(3)")
    var  c  =  readLine()
    println("Mutation	?	:	INTERCHANGE(I)	/	REVERSE(R)")
    var  m  =  readLine()
    println("Function	?	:	SPHERE(S)	/	ACKLEY(A)")
    var  o  =  readLine()
    println("Generation Gap	?	:")
    val  gp  =  readInt()
    println("Number of Solutions to Share	?	:")
    val  k  =  readInt()
    println("Sharing Strategy	?	:	B2B	/	B2W	/	H2B	/	H2W	/	BB2B	/	BB2W")
    val  st  =  readLine().toUpperCase()
    println("Number of Partitions ? ")
    val  parti  =  readInt()
    val cp  =  50
    val mp  =  5
  
    
    if(s.toLowerCase()=="r" || s.toLowerCase()=="roulette")  s="ROULETTE"
    if(s.toLowerCase()=="m" || s.toLowerCase()=="random")  s="RANDOM"
    
    if(r.toLowerCase()=="w" || r.toLowerCase()=="weakparent")  r="WEAKPARENT"
    if(r.toLowerCase()=="b" || r.toLowerCase()=="bothparent")  r="BOTHPARENT"
    
    if(c.toLowerCase()=="u" || c.toLowerCase()=="uniform")  c="UNIFORM"
    if(c.toLowerCase()=="s" || c.toLowerCase()=="single")  c="SINGLE"
    if(c.toLowerCase()=="3" || c.toLowerCase()=="3parent")  c="THREEPARENT"
    
    if(m.toLowerCase()=="i" || m.toLowerCase()=="interchange")  m="INTERCHANGE"
    if(m.toLowerCase()=="r" || m.toLowerCase()=="reverse")  m="REVERSE"
    
    if(o.toLowerCase()=="s" || o.toLowerCase()=="sphere")  o="SPHERE"
    if(o.toLowerCase()=="a" || o.toLowerCase()=="ackley")  o="ACKLEY"
    
//    if(o=="SPHERE"){
//    val ri=new RandomDoubleInitializer(p, d,  TestFunctions.SphereBound ,  TestFunctions.SphereFunc  )
//    
//    val parGA=new GA(  TestFunctions.SphereFunc,  ri,  s,  m,  r,  c,  "MIN",  30  ,  2  ,  
//        "MAX_GENS",  g,  parti ,  gp  ,  st  ,  k  ,  "local[*]",  p, d,  TestFunctions.SphereBound  )
    val conf = new SparkConf().setAppName("Parallel GA").setMaster("local[*]")
    val sc  =  new SparkContext  (  conf  )
    
//    val chromoRDD = sc.parallelize(  0 until p, parti).mapPartitionsWithIndex { (idx, iter) =>
//        val random = new Random(  idx  )
//        println("dim is  "+d)
//        var tempData  =  iter.map(i => Array.fill(d)(  5  )  )
//        println  (  tempData.mkString(", "))
//        tempData.toIterator
//    }.persist(StorageLevel.MEMORY_AND_DISK)
      var min  =  TestFunctions.SphereBound(0)
      var max  =  TestFunctions.SphereBound(1)
      val chromoRDD = sc.parallelize(  0 until p, parti).mapPartitionsWithIndex { (idx, iter) =>
        val random = new Random(  idx  )
        var tempData  =  iter.map(i => (i*1.0,Array.fill(d)(  (  new Gene  (random.nextDouble()*(max-min)+min  )  )  )  )  )
        var Data  =  tempData.map(i => (  (  idx*parti+i._1).toDouble  ,  
          new Chromosome(  (  idx*parti+i._1).toDouble, i._2,  TestFunctions.SphereFunc  ,  TestFunctions.SphereFunc(  i._2  )  )   )  )
        //  iter.map(i => (1.0, Vectors.dense(Array.fill(d)(  random.nextDouble()*(max-min)+min  ))))
        Data.toIterator
      }  .  persist(StorageLevel.MEMORY_AND_DISK)
    
    val parGA=new GA(  TestFunctions.SphereFunc,  s,  m,  r,  c,  "MIN",  cp  ,  mp  ,  
        "MAX_GENS",  g,  parti ,  gp  ,  st  ,  k  ,  "local[*]",  p, d,  TestFunctions.SphereBound,  sc  ,   chromoRDD)
//    }
//    else{
//    val ri=new RandomDoubleInitializer(p, d,  TestFunctions.AckleyBound ,  TestFunctions.AckleyFunc  )
//    
//    val parGA=new GA(  TestFunctions.AckleyFunc,  ri,  s,  m,  r,  c,  "MIN",  30  ,  2  ,
//        "MAX_GENS",  g,  parti ,  gp  ,  st  ,  k  ,  "local[*]"  )
//    }

    
    

  }
}