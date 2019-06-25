package org.evop.spark.dga

import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.util.Random
import org.apache.spark.storage.StorageLevel

object example2 {
  def main(  args:  Array[String]   )  {

      var  d  =  args(0).toInt

      val  p  =  args(1).toInt

      val  g  =  args(2).toInt
  
      var  s  =  args(3)

      var  r  =  args(4)

      var  c  =  args(5)

      var  m  =  args(6)

      var  o  =  args(7)

      val  gp  =  args(8).toInt

      val  k  =  args(9).toInt

      val  st  =  args(10).toUpperCase()

      val  parti  =  args(11).toInt
      
      d  =  d  /  parti
      
      var cp  =  args(12).toInt
      
      var mp  =  args(13).toInt
      var configs  =  args(14)
  
    
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
    if(o.toLowerCase()=="g" || o.toLowerCase()=="griewank")  o="GRIEWANK"
    if(o.toLowerCase()=="r" || o.toLowerCase()=="rastrigin")  o="RASTRIGIN"
    if(o.toLowerCase()=="sp" || o.toLowerCase()=="sumpowers")  o="SUMPOWERS"
    if(o.toLowerCase()=="ss" || o.toLowerCase()=="sumsquares")  o="SUMSQUARES"
    if(o.toLowerCase()=="z" || o.toLowerCase()=="zakharov")  o="ZAKHAROV"
    if(o.toLowerCase()=="sc" || o.toLowerCase()=="schwefel")  o="SCHWEFEL"
    if(o.toLowerCase()=="ro" || o.toLowerCase()=="rosen")  o="ROSEN"
    if(o.toLowerCase()=="d" || o.toLowerCase()=="dixon")  o="DIXON"
    if(o.toLowerCase()=="rt" || o.toLowerCase()=="dixon")  o="ROTATED"
    
    TestFunctions.func  =  o
    
    val conf = new SparkConf().setAppName("Parallel GA").setMaster(configs)
    val sc  =  new SparkContext  (  conf  )
    
         
    o match {
        case "ACKLEY" =>  {
          //val ri=new RandomDoubleInitializer(p, d,  TestFunctions.AckleyBound ,  TestFunctions.AckleyFunc  )
          var min  =  TestFunctions.AckleyBound(0)
            var max  =  TestFunctions.AckleyBound(1)
            val chromoRDD = sc.parallelize(  0 until p, parti).mapPartitionsWithIndex { (idx, iter) =>
              val random = new Random(  idx  )
              var tempData  =  iter.map(i => (i*1.0,Array.fill(d)(  new Gene(  random.nextDouble()*(max-min)+min  )  )  )  )
              var Data  =  tempData.map(i => (  (  idx*parti+i._1).toDouble  ,  
              new Chromosome(  (  idx*parti+i._1).toDouble, i._2,  TestFunctions.AckleyFunc  ,  TestFunctions.AckleyFunc(  i._2  ,  0  )  )   )  )
              Data.toIterator
              }  .  persist(StorageLevel.MEMORY_AND_DISK)
          val parGA=new GA(  TestFunctions.AckleyFunc,  s,  m,  r,  c,  "MIN",  cp  ,  mp  ,
        "MAX_GENS",  g,  parti ,  gp  ,  st  ,  k  ,  configs,  p, d,  TestFunctions.AckleyBound,  sc  ,   chromoRDD  )
        }
        case "SPHERE" =>  {
            //val ri=new RandomDoubleInitializer(p, d,  TestFunctions.SphereBound ,  TestFunctions.SphereFunc  )
            var min  =  TestFunctions.SphereBound(0)
            var max  =  TestFunctions.SphereBound(1)
            val chromoRDD = sc.parallelize(  0 until p, parti).mapPartitionsWithIndex { (idx, iter) =>
              val random = new Random(  idx  )
              var tempData  =  iter.map(i => (i*1.0,Array.fill(d)(  new Gene(  random.nextDouble()*(max-min)+min  )  )  )  )
              var Data  =  tempData.map(i => (  (  idx*parti+i._1).toDouble  ,  
              new Chromosome(  (  idx*parti+i._1).toDouble, i._2,  TestFunctions.SphereFunc  ,  TestFunctions.SphereFunc(  i._2 , 0 )  )   )  )
              Data.toIterator
              }  .  persist(StorageLevel.MEMORY_AND_DISK)
            val parGA=new GA(  TestFunctions.SphereFunc,  s,  m,  r,  c,  "MIN",  cp  ,  mp  ,  
              "MAX_GENS",  g,  parti ,  gp  ,  st  ,  k  ,  configs,  p, d,  TestFunctions.SphereBound,  sc  ,   chromoRDD)
        }
         case "GRIEWANK" =>  {
            //val ri=new RandomDoubleInitializer(p, d,  TestFunctions.SphereBound ,  TestFunctions.SphereFunc  )
            var min  =  TestFunctions.GriewankBound(0)
            var max  =  TestFunctions.GriewankBound(1)
            val chromoRDD = sc.parallelize(  0 until p, parti).mapPartitionsWithIndex { (idx, iter) =>
              val random = new Random(  idx  )
              var tempData  =  iter.map(i => (i*1.0,Array.fill(d)(  new Gene(  random.nextDouble()*(max-min)+min  )  )  )  )
              var Data  =  tempData.map(i => (  (  idx*parti+i._1).toDouble  ,  
              new Chromosome(  (  idx*parti+i._1).toDouble, i._2,  TestFunctions.GriewankFunc  ,  TestFunctions.GriewankFunc(  i._2 , 0 )  )   )  )
              Data.toIterator
              }  .  persist(StorageLevel.MEMORY_AND_DISK)
            val parGA=new GA(  TestFunctions.GriewankFunc,  s,  m,  r,  c,  "MIN",  cp  ,  mp  ,  
              "MAX_GENS",  g,  parti ,  gp  ,  st  ,  k  ,  configs,  p, d,  TestFunctions.GriewankBound,  sc  ,   chromoRDD)
        }
         case "RASTRIGIN" =>  {
            //val ri=new RandomDoubleInitializer(p, d,  TestFunctions.SphereBound ,  TestFunctions.SphereFunc  )
            var min  =  TestFunctions.RastriginBound(0)
            var max  =  TestFunctions.RastriginBound(1)
            val chromoRDD = sc.parallelize(  0 until p, parti).mapPartitionsWithIndex { (idx, iter) =>
              val random = new Random(  idx  )
              var tempData  =  iter.map(i => (i*1.0,Array.fill(d)(  new Gene(  random.nextDouble()*(max-min)+min  )  )  )  )
              var Data  =  tempData.map(i => (  (  idx*parti+i._1).toDouble  ,  
              new Chromosome(  (  idx*parti+i._1).toDouble, i._2,  TestFunctions.RastriginFunc  ,  TestFunctions.RastriginFunc(  i._2 , 0 )  )   )  )
              Data.toIterator
              }  .  persist(StorageLevel.MEMORY_AND_DISK)
            val parGA=new GA(  TestFunctions.RastriginFunc,  s,  m,  r,  c,  "MIN",  cp  ,  mp  ,  
              "MAX_GENS",  g,  parti ,  gp  ,  st  ,  k  ,  configs,  p, d,  TestFunctions.RastriginBound,  sc  ,   chromoRDD)
        }
         case "ZAKHAROV" =>  {
            //val ri=new RandomDoubleInitializer(p, d,  TestFunctions.SphereBound ,  TestFunctions.SphereFunc  )
            var min  =  TestFunctions.ZakharovBound(0)
            var max  =  TestFunctions.ZakharovBound(1)
            val chromoRDD = sc.parallelize(  0 until p, parti).mapPartitionsWithIndex { (idx, iter) =>
              val random = new Random(  idx  )
              var tempData  =  iter.map(i => (i*1.0,Array.fill(d)(  new Gene(  random.nextDouble()*(max-min)+min  )  )  )  )
              var Data  =  tempData.map(i => (  (  idx*parti+i._1).toDouble  ,  
              new Chromosome(  (  idx*parti+i._1).toDouble, i._2,  TestFunctions.ZakharovFunc  ,  TestFunctions.ZakharovFunc(  i._2 , 0 )  )   )  )
              Data.toIterator
              }  .  persist(StorageLevel.MEMORY_AND_DISK)
            val parGA=new GA(  TestFunctions.ZakharovFunc,  s,  m,  r,  c,  "MIN",  cp  ,  mp  ,  
              "MAX_GENS",  g,  parti ,  gp  ,  st  ,  k  ,  configs,  p, d,  TestFunctions.ZakharovBound,  sc  ,   chromoRDD)
        }
        case "SUMPOWERS" =>  {
            //val ri=new RandomDoubleInitializer(p, d,  TestFunctions.SphereBound ,  TestFunctions.SphereFunc  )
            var min  =  TestFunctions.SumOfDiffPowersBound(0)
            var max  =  TestFunctions.SumOfDiffPowersBound(1)
            val chromoRDD = sc.parallelize(  0 until p, parti).mapPartitionsWithIndex { (idx, iter) =>
              val random = new Random(  idx  )
              var tempData  =  iter.map(i => (i*1.0,Array.fill(d)(  new Gene(  random.nextDouble()*(max-min)+min  )  )  )  )
              var Data  =  tempData.map(i => (  (  idx*parti+i._1).toDouble  ,  
              new Chromosome(  (  idx*parti+i._1).toDouble, i._2,  TestFunctions.SumOfDiffPowersFunc  ,  TestFunctions.SumOfDiffPowersFunc(  i._2 , 0 )  )   )  )
              Data.toIterator
              }  .  persist(StorageLevel.MEMORY_AND_DISK)
            val parGA=new GA(  TestFunctions.SumOfDiffPowersFunc,  s,  m,  r,  c,  "MIN",  cp  ,  mp  ,  
              "MAX_GENS",  g,  parti ,  gp  ,  st  ,  k  ,  configs,  p, d,  TestFunctions.SumOfDiffPowersBound,  sc  ,   chromoRDD)
        }
        case "SUMSQUARES" =>  {
            //val ri=new RandomDoubleInitializer(p, d,  TestFunctions.SphereBound ,  TestFunctions.SphereFunc  )
            var min  =  TestFunctions.SumSquaresBound(0)
            var max  =  TestFunctions.SumSquaresBound(1)
            val chromoRDD = sc.parallelize(  0 until p, parti).mapPartitionsWithIndex { (idx, iter) =>
              val random = new Random(  idx  )
              var tempData  =  iter.map(i => (i*1.0,Array.fill(d)(  new Gene(  random.nextDouble()*(max-min)+min  )  )  )  )
              var Data  =  tempData.map(i => (  (  idx*parti+i._1).toDouble  ,  
              new Chromosome(  (  idx*parti+i._1).toDouble, i._2,  TestFunctions.SumSquaresFunc  ,  TestFunctions.SumSquaresFunc(  i._2 , 0 )  )   )  )
              Data.toIterator
              }  .  persist(StorageLevel.MEMORY_AND_DISK)
            val parGA=new GA(  TestFunctions.SumSquaresFunc,  s,  m,  r,  c,  "MIN",  cp  ,  mp  ,  
              "MAX_GENS",  g,  parti ,  gp  ,  st  ,  k  ,  configs,  p, d,  TestFunctions.SumSquaresBound,  sc  ,   chromoRDD)
        }
        case "SCHWEFEL" =>  {
            //val ri=new RandomDoubleInitializer(p, d,  TestFunctions.SphereBound ,  TestFunctions.SphereFunc  )
            var min  =  TestFunctions. SchwefelBound(0)
            var max  =  TestFunctions. SchwefelBound(1)
            val chromoRDD = sc.parallelize(  0 until p, parti).mapPartitionsWithIndex { (idx, iter) =>
              val random = new Random(  idx  )
              var tempData  =  iter.map(i => (i*1.0,Array.fill(d)(  new Gene(  random.nextDouble()*(max-min)+min  )  )  )  )
              var Data  =  tempData.map(i => (  (  idx*parti+i._1).toDouble  ,  
              new Chromosome(  (  idx*parti+i._1).toDouble, i._2,  TestFunctions. SchwefelFunc  ,  TestFunctions. SchwefelFunc(  i._2 , 0 )  )   )  )
              Data.toIterator
              }  .  persist(StorageLevel.MEMORY_AND_DISK)
            val parGA=new GA(  TestFunctions. SchwefelFunc,  s,  m,  r,  c,  "MIN",  cp  ,  mp  ,  
              "MAX_GENS",  g,  parti ,  gp  ,  st  ,  k  ,  configs,  p, d,  TestFunctions. SchwefelBound,  sc  ,   chromoRDD)
        }
        case "ROSEN" =>  {
            //val ri=new RandomDoubleInitializer(p, d,  TestFunctions.SphereBound ,  TestFunctions.SphereFunc  )
            var min  =  TestFunctions. RosenBound(0)
            var max  =  TestFunctions. RosenBound(1)
            val chromoRDD = sc.parallelize(  0 until p, parti).mapPartitionsWithIndex { (idx, iter) =>
              val random = new Random(  idx  )
              var tempData  =  iter.map(i => (i*1.0,Array.fill(d)(  new Gene(  random.nextDouble()*(max-min)+min  )  )  )  )
              var Data  =  tempData.map(i => (  (  idx*parti+i._1).toDouble  ,  
              new Chromosome(  (  idx*parti+i._1).toDouble, i._2,  TestFunctions. RosenFunc  ,  TestFunctions. RosenFunc(  i._2 , 0 )  )   )  )
              Data.toIterator
              }  .  persist(StorageLevel.MEMORY_AND_DISK)
            val parGA=new GA(  TestFunctions. RosenFunc,  s,  m,  r,  c,  "MIN",  cp  ,  mp  ,  
              "MAX_GENS",  g,  parti ,  gp  ,  st  ,  k  ,  configs,  p, d,  TestFunctions. RosenBound,  sc  ,   chromoRDD)
        }
        case "DIXON" =>  {
            //val ri=new RandomDoubleInitializer(p, d,  TestFunctions.SphereBound ,  TestFunctions.SphereFunc  )
            var min  =  TestFunctions. DixonBound(0)
            var max  =  TestFunctions. DixonBound(1)
            val chromoRDD = sc.parallelize(  0 until p, parti).mapPartitionsWithIndex { (idx, iter) =>
              val random = new Random(  idx  )
              var tempData  =  iter.map(i => (i*1.0,Array.fill(d)(  new Gene(  random.nextDouble()*(max-min)+min  )  )  )  )
              var Data  =  tempData.map(i => (  (  idx*parti+i._1).toDouble  ,  
              new Chromosome(  (  idx*parti+i._1).toDouble, i._2,  TestFunctions. DixonFunc  ,  TestFunctions. DixonFunc(  i._2 , 0 )  )   )  )
              Data.toIterator
              }  .  persist(StorageLevel.MEMORY_AND_DISK)
            val parGA=new GA(  TestFunctions. DixonFunc,  s,  m,  r,  c,  "MIN",  cp  ,  mp  ,  
              "MAX_GENS",  g,  parti ,  gp  ,  st  ,  k  ,  configs,  p, d,  TestFunctions. DixonBound,  sc  ,   chromoRDD)
        }
        case "ROTATED" =>  {
            //val ri=new RandomDoubleInitializer(p, d,  TestFunctions.SphereBound ,  TestFunctions.SphereFunc  )
            var min  =  TestFunctions. RotatedBound(0)
            var max  =  TestFunctions. RotatedBound(1)
            val chromoRDD = sc.parallelize(  0 until p, parti).mapPartitionsWithIndex { (idx, iter) =>
              val random = new Random(  idx  )
              var tempData  =  iter.map(i => (i*1.0,Array.fill(d)(  new Gene(  random.nextDouble()*(max-min)+min  )  )  )  )
              var Data  =  tempData.map(i => (  (  idx*parti+i._1).toDouble  ,  
              new Chromosome(  (  idx*parti+i._1).toDouble, i._2,  TestFunctions. RotatedFunc  ,  TestFunctions. RotatedFunc(  i._2 , 0 )  )   )  )
              Data.toIterator
              }  .  persist(StorageLevel.MEMORY_AND_DISK)
            val parGA=new GA(  TestFunctions. RotatedFunc,  s,  m,  r,  c,  "MIN",  cp  ,  mp  ,  
              "MAX_GENS",  g,  parti ,  gp  ,  st  ,  k  ,  configs,  p, d,  TestFunctions. RotatedBound,  sc  ,   chromoRDD)
        }
//        case "GRIEWANK" =>  {
//          //val ri=new RandomDoubleInitializer(p, d,  TestFunctions.GriewankBound ,  TestFunctions.GriewankFunc  )
//          val parGA=new GA(  TestFunctions.GriewankFunc,  s,  m,  r,  c,  "MIN",  cp  ,  mp  ,
//        "MAX_GENS",  g,  parti ,  gp  ,  st  ,  k  ,  configs,  p, d,  TestFunctions.GriewankBound  )
//        }
//        case "RASTRIGIN" =>  {
//           //val ri=new RandomDoubleInitializer(p, d,  TestFunctions.RastriginBound ,  TestFunctions.RastriginFunc  )
//           val parGA=new GA(  TestFunctions.RastriginFunc,  s,  m,  r,  c,  "MIN",  cp  ,  mp  ,  
//        "MAX_GENS",  g,  parti ,  gp  ,  st  ,  k  ,  configs,  p, d,  TestFunctions.RastriginBound  )
//        }
//        case "SUMPOWERS" =>  {
//          val ri=new RandomDoubleInitializer(p, d,  TestFunctions.SumOfDiffPowersBound ,  TestFunctions.SumOfDiffPowersFunc  )
//          val parGA=new GA(  TestFunctions.SumOfDiffPowersFunc,  s,  m,  r,  c,  "MIN",  cp  ,  mp  ,
//        "MAX_GENS",  g,  parti ,  gp  ,  st  ,  k  ,  configs,  p, d,  TestFunctions.SumOfDiffPowersBound  )         
//        }
//        case "SUMSQUARES" =>  {
//          //val ri=new RandomDoubleInitializer(p, d,  TestFunctions.SumSquaresBound ,  TestFunctions.SumSquaresFunc  )
//          val parGA=new GA(  TestFunctions.SumSquaresFunc,  s,  m,  r,  c,  "MIN",  cp  ,  mp  ,
//        "MAX_GENS",  g,  parti ,  gp  ,  st  ,  k  ,  configs,  p, d,  TestFunctions.SumSquaresBound  )  
//        }
//        case "ZAKHAROV" =>  {
//          //val ri=new RandomDoubleInitializer(p, d,  TestFunctions.ZakharovBound ,  TestFunctions.ZakharovFunc  )
//          val parGA=new GA(  TestFunctions.ZakharovFunc,  s,  m,  r,  c,  "MIN",  cp  ,  mp  ,
//        "MAX_GENS",  g,  parti ,  gp  ,  st  ,  k  ,  configs,  p, d,  TestFunctions.ZakharovBound  )
//        }
//        
    }

    
    

  }
}