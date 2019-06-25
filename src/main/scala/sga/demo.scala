package sga

object demo {
  def main(  args:  Array[String]  )  {
    
    println("Enter the DIMENSIONS")
    val  d  =  args(0).toInt  //readInt() 
    println("Enter the POPULATIONS")
    val  p  =  args(1).toInt  //readInt()
    println("Enter the GENERATIONS")
    val  g  =  args(2).toInt  //readInt()
    println("Enter the SELECTION SCHEME")
    var  s  =  args(3)  //readLine()
    println("Enter the REPLACEMENT SCHEME")
    var  r  =  args(4)  //readLine()
    println("Enter the CROSSOVER SCHEME")
    var  c  =  args(5)  //readLine()
    println("Enter the MUTATION SCHEME")
    var  m  =  args(6)  //readLine()
    println("Enter the OPTIMIZATION FUNCTION")
    var  o  =  args(7)  //readLine()
    
    if(s.toLowerCase()=="r" || s.toLowerCase()=="roulette")  s="ROULETTE"
    if(s.toLowerCase()=="m" || s.toLowerCase()=="random")  s="RANDOM"
    
    
    if(r.toLowerCase()=="w" || r.toLowerCase()=="weakparent")  r="WEAKPARENT"
    if(r.toLowerCase()=="b" || r.toLowerCase()=="bothparent")  r="BOTHPARENT"
    
    if(c.toLowerCase()=="u" || c.toLowerCase()=="uniform")  c="UNIFORM"
    if(c.toLowerCase()=="s" || c.toLowerCase()=="single")  c="SINGLE"
    if(c.toLowerCase()=="3" || c.toLowerCase()=="3parent")  c="THREEPARENT"
    
    if(m.toLowerCase()=="i" || m.toLowerCase()=="interchange")  m="INTERCHANGE"
    if(m.toLowerCase()=="r" || m.toLowerCase()=="reverse")  m="REVERSE"
    
    
    if  (  o=="ACKLEY"  )  { 
      val ri=new RandomDoubleInitializer(p, d,  TestFunctions.AckleyBound ,  TestFunctions.AckleyFunc  )
      val parGA=new GA(  TestFunctions.AckleyFunc,  ri,  s,  m,  r,  c,  "MIN",  30  ,  5  ,  "MAX_GENS",  g,  5 )
    }
    
    if  (  o=="SPHERE"  )  {
      val ri=new RandomDoubleInitializer(p, d,  TestFunctions.SphereBound ,  TestFunctions.SphereFunc  )
      val parGA=new GA(  TestFunctions.SphereFunc,  ri,  s,  m,  r,  c,  "MIN",  30  ,  5  ,  "MAX_GENS",  g,  5 )
    }
    
    
    if  (  o=="GRIEWANK"  )  {
      val ri=new RandomDoubleInitializer(p, d,  TestFunctions.GriewankBound ,  TestFunctions.GriewankFunc  )
      val parGA=new GA(  TestFunctions.GriewankFunc,  ri,  s,  m,  r,  c,  "MIN",  30  ,  5  ,  "MAX_GENS",  g,  5 )
    }
    
    
  }
}