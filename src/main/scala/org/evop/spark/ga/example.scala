package org.evop.spark.ga

object example {
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
    
    if(o=="SPHERE"){
    val ri=new RandomDoubleInitializer(p, d,  TestFunctions.SphereBound ,  TestFunctions.SphereFunc  )
    println("NFC          "+TestFunctions.NFC)
    val parGA=new GA(  TestFunctions.SphereFunc,  ri,  s,  m,  r,  c,  "MIN",  30  ,  2  ,  
        "MAX_GENS",  g,  2 ,  gp  ,  st  ,  k  )
    }
    else{
    val ri=new RandomDoubleInitializer(p, d,  TestFunctions.AckleyBound ,  TestFunctions.AckleyFunc  )
    println("NFC          "+TestFunctions.NFC)
    val parGA=new GA(  TestFunctions.AckleyFunc,  ri,  s,  m,  r,  c,  "MIN",  30  ,  2  ,
        "MAX_GENS",  g,  2 ,  gp  ,  st  ,  k  )
    }

    println("NFC          "+TestFunctions.NFC)
    

  }
}