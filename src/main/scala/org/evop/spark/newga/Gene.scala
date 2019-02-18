package org.evop.spark.newga

class Gene(geneVal: Double) extends Serializable   {
  
  val Allele:Double = geneVal
 
  def Mutate(newAllele: Double):Gene  =  new Gene(newAllele)
  override def toString(): String = Allele.toString()
  def toDouble(): Double = Allele.toDouble
   
}