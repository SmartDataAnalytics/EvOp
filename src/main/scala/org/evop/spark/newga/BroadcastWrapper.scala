package org.evop.spark.newga

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast


import scala.reflect.ClassTag


case class BroadcastWrapper[T: ClassTag](
                                          @transient sc: SparkContext,
                                          @transient _v: T
                                        ) extends Serializable {

  var broadcasted: Broadcast[T] = sc.broadcast(_v)
  
  def unpersist()  {
    broadcasted.unpersist()
    broadcasted.destroy()
  }

  def update(v: T): Unit = {
    
    try {
      broadcasted.unpersist()
      broadcasted.destroy()
    }
    catch {
      case e: Throwable =>
        println("broadcast cannot be destroyed", e)
    }
    broadcasted = sc.broadcast(v)
  }

  def value: T = broadcasted.value
}


//
//import java.io.{ObjectInputStream, ObjectOutputStream}
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.streaming.StreamingContext
//import scala.reflect.ClassTag
//
//// This wrapper lets us update brodcast variables within DStreams' foreachRDD
//
//// without running into serialization issues
//case class BroadcastWrapper[T: ClassTag](
//    
//  @transient private val ssc: StreamingContext,
//  @transient private val _v: T
//) extends Serializable {
//
//  @transient private var v = ssc.sparkContext.broadcast(_v)
//
//  def update(newValue: T, blocking: Boolean = false): Unit = {
//    v.unpersist(blocking)
//    v = ssc.sparkContext.broadcast(newValue)
//  }
//
//  def value: T = v.value
//
//  private def writeObject(out: ObjectOutputStream): Unit = {
//    out.writeObject(v)
//  }
//
//  private def readObject(in: ObjectInputStream): Unit = {
//    v = in.readObject().asInstanceOf[Broadcast[T]]
//}
//}





