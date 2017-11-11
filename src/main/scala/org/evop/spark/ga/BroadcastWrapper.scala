package pGA

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast


import scala.reflect.ClassTag


case class BroadcastWrapper[T: ClassTag](
                                          @transient sc: SparkContext,
                                          @transient _v: T
                                        ) extends Serializable {

  var broadcasted: Broadcast[T] = sc.broadcast(_v)

  def update(v: T): Unit = {
    
    try {
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
