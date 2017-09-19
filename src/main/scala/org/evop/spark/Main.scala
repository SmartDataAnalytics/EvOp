package org.evop.spark

import java.net.URI
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.evop.spark.ga.GeneticAlgorithm

object EvolutionaryOptimization {

  def main(args: Array[String]) = {
    if (args.length < 2) {
      System.err.println(
        "Usage: GeneticAlgorithm <PopulationInputFile> <fitnes>")
      System.exit(1)
    }
    val initialpopulation = args(0)
    val fitness = args(1).toDouble
    val optionsList = args.drop(2).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _             => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }
    val options = mutable.Map(optionsList: _*)

    options.foreach {
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }
    println("======================================")
    println("|        Genetic Algorithm Main      |")
    println("======================================")

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Genetic Algorithm (" + fitness + ")")
      .getOrCreate()

    //GeneticAlgorithm.evaluate()

    sparkSession.stop

  }

}