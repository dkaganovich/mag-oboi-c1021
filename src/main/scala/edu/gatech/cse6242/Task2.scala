package edu.gatech.cse6242

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Task2 {
  def main(args: Array[String]) {
    val sparkCtx = new SparkContext(new SparkConf().setAppName("task2"))

    val inputFile = sparkCtx.textFile("hdfs://localhost:8020" + args(0))

    val node2Weight = inputFile.map((line: String) => {
      val tkns = line.split("\t"); val node  = tkns(1).toInt; val weight = tkns(2).toInt
      (node, weight)
    }).reduceByKey(_ + _)

    node2Weight.map { case (node, totalWeight) => Array(node, totalWeight).mkString("\t") }
      .saveAsTextFile("hdfs://localhost:8020" + args(1))
  }
}
