package com.github.codelionx

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object WordCount {

  def main(args: Array[String]) : Unit = {
    val arguments = ParameterTool.fromArgs(args)

    // the file to read the data from
    val input: String = arguments.get("input")
    if(input == null) {
      System.err.println("No input specified. Please run 'SocketWindowWordCount --input <file_path>'")
      return
    }

    val output: String = arguments.get("output")
    if(output == null) {
      System.err.println("No output specified. Please run 'SocketWindowWordCount --output <file_path>'")
      return
    }

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by creading the file
    val text = env.readTextFile(input)

    // parse the data, group it, window it, and aggregate the counts
    val windowCounts = text
      .flatMap( _.split("\\s") )
      .map( _.toLowerCase.replaceAll("[^a-z]", ""))
      .filter(_.nonEmpty)
      .map( WordWithCount(_, 1))
      .keyBy("word")
      .sum("count")

    // print the results with a single thread, rather than in parallel as csv
    windowCounts.writeAsCsv(output).setParallelism(1)

    env.execute("Socket Window WordCount")
  }

  // Data type for words with count
  case class WordWithCount(word: String, count: Long)
}