/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.codelionx

import org.apache.flink.api.common.functions._
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem

import scala.collection.JavaConverters._

/**
  * This example implements a basic K-Means clustering algorithm.
  *
  * K-Means is an iterative clustering algorithm and works as follows:
  * K-Means is given a set of data points to be clustered and an initial set of ''K'' cluster
  * centers.
  * In each iteration, the algorithm computes the distance of each data point to each cluster center.
  * Each point is assigned to the cluster center which is closest to it.
  * Subsequently, each cluster center is moved to the center (''mean'') of all points that have
  * been assigned to it.
  * The moved cluster centers are fed into the next iteration.
  * The algorithm terminates after a fixed number of iterations (as in this implementation)
  * or if cluster centers do not (significantly) move in an iteration.
  * This is the Wikipedia entry for the [[http://en.wikipedia
  * .org/wiki/K-means_clustering K-Means Clustering algorithm]].
  *
  * This implementation works on two-dimensional data points.
  * It computes an assignment of data points to cluster centers, i.e.,
  * each data point is annotated with the id of the final cluster (center) it belongs to.
  *
  * Input files are plain text files and must be formatted as follows:
  *
  * - Data points are represented as two double values separated by a blank character.
  * Data points are separated by newline characters.
  * For example `"1.2 2.3\n5.3 7.2\n"` gives two data points (x=1.2, y=2.3) and (x=5.3,
  * y=7.2).
  * - Cluster centers are represented by an integer id and a point value.
  * For example `"1 6.2 3.2\n2 2.9 5.7\n"` gives two centers (id=1, x=6.2,
  * y=3.2) and (id=2, x=2.9, y=5.7).
  *
  * Usage:
  * {{{
  *   KMeans --points <path> --centroids <path> --output <path> --iterations <n>
  * }}}
  * If no parameters are provided, the program is run with default data from
  * [[org.apache.flink.examples.java.clustering.util.KMeansData]]
  * and 10 iterations.
  *
  * This example shows how to use:
  *
  * - Bulk iterations
  * - Broadcast variables in bulk iterations
  * - Scala case classes
  */
object CellCluster {

  def main(args: Array[String]) {

    // checking input parameters
    val params: ParameterTool = ParameterTool.fromArgs(args)

    val k: Int = try {
      params.getInt("k")
    } catch {
      case e: Throwable =>
        println(s"Could not parse argument k, because $e")
        -1
    }

    val mnc: Seq[Int] = {
      val mncString = params.get("mnc")
      if(mncString == null) {
        println("No mnv specified ... ignoring.")
        Seq.empty
      } else {
        try {
          mncString.split(",").map(_.toInt)
        } catch {
          case e: Throwable =>
            println(s"Could not parse argument mnc, because $e")
            Seq.empty
        }
      }
    }

    // set up execution environment
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // get input data:
    val cellData: DataSet[MobileCellData] = readCellData(params, env)

    val centroids: DataSet[Centroid] = {
      val data = cellData.filter(_.radio.equals("LTE"))
      if(k <= 0) data
      else       data.first(k)
    }.map( mobileCell => Centroid(mobileCell.cell, mobileCell.lon, mobileCell.lat) )

    val points: DataSet[Point] = {
      val data = cellData.filter(_.radio.matches("UMTS|GSM"))
      if(mnc.isEmpty) data
      else            data.filter(cell => mnc.contains(cell.net))
    }.map( mobileCell => Point(mobileCell.lon, mobileCell.lat))

    val finalCentroids = centroids.iterate(params.getInt("iterations", 10)) { currentCentroids =>
      val newCentroids = points
        .map(new SelectNearestCenter).withBroadcastSet(currentCentroids, "centroids")
        .map { x => (x._1, x._2, 1L) }.withForwardedFields("_1; _2")
        .groupBy(0)
        .reduce { (p1, p2) => (p1._1, p1._2 + p2._2, p1._3 + p2._3) }.withForwardedFields("_1")
        .map { x => new Centroid(x._1, x._2 / x._3) }.withForwardedFields("_1->id")
      newCentroids
    }

    val clusteredPoints: DataSet[(Int, Point)] =
      points.map(new SelectNearestCenter).withBroadcastSet(finalCentroids, "centroids")

    if (params.has("output")) {
      clusteredPoints
        .map( tuple => (tuple._1, tuple._2.x, tuple._2.y) )
        .writeAsCsv(params.get("output"), writeMode = FileSystem.WriteMode.OVERWRITE)
        .setParallelism(1)
      env.execute("Scala KMeans Example")
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      clusteredPoints.print()
    }

  }

  // *************************************************************************
  //     UTIL FUNCTIONS
  // *************************************************************************

  def readCellData(params: ParameterTool, env: ExecutionEnvironment): DataSet[MobileCellData] = {
    if(params.has("input")) {
      val cellDataTuple = env.readCsvFile[Tuple14[String, String, String, String, String, String, String, String, String, String, String, String, String, String]](params.get("input"), ignoreFirstLine = true)
      cellDataTuple.map( tuple => {
        try {
          MobileCellData(
            radio = tuple._1,
            cell = tuple._5.toInt,
            net = tuple._3.toInt,
            lon = tuple._7.toDouble,
            lat = tuple._8.toDouble
          )
        } catch {
          case e: NumberFormatException =>
            println(
              s"""Could not parse $tuple
                 |Reason:
                 |$e
               """.stripMargin)
            throw e
        }
      })
    } else {
      throw new RuntimeException("No input file was specified")
    }
  }

  def getCentroidDataSet(params: ParameterTool, env: ExecutionEnvironment): DataSet[Centroid] = {
    if (params.has("centroids")) {
      env.readCsvFile[Centroid](
        params.get("centroids"),
        fieldDelimiter = " ",
        includedFields = Array(0, 1, 2))
    } else {
      println("Executing K-Means example with default centroid data set.")
      println("Use --centroids to specify file input.")
      env.fromCollection(KMeansData.CENTROIDS map {
        case Array(id, x, y) =>
          Centroid(id.asInstanceOf[Int], x.asInstanceOf[Double], y.asInstanceOf[Double])
      })
    }
  }

  def getPointDataSet(params: ParameterTool, env: ExecutionEnvironment): DataSet[Point] = {
    if (params.has("points")) {
      env.readCsvFile[Point](
        params.get("points"),
        fieldDelimiter = " ",
        includedFields = Array(0, 1))
    } else {
      println("Executing K-Means example with default points data set.")
      println("Use --points to specify file input.")
      env.fromCollection(KMeansData.POINTS map {
        case Array(x, y) => Point(x.asInstanceOf[Double], y.asInstanceOf[Double])
      })
    }
  }

  // *************************************************************************
  //     DATA TYPES
  // *************************************************************************

  case class MobileCellData(radio: String, cell: Int, net: Int, lon: Double, lat: Double)

  /**
    * Common trait for operations supported by both points and centroids
    * Note: case class inheritance is not allowed in Scala
    */
  trait Coordinate extends Serializable {

    var x: Double
    var y: Double

    def +(other: Coordinate): this.type = add(other)
    def add(other: Coordinate): this.type = {
      x += other.x
      y += other.y
      this
    }

    def /(other: Long): this.type = div(other)
    def div(other: Long): this.type = {
      x /= other
      y /= other
      this
    }

    def euclideanDistance(other: Coordinate): Double =
      Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y))

    def clear(): Unit = {
      x = 0
      y = 0
    }

    override def toString: String = s"$x $y"
  }

  /**
    * A simple two-dimensional point.
    */
  case class Point(override var x: Double = 0, override var y: Double = 0) extends Coordinate

  /**
    * A simple two-dimensional centroid, basically a point with an ID.
    */
  case class Centroid(var id: Int = 0, override var x: Double = 0, override var y: Double = 0) extends Coordinate {

    def this(id: Int, p: Point) {
      this(id, p.x, p.y)
    }

    override def toString: String =
      s"$id ${super.toString}"

  }

  /** Determines the closest cluster center for a data point. */
  @ForwardedFields(Array("*->_2"))
  final class SelectNearestCenter extends RichMapFunction[Point, (Int, Point)] {
    private var centroids: Traversable[Centroid] = null

    /** Reads the centroid values from a broadcast variable into a collection. */
    override def open(parameters: Configuration) {
      centroids = getRuntimeContext.getBroadcastVariable[Centroid]("centroids").asScala
    }

    def map(p: Point): (Int, Point) = {
      var minDistance: Double = Double.MaxValue
      var closestCentroidId: Int = -1
      for (centroid <- centroids) {
        val distance = p.euclideanDistance(centroid)
        if (distance < minDistance) {
          minDistance = distance
          closestCentroidId = centroid.id
        }
      }
      (closestCentroidId, p)
    }

  }
}

object KMeansData { // We have the data as object arrays so that we can also generate Scala Data Sources from it.
  val CENTROIDS: Array[Array[Any]] = Array[Array[Any]](Array[Any](1, -31.85, -44.77), Array[Any](2, 35.16, 17.46), Array[Any](3, -5.16, 21.93), Array[Any](4, -24.06, 6.81))
  val POINTS: Array[Array[Any]] = Array[Array[Any]](Array[Any](-14.22, -48.01), Array[Any](-22.78, 37.10), Array[Any](56.18, -42.99), Array[Any](35.04, 50.29), Array[Any](-9.53, -46.26), Array[Any](-34.35, 48.25), Array[Any](55.82, -57.49), Array[Any](21.03, 54.64), Array[Any](-13.63, -42.26), Array[Any](-36.57, 32.63), Array[Any](50.65, -52.40), Array[Any](24.48, 34.04), Array[Any](-2.69, -36.02), Array[Any](-38.80, 36.58), Array[Any](24.00, -53.74), Array[Any](32.41, 24.96), Array[Any](-4.32, -56.92), Array[Any](-22.68, 29.42), Array[Any](59.02, -39.56), Array[Any](24.47, 45.07), Array[Any](5.23, -41.20), Array[Any](-23.00, 38.15), Array[Any](44.55, -51.50), Array[Any](14.62, 59.06), Array[Any](7.41, -56.05), Array[Any](-26.63, 28.97), Array[Any](47.37, -44.72), Array[Any](29.07, 51.06), Array[Any](0.59, -31.89), Array[Any](-39.09, 20.78), Array[Any](42.97, -48.98), Array[Any](34.36, 49.08), Array[Any](-21.91, -49.01), Array[Any](-46.68, 46.04), Array[Any](48.52, -43.67), Array[Any](30.05, 49.25), Array[Any](4.03, -43.56), Array[Any](-37.85, 41.72), Array[Any](38.24, -48.32), Array[Any](20.83, 57.85))
}
