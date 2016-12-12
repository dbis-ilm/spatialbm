import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

import dbis.stark.STObject
import dbis.stark.spatial._
import dbis.stark.spatial.JoinPredicate
import dbis.stark.spatial.plain.PlainSpatialRDDFunctions
import dbis.stark.spatial.SpatialRDD._


import Platform._
import Predicate._
import IndexType._
import Partitioner._
import OP._

object starkrunnerqsize {

  val showProgress = "true" // show Spark's progress bar (makes log file look ugly)

  val numRuns = 5

  private val cellSize = 2
  private val cellSizePoly = 2

  private val maxCostPoints = 100 * 1000
  private val maxCostPolies = 10 * 1000

  private val ppD = 90

  private val treeOrderPoints = 10
  private val treeOrderPolies = 10


  val initialPartitions = 32
  val bsp_threshold = 32


  val doFilters = true

  val nonPersistent = true

  val testPartis = Array(Partitioner.NONE, Partitioner.GRID, Partitioner.BSP)
  val testIdxs = Array(IndexType.NONE, IndexType.LIVE)


  val pointFile =   "/data/spatial_synth/point_50000000.wkt"
  
  def loadRDD(sc: SparkContext, fName: String) =  sc.textFile(fName,initialPartitions)
  .map { line => line.split(';') }
  .map { arr => (STObject(arr(1)), arr(0)) }


  val queryPoint = STObject("POINT (-29.651041760751582 26.961568803715153)")
  val queryPolies = Array(
    STObject("POLYGON((13.3 40.2, 14.3 40.2, 14.3 41.2, 13.3 41.2, 13.3 40.2))"),
    STObject("POLYGON((13.3 40.2, 18.3 40.2, 18.3 48.2, 13.3 48.2, 13.3 40.2))"),
    STObject("POLYGON((13.3 40.2, 23.3 40.2, 23.3 50.2, 13.3 50.2, 13.3 40.2))"),
    STObject("POLYGON((13.3 40.2, 63.3 40.2, 63.3 90.2, 13.3 90.2, 13.3 40.2))"),
    STObject("POLYGON((13.3 40.2, 113.3 40.2, 113.3 140.2, 13.3 140.2, 13.3 40.2))"),
    STObject("POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))")
  )


  def extent(rdd: RDD[(STObject, String)]) = {
    val extentPoints = rdd.map(_._1.getGeo.getEnvelopeInternal).map(x => (x.getMinX, x.getMinY, x.getMaxX, x.getMaxY)).reduce((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4))
    val pointMinMax = (extentPoints._1, extentPoints._3 + 1, extentPoints._2, extentPoints._4 + 1)
    pointMinMax
  }

  def main(args: Array[String]) {

    val starkStats = new StatsCollector(Platform.STARK)
    try {

      val conf = new SparkConf().setAppName(s"stark_edbt").set("spark.ui.showConsoleProgress", showProgress)
      val sc = new SparkContext(conf)

      val points = loadRDD(sc, pointFile)

      val pointMinMax = extent(points)

      for(parti <- testPartis) {
        println(s"parti: ${parti.toString}")

        for(idxType <- testIdxs) {
          println(s"\tIdxType: ${idxType.toString}")
          var numPoly = 0
          for(queryPoly <- queryPolies) {
            println(s"num querypoly $numPoly")
            numPoly += 1
            for(i <- 0 until numRuns) {
              println(s"\t\trun $i")

              val (partedPoints, pointsParti) = parti match {
                case Partitioner.NONE => (points, None)
                case Partitioner.BSP =>
                BSPartitioner.numCellThreshold = bsp_threshold

                val pointsParti = new BSPartitioner(points, cellSize, maxCostPoints, withExtent = false, minMax = pointMinMax)
                (points.partitionBy(pointsParti),
                Some(pointsParti) )

                case Partitioner.GRID =>
                val pointsParti = new SpatialGridPartitioner(points, ppD, withExtent = false, minMax = pointMinMax, dimensions = 2)

                (points.partitionBy(pointsParti),
                Some(pointsParti))

              }

              val theRddPoints = idxType match {
                case IndexType.NONE =>
                /* wrap into function object, because live returns also a function object
                * maybe we can improve the STARK API
                */
                new PlainSpatialRDDFunctions(partedPoints)
                case IndexType.LIVE =>
                partedPoints.liveIndex(pointsParti,treeOrderPoints)
                // PersistantFunctions does not implement tha SpatialFunctionsRDD, we need to handle it separately
                // case IndexType.PERSISTENT => partedRdd.index(10000, 1, 10)
              }


              //----------------------------------------------------------------------------------------------------------
              // STARK FILTERS

              println(s"\t\tfilter contains point-poly-$numPoly")
              starkStats.timing(OP.FILTER,parti, idxType,Predicate.CONTAINEDBY, extra = s"point-poly-$numPoly") {
                theRddPoints.containedby(queryPoly).count()
              }

            }
          }
        }
      }

      println(starkStats.evaluate)


      sc.stop()


    } catch {
      case e: Throwable =>
      println(starkStats.evaluate)
      println(e.getMessage)
      e.printStackTrace(System.err)


    }

  }
}
