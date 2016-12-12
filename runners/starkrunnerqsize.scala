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

  val persistPointsPath = "/data/edbt2017/stark_persist_points"
  val persistPoliesPath = "/data/edbt2017/stark_persist_polies"

  val doFilters = true
  val doJoins = false

  val nonPersistent = true
  val persistent = false

  val testPartis = Array(Partitioner.NONE, Partitioner.GRID, Partitioner.BSP)
  val testIdxs = Array(IndexType.NONE, IndexType.LIVE)


  val polyFile  = "/data/spatial_synth/polygon_50000000.wkt"
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
    val polies = sc.parallelize( Array(
      (STObject(queryPolies(0)), "test")))//loadRDD(sc, polyFile)

    val pointMinMax = extent(points)
    val polyMinMax = extent(polies)


    if(nonPersistent){
    for(parti <- testPartis) {
      println(s"parti: ${parti.toString}")

      val (partedPoints, partedPolies, pointsParti, polyParti) = parti match {
        case Partitioner.NONE => (points, polies, None, None)
        case Partitioner.BSP =>
          BSPartitioner.numCellThreshold = bsp_threshold

          val pointsParti = new BSPartitioner(points, cellSize, maxCostPoints, withExtent = false, minMax = pointMinMax)
          val polyParti = new BSPartitioner(polies, cellSizePoly, maxCostPolies, withExtent = true, minMax = polyMinMax)
          (points.partitionBy(pointsParti),
           polies.partitionBy(polyParti),
           Some(pointsParti),Some(polyParti) )

        case Partitioner.GRID =>
          val pointsParti = new SpatialGridPartitioner(points, ppD, withExtent = false, minMax = pointMinMax, dimensions = 2)
          val polyParti = new SpatialGridPartitioner(polies, ppD, withExtent = true, minMax = polyMinMax, dimensions = 2)

          (points.partitionBy(pointsParti),
           polies.partitionBy(polyParti),

           Some(pointsParti), Some(polyParti))

      }

      for(idxType <- testIdxs) {
        println(s"\tIdxType: ${idxType.toString}")
        var numPoly = 0
        for(queryPoly <- queryPolies) {
          println(s"num querypoly $numPoly")
          numPoly += 1
        for(i <- 0 until numRuns) {
          println(s"\t\trun $i")

          val (theRddPoints, theRddPolies) = idxType match {
            case IndexType.NONE =>
            /* wrap into function object, because live returns also a function object
             * maybe we can improve the STARK API
             */
              (new PlainSpatialRDDFunctions(partedPoints),
               new PlainSpatialRDDFunctions(partedPolies) )
            case IndexType.LIVE =>
              (partedPoints.liveIndex(pointsParti,treeOrderPoints),
               partedPolies.liveIndex(polyParti,treeOrderPolies) )
            // PersistantFunctions does not implement tha SpatialFunctionsRDD, we need to handle it separately
            // case IndexType.PERSISTENT => partedRdd.index(10000, 1, 10)
          }


          //----------------------------------------------------------------------------------------------------------
          // STARK FILTERS
          if(doFilters) {

            // if(parti != Partitioner.BSP) {
            //   println(s"\t\tfilter withindistance")
            //   starkStats.timing(OP.FILTER,parti, idxType,Predicate.WITHINDISTANCE, extra = "point-point") {
            //     theRddPoints.withinDistance(queryPoint, 0.05, (g1,g2) => g1.getGeo.distance(g2.getGeo) ).count()
            //   }
            // } else {
            //   starkStats.timing(OP.FILTER,parti, idxType,Predicate.WITHINDISTANCE, extra = "point-point")(-1)
            // }

            println(s"\t\tfilter contains poly-point-$numPoly")
            starkStats.timing(OP.FILTER,parti, idxType,Predicate.CONTAINS, extra = s"poly-point-$numPoly") {
              // -1
              theRddPoints.containedby(queryPoly).count()
            }

            // println(s"\t\tfilter containedby point-poly")
            // starkStats.timing(OP.FILTER,parti, idxType,Predicate.CONTAINEDBY, extra = "point-poly") {
            //   // -1
            //   theRddPoints.containedby(queryPoly).count()
            // }
            // println(s"\t\tfilter intersects poly-poly")
            // starkStats.timing(OP.FILTER,parti, idxType,Predicate.INTERSECTS, extra = "poly-poly") {
            //   // -1
            //   theRddPolies.intersects(queryPoly).count()
            // }

          } else {
            // starkStats.timing(OP.FILTER,parti, idxType,Predicate.WITHINDISTANCE, extra = "point-point")(-1)
            // starkStats.timing(OP.FILTER,parti, idxType,Predicate.CONTAINS, extra = "poly-point") (-1)
            // starkStats.timing(OP.FILTER,parti, idxType,Predicate.CONTAINEDBY, extra = "point-poly")(-1)
            // starkStats.timing(OP.FILTER,parti, idxType,Predicate.INTERSECTS, extra = "poly-poly")(-1)
          }

          //----------------------------------------------------------------------------------------------------------
          // STARK joins
          if (doJoins) { // && parti != Partitioner.NONE

            if(idxType == IndexType.NONE) {
            //
            //   println("\t\tselfjoin withinDistance points points")
            //   starkStats.timing(OP.JOIN,parti, idxType,Predicate.WITHINDISTANCE, extra = "self-point-point") {
            //     theRddSample.join(partedSamplePoints, Predicates.withinDistance(0.005, (g1,g2) => g1.getGeo.distance(g2.getGeo)) _).count()
            //   }
            //
              println(s"\t\tselfjoin contains point point")
              starkStats.timing(OP.JOIN,parti, idxType,Predicate.CONTAINS, extra = "self-point-point") {
                theRddPoints.join(partedPoints, JoinPredicate.CONTAINS, None).count()
              }
            //
            //   println(s"\t\tjoin intersects poly poly block-world")
            //   starkStats.timing(OP.JOIN,parti, idxType,Predicate.INTERSECTS, extra = "poly-poly-block-world") {
            //     theRddBlocks.join(partedPolies, JoinPredicate.INTERSECTS, None).count()
            //   }
            //
            //   println(s"\t\tjoin intersects poly poly world-block")
            //   starkStats.timing(OP.JOIN,parti, idxType,Predicate.INTERSECTS, extra = "poly-poly-world-block") {
            //     theRddPolies.join(partedBlocks, JoinPredicate.INTERSECTS, None).count()
            //   }
          } //else {
            //     starkStats.timing(OP.JOIN,parti, idxType,Predicate.WITHINDISTANCE, extra = "self-point-point")(-1)
            //     starkStats.timing(OP.JOIN,parti, idxType,Predicate.CONTAINS, extra = "poly-point")(-1)
            //     starkStats.timing(OP.JOIN,parti, idxType,Predicate.INTERSECTS, extra = "poly-poly-block-world")(-1)
            //     starkStats.timing(OP.JOIN,parti, idxType,Predicate.INTERSECTS, extra = "poly-poly-world-block") (-1)
            // }
          }
        }
        }
      }
    }

    println(starkStats.evaluate)

  }
  sc.stop()

    println(starkStats.evaluate)

  } catch {
    case e: Throwable =>
      println(starkStats.evaluate)
      println(e.getMessage)
      e.printStackTrace(System.err)


  }

  }
}
