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

object starkrunner2 {

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

  val doFilters = false
  val doJoins = true

  val nonPersistent = true
  val persistent = false

  val testPartis = Array(Partitioner.NONE, Partitioner.GRID, Partitioner.BSP)
  val testIdxs = Array(IndexType.NONE, IndexType.LIVE)


  val polyFile  = "/data/spatial_synth/polygon_10000000.wkt"//polygon_50000000.wkt"
  val pointFile =   "/data/spatial_synth/point_1000000.wkt"//50000000.wkt"

  def loadRDD(sc: SparkContext, fName: String) =  sc.textFile(fName,initialPartitions)
  .map { line => line.split(';') }
  .map { arr => (STObject(arr(1)), arr(0)) }


  val queryPoint = STObject("POINT (-29.651041760751582 26.961568803715153)")
  val queryPoly = STObject("POLYGON((45.67050858961777 63.71836235997347,45.73714552639256 63.710784635707995,59.36758739339917 64.46718036194419,59.37605569764713 64.48743337914999,59.382775091960156 64.51384113118765,59.3828739717612 64.51445990300579,59.382980860498414 64.51514850919028,59.38336308587107 64.51781221599693,59.38337194463155 64.51787835496746,59.38338039834879 64.5179416840974,59.38338991064831 64.51801319611424,59.38338997487478 64.51801367987522,59.38338999223398 64.51801381062877,59.38338999316217 64.51801381762013,59.383389993757405 64.51801382210358,59.38338999377597 64.51801382224343,59.383389994124876 64.5180138248715,59.3833899953314 64.51801383395932,59.383389995473834 64.51801383503222,59.3833899957503 64.51801383711461,59.383389995755216 64.51801383715167,59.38338999577108 64.51801383727114,59.38338999578087 64.51801383734485,59.38338999578088 64.51801383734498,59.38338999578929 64.51801383740828,59.38338999579331 64.51801383743859,45.67050858961777 63.71836235997347))")

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

      val points =  loadRDD(sc, pointFile)
      val polies = loadRDD(sc, polyFile)

      val pointMinMax = extent(points)
      val polyMinMax = extent(polies)


      if(nonPersistent){
        for(parti <- testPartis) {
          println(s"parti: ${parti.toString}")

          for(idxType <- testIdxs) {
            println(s"\tIdxType: ${idxType.toString}")

            for(i <- 0 until numRuns) {
              println(s"\t\trun $i")

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

                println(s"\t\tfilter contains poly-point")
                starkStats.timing(OP.FILTER,parti, idxType,Predicate.CONTAINS, extra = "poly-point") {
                  // -1
                  theRddPolies.contains(queryPoint).count()
                }

              }

              //----------------------------------------------------------------------------------------------------------
              // STARK joins
              if (doJoins && parti != Partitioner.NONE) {

                println(s"\t\tselfjoin contains point point")
                starkStats.timing(OP.JOIN,parti, idxType,Predicate.CONTAINS, extra = "self-point-point") {
                  theRddPoints.join(partedPoints, JoinPredicate.CONTAINS, None).count()
                }
              }
            }
          }
        }
      }
      sc.stop()

    } catch {
      case e: Throwable =>

      println(e.getMessage)
      e.printStackTrace(System.err)

    } finally {
      println(starkStats.evaluate)
    }
  }
}
