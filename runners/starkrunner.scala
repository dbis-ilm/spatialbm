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

object starkrunner {

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
  val doJoins = true

  val nonPersistent = true
  val persistent = false

  val testPartis = Array(Partitioner.NONE, Partitioner.GRID, Partitioner.BSP)
  val testIdxs = Array(IndexType.NONE, IndexType.LIVE)


  val polyFile  = "/data/spatial_synth/polygon_1000000.wkt"
  val pointFile =   "/data/spatial_synth/point_1000000.wkt"

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

    val points = sc.parallelize(Array((STObject(queryPoint), "")))//loadRDD(sc, pointFile)
    val polies = loadRDD(sc, polyFile)

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

            println(s"\t\tfilter contains poly-point")
            starkStats.timing(OP.FILTER,parti, idxType,Predicate.CONTAINS, extra = "poly-point") {
              // -1
              theRddPolies.contains(queryPoint).count()
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

            // if(idxType == IndexType.NONE) {
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
            // } else {
            //     starkStats.timing(OP.JOIN,parti, idxType,Predicate.WITHINDISTANCE, extra = "self-point-point")(-1)
            //     starkStats.timing(OP.JOIN,parti, idxType,Predicate.CONTAINS, extra = "poly-point")(-1)
            //     starkStats.timing(OP.JOIN,parti, idxType,Predicate.INTERSECTS, extra = "poly-poly-block-world")(-1)
            //     starkStats.timing(OP.JOIN,parti, idxType,Predicate.INTERSECTS, extra = "poly-poly-world-block") (-1)
            // }
          }
        }
      }
    }

    println(starkStats.evaluate)

  }
  sc.stop()
    //----------------------------------------------------------------------------------------------------------
    // PERSISTENT INDEX

    if(persistent) {

      println(" PERSISTENT")

      val conf2 = new SparkConf().setAppName(s"peristent").set("spark.ui.showConsoleProgress", showProgress)
      val sc2 = new SparkContext(conf2)

      // import org.apache.hadoop.fs.FileSystem
      // import org.apache.hadoop.fs.Path
      // val fs: FileSystem = FileSystem.get(new java.net.URI(persistPointsPath), sc2.hadoopConfiguration);

      val points2 = loadRDD(sc2, pointFile)
      val polies2 = loadRDD(sc2,  polyFile)

      println(" creating partitioners")
      BSPartitioner.numCellThreshold = bsp_threshold
      val pointsParti = new BSPartitioner(points2, cellSize, maxCostPoints, withExtent = false, minMax = pointMinMax)
      val polyParti = new BSPartitioner(polies2, cellSizePoly, maxCostPolies, withExtent = true, minMax = polyMinMax)

      // val pointsParti = new SpatialGridPartitioner(points2, ppD, withExtent = false, minMax = pointMinMax, dimensions = 2)
      // val polyParti = new SpatialGridPartitioner(polies2, ppD, withExtent = true, minMax = polyMinMax, dimensions = 2)


      println(" partitioning")

      val points2parted = points2.partitionBy(pointsParti)
      val polies2parted = polies2.partitionBy(polyParti)

      // val pointsPartitions = points2parted.getNumPartitions
      // val poliesPartitions = polies2parted.getNumPartitions

      println(" indexing")

      val pointsIdx = points2.index(pointsParti, treeOrderPoints)
      val polyIdx = polies2.index(polyParti, treeOrderPolies)

      println(" counting")

      // dummy counts
      // val pointsC = pointsIdx.count()
      // val polyC = polyIdx.count()
      // println(s"points count: $pointsC")
      // println(s"polies count: $polyC")

      // if(fs.exists(new Path(persistPointsPath))) {
      //   fs.delete(new Path(persistPointsPath), true)
      //   fs.delete(new Path(persistPoliesPath), true)
      // }

      // println(" [`date`] writing to disk")
      //
      // val writeStart = System.currentTimeMillis
      // pointsIdx.saveAsObjectFile(persistPointsPath)
      // val write1 = System.currentTimeMillis
      // polyIdx.saveAsObjectFile(persistPoliesPath)
      // val writeEnd = System.currentTimeMillis


      // println(s"  writing points took: ${write1 - writeStart}")
      // println(s"  writing polies took: ${writeEnd - write1}")

      // def loadPolyIdx : RDD[dbis.stark.spatial.indexed.RTree[STObject, (STObject, String) ]] = sc2.objectFile(persistPoliesPath, poliesPartitions)
      // def loadPointIdx : RDD[dbis.stark.spatial.indexed.RTree[STObject, (STObject, String) ]] = sc2.objectFile(persistPointsPath, pointsPartitions)

      val idxType = IndexType.PERSISTENT

      for(i <- 0 until numRuns) {
        println(s"    run $i")

        //----------------------------------------------------------------------------------------------------------
        // FILTERS
        if(doFilters) {
          println(s"\t\tfilter contains poly-point")
          starkStats.timing(OP.FILTER,Partitioner.BSP, IndexType.PERSISTENT,Predicate.CONTAINS, extra = "poly-point") {
            polyIdx.contains(queryPoint).count()
          }

          println("\t\tfiler intersects poly-poly")
          starkStats.timing(OP.FILTER,Partitioner.BSP, IndexType.PERSISTENT,Predicate.INTERSECTS, extra="poly-poly") {
            polyIdx.intersects(queryPoly).count()
          }

          println(s"\t\tfilter withindistance")
          starkStats.timing(OP.FILTER,Partitioner.BSP, IndexType.PERSISTENT,Predicate.WITHINDISTANCE, extra = "point-point") {
            pointsIdx.withinDistance(queryPoint, 0.005, (g1,g2) => g1.getGeo.distance(g2.getGeo) ).count()
          }

          // ---------- RO ----------------
          println(s"\t\tfilter contains poly-point")
          starkStats.timing(OP.FILTER,Partitioner.BSP, IndexType.PERSISTENT,Predicate.CONTAINS, extra = "poly-point-ro") {
            polyIdx.containsRO(queryPoint).flatten.count()
          }

          println("\t\tfiler intersects poly-poly")
          starkStats.timing(OP.FILTER,Partitioner.BSP, IndexType.PERSISTENT,Predicate.INTERSECTS, extra="poly-poly-ro") {
            polyIdx.intersectsRO(queryPoly).flatten.count()
          }

          println(s"\t\tfilter withindistance")
          starkStats.timing(OP.FILTER,Partitioner.BSP, IndexType.PERSISTENT,Predicate.WITHINDISTANCE, extra = "point-point-ro") {
            pointsIdx.withinDistanceRO(queryPoint, 0.005, (g1,g2) => g1.getGeo.distance(g2.getGeo) ).flatten.count()
          }

          println(starkStats.evaluate)

        }

        //----------------------------------------------------------------------------------------------------------
        // JOINS

        if(doJoins) {

          // println("\t\tselfjoin withinDistance points points")
          // starkStats.timing(OP.JOIN,Partitioner.BSP, idxType,Predicate.WITHINDISTANCE, extra = "self-point-point") {
          //   sampleIdx.join(pointsSample2parted, Predicates.withinDistance(0.005, (g1,g2) => g1.getGeo.distance(g2.getGeo)) _).count()
          // }
          //
          // println(s"\t\tjoin contains poly point")
          // starkStats.timing(OP.JOIN,Partitioner.BSP, idxType,Predicate.CONTAINS, extra = "poly-point") {
          //   blockIdx.join(pointsSample2parted, JoinPredicate.CONTAINS, None).count()
          // }
          //
          // println(s"\t\tjoin intersects poly poly block-world")
          // starkStats.timing(OP.JOIN,Partitioner.BSP, idxType,Predicate.INTERSECTS, extra = "poly-poly-block-world") {
          //   blockIdx.join(polies2parted, JoinPredicate.INTERSECTS, None).count()
          // }
          //
          // println(s"\t\tjoin intersects poly poly world-block")
          // starkStats.timing(OP.JOIN,Partitioner.BSP, idxType,Predicate.INTERSECTS, extra = "poly-poly-world-block") {
          //   loadPolyIdx.join(block2parted, JoinPredicate.INTERSECTS, None).count()
          // }
        }


      }

      sc2.stop()
    }
    println(starkStats.evaluate)

  } catch {
    case e: Throwable =>
      println(starkStats.evaluate)
      println(e.getMessage)
      e.printStackTrace(System.err)


  }

  }
}
