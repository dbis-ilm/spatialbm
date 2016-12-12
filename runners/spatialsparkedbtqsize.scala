import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

import spatialspark.query.RangeQuery
import spatialspark.operator.SpatialOperator

import com.vividsolutions.jts.io.WKTReader

import Platform._
import Predicate._
import IndexType._
import Partitioner._
import OP._
import spatialspark.index.STIndex
import spatialspark.util.MBR
import spatialspark.index.IndexConf
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI
import spatialspark.join.BroadcastSpatialJoin
import spatialspark.partition.stp.SortTilePartitionConf
import spatialspark.partition.bsp.BinarySplitPartitionConf
import spatialspark.partition.fgp.FixedGridPartitionConf
import spatialspark.join.PartitionedSpatialJoin

object spatialsparkedbtqsize {
  val showProgress = "true" // show Spark's progress bar (makes log file look ugly)

  val numRuns = 5

  private val cellSize = 2
  private val cellSizePoly = 2

  private val maxCostPoints = 100 * 1000
  private val maxCostPolies = 10 * 1000

  private val ppD = 90
  private val partitions = ppD * ppD

  private val treeOrderPoints = 10
  private val treeOrderPolies = 10

  val initialPartitions = 32
  val bsp_threshold = 32

  val ratio = 0.0001
  val levels = 10
  val parallel = true

  //  val persistPointsPath = "/data/edbt2017/stark_persist_points"
  //  val persistPoliesPath = "/data/edbt2017/stark_persist_polies"

  val doFilters = true
  val doJoins = false

  val nonPersistent = true
  val persistent = false

  val testPartis = Array(Partitioner.NONE, Partitioner.GRID, Partitioner.BSP, Partitioner.TILE)
  val testIdxs = Array(IndexType.NONE, IndexType.PERSISTENT)

  val pointFile = "/data/spatial_synth/point_50000000.wkt"

  val queryPolies = Array(
    new WKTReader().read("POLYGON((13.3 40.2, 14.3 40.2, 14.3 41.2, 13.3 41.2, 13.3 40.2))"),
    new WKTReader().read("POLYGON((13.3 40.2, 18.3 40.2, 18.3 48.2, 13.3 48.2, 13.3 40.2))"),
    new WKTReader().read("POLYGON((13.3 40.2, 23.3 40.2, 23.3 50.2, 13.3 50.2, 13.3 40.2))"),
    new WKTReader().read("POLYGON((13.3 40.2, 63.3 40.2, 63.3 90.2, 13.3 90.2, 13.3 40.2))"),
    new WKTReader().read("POLYGON((13.3 40.2, 113.3 40.2, 113.3 140.2, 13.3 140.2, 13.3 40.2))"),
    new WKTReader().read("POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))")
  )
  val offset = 1

  def main(args: Array[String]) {
    val spatialsparkStats = new StatsCollector(Platform.SPATIALSPARK)

    try {
      val conf = new SparkConf().setAppName("spatialspark_qsize")
      val sc = new SparkContext(conf)

      def pointRDD(sc: SparkContext) = sc.textFile(pointFile)
        .map(_.split(";"))
        .map { arr => (new WKTReader().read(arr(offset))) }
        .zipWithIndex().map(_.swap)

      for (parti <- testPartis) {
        println(s"${parti.toString}")
        
        def partedPoints = pointRDD(sc) //.cache()
        var numPoly = 0
        for(queryPoly <- queryPolies) {
          println(s"num querypoly $numPoly")
          numPoly += 1
          for (i <- 0 until numRuns) {
            println(s"\trun $i")

            /*************FILTER CONTAINS*********************************************************************************/
            if (doFilters) {
              if (parti == Partitioner.NONE) {
                println(s"\t\tfilter contains point-poly-$numPoly")
                spatialsparkStats.timing(OP.FILTER, parti, IndexType.NONE, Predicate.CONTAINEDBY, extra = s"point-poly-$numPoly") {
                  RangeQuery(sc, partedPoints, queryPoly, SpatialOperator.Within).count()
                }
              } else {
                println(s"\t\tfilter contains (not possible)")
                spatialsparkStats.timing(OP.FILTER, parti, IndexType.NONE, Predicate.CONTAINEDBY, extra = s"point-poly-$numPoly")(-1L)
              }

              println(s"\t\tfilter contains w/ Index (not possible)")
              spatialsparkStats.timing(OP.FILTER, parti, IndexType.LIVE, Predicate.CONTAINEDBY, extra = s"point-poly-$numPoly")(-1L)
            }

          } // Loop Runs
        } // Loop numPoly
      } // Loop Partitioner
      println(spatialsparkStats.evaluate)
      sc.stop()
    } catch {
      case e: Throwable =>
        println(spatialsparkStats.evaluate)
        println(e.getMessage)
        e.printStackTrace(System.err)
    }
  }
}
