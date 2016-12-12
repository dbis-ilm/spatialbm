import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.api.java.JavaSparkContext.fromSparkContext
import org.datasyslab.geospark.spatialOperator.DistanceJoin
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialOperator.KNNQuery
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geospark.spatialRDD.PointRDD
import org.datasyslab.geospark.spatialRDD.PolygonRDD

import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geom.Point
import com.vividsolutions.jts.geom.Polygon
import com.vividsolutions.jts.io.WKTReader

import Partitioner._

object geosparkedbtqsize {

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

  //  val persistPointsPath = "/data/edbt2017/stark_persist_points"
  //  val persistPoliesPath = "/data/edbt2017/stark_persist_polies"

   val doFilters = true
   val doJoins = false

   val nonPersistent = true
   val persistent = false

   val testPartis = Array(Partitioner.NONE, Partitioner.GRID, Partitioner.VORONOI, Partitioner.HILBERT, Partitioner.RTREE)
   val testIdxs = Array(IndexType.NONE, IndexType.LIVE)


   val polyFile  = "/data/spatial_synth/polygon_1000000.wkt" //polygon_50000000.wkt"
   val pointFile = "/data/spatial_synth/point_10000000.wkt" //point_50000000.wkt"

   val queryPoi = new WKTReader().read("POINT (-29.651041760751582 26.961568803715153)").getEnvelopeInternal
    val queryPolies = Array(
      new WKTReader().read("POLYGON((13.3 40.2, 14.3 40.2, 14.3 41.2, 13.3 41.2, 13.3 40.2))").getEnvelopeInternal,
      new WKTReader().read("POLYGON((13.3 40.2, 18.3 40.2, 18.3 45.2, 13.3 48.2, 13.3 40.2))").getEnvelopeInternal,
      new WKTReader().read("POLYGON((13.3 40.2, 23.3 40.2, 23.3 50.2, 13.3 50.2, 13.3 40.2))").getEnvelopeInternal,
      new WKTReader().read("POLYGON((13.3 40.2, 63.3 40.2, 63.3 90.2, 13.3 90.2, 13.3 40.2))").getEnvelopeInternal,
      new WKTReader().read("POLYGON((13.3 40.2, 113.3 40.2, 113.3 140.2, 13.3 140.2, 13.3 40.2))").getEnvelopeInternal,
      new WKTReader().read("POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))").getEnvelopeInternal
    )

   val offset = 1

   def load(sc: SparkContext, file: String, offset: Int, partitioner: Partitioner, numPart: Int, isPoint: Boolean = true) = {
     val gridType = partitioner match {
       case Partitioner.NONE    => ""
       case Partitioner.GRID    => "equalgrid"
       case Partitioner.VORONOI => "voronoi"
       case Partitioner.HILBERT => "hilbert"
       case Partitioner.RTREE   => "rtree"
     }
     if (partitioner == Partitioner.NONE) {
       if (isPoint)
         new PointRDD(sc, file, offset, "wkt")
       else
         new PolygonRDD(sc, file, offset, "wkt")
     } else {
       if (isPoint)
         new PointRDD(sc, file, offset, "wkt", gridType, numPart)
       else
         new PolygonRDD(sc, file, offset, "wkt", gridType, numPart)
     }
   }

  def main(args: Array[String]) {

    val geosparkStats = new StatsCollector(Platform.GEOSPARK)
    try {

      val conf = new SparkConf().setAppName(s"geospark_edbt").set("spark.ui.showConsoleProgress", showProgress)
      val sc = new SparkContext(conf)

      for (parti <- testPartis) {
        println(s"${parti.toString}")
        def partedPoints = load(sc, pointFile, offset, parti, partitions).asInstanceOf[PointRDD]
        def partedPolies = load(sc, polyFile, offset, parti, partitions, false).asInstanceOf[PolygonRDD]
        var numPoly = 0
        for(queryPoly <- queryPolies) {
          println(s"num querypoly $numPoly")
          numPoly += 1
        for (i <- 0 until numRuns) {
          println(s"\trun $i")
          if (doFilters) {

            /*************FILTER CONTAINS*********************************************************************************/
            println(s"\t\tfilter contains point-poly-$numPoly")
            geosparkStats.timing(OP.FILTER, parti, IndexType.NONE, Predicate.CONTAINEDBY, extra = s"point-poly-$numPoly") { //Not supported - Point query for Polygon RDD (manually added)
              RangeQuery.SpatialRangeQuery(partedPoints, queryPoly, 1).getRawPointRDD.count()
            }
            if (parti == Partitioner.NONE) {
              println(s"\t\tfilter contains w/ Index")
              val indexedRddC = load(sc, pointFile, offset, parti, partitions).asInstanceOf[PointRDD]
              geosparkStats.timing(OP.FILTER, parti, IndexType.LIVE, Predicate.CONTAINEDBY, extra = s"point-poly-$numPoly") {
                indexedRddC.buildIndex("rtree")
                RangeQuery.SpatialRangeQueryUsingIndex(indexedRddC, queryPoly, 1).getRawPointRDD.count()
              }
            } else {
              geosparkStats.timing(OP.FILTER, parti, IndexType.LIVE, Predicate.CONTAINEDBY, extra = s"point-poly-$numPoly")(-1L)
            }
            // geosparkStats.timing(OP.FILTER, parti, IndexType.PERSISTENT, Predicate.CONTAINS, extra = "poly-point")(-1L)
          } // FILTER ON/OFF

          if (doJoins) {
            if (parti != Partitioner.NONE) {
              println(s"\t\tselfjoin contains point point")
              geosparkStats.timing(OP.JOIN,parti, IndexType.NONE, Predicate.CONTAINS, extra = "self-point-point"){
                DistanceJoin.SpatialJoinQueryWithoutIndex(sc, partedPoints, partedPoints, 0.0).count()
              }

              println(s"\t\tselfjoin contains w/ Index")
              val indexedRddC2 = load(sc, pointFile, offset, parti, partitions).asInstanceOf[PointRDD]
              geosparkStats.timing(OP.JOIN, parti, IndexType.LIVE, Predicate.CONTAINS, extra = "self-point-point") {
                indexedRddC2.buildIndex("rtree")
                DistanceJoin.SpatialJoinQueryUsingIndex(sc, indexedRddC2, partedPoints, 0.0).count()
              }
            } else {
              println(s"\t\tjoin contains (not possible)")
              geosparkStats.timing(OP.JOIN, parti, IndexType.NONE, Predicate.CONTAINS, extra = "self-point-point")(-1L)
              geosparkStats.timing(OP.JOIN, parti, IndexType.LIVE, Predicate.CONTAINS, extra = "self-point-point")(-1L)
            }
          } //JOIN ON/OFF
        } // Loop Runs
        } // Loop numPoly
      } // Loop Partitioner

      println(geosparkStats.evaluate)
      sc.stop()
    } catch {
      case e: Throwable =>
        println(geosparkStats.evaluate)
        println(e.getMessage)
        e.printStackTrace(System.err)
    }
  }
}
