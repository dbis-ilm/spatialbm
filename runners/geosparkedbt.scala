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

object geosparkedbt {

  val showProgress = "true" // show Spark's progress bar (makes log file look ugly)

   val numRuns = 3

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
   val queryPoly = new WKTReader().read("POLYGON((45.67050858961777 63.71836235997347,45.73714552639256 63.710784635707995,59.36758739339917 64.46718036194419,59.37605569764713 64.48743337914999,59.382775091960156 64.51384113118765,59.3828739717612 64.51445990300579,59.382980860498414 64.51514850919028,59.38336308587107 64.51781221599693,59.38337194463155 64.51787835496746,59.38338039834879 64.5179416840974,59.38338991064831 64.51801319611424,59.38338997487478 64.51801367987522,59.38338999223398 64.51801381062877,59.38338999316217 64.51801381762013,59.383389993757405 64.51801382210358,59.38338999377597 64.51801382224343,59.383389994124876 64.5180138248715,59.3833899953314 64.51801383395932,59.383389995473834 64.51801383503222,59.3833899957503 64.51801383711461,59.383389995755216 64.51801383715167,59.38338999577108 64.51801383727114,59.38338999578087 64.51801383734485,59.38338999578088 64.51801383734498,59.38338999578929 64.51801383740828,59.38338999579331 64.51801383743859,45.67050858961777 63.71836235997347))")
    .getEnvelopeInternal

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

        for (i <- 0 until numRuns) {
          println(s"\trun $i")
          if (doFilters) {

/*************FILTER CONTAINS*********************************************************************************/
            println(s"\t\tfilter contains")
            geosparkStats.timing(OP.FILTER, parti, IndexType.NONE, Predicate.CONTAINS, extra = "poly-point") { //Not supported - Point query for Polygon RDD (manually added)
              RangeQuery.SpatialRangeQuery(partedPolies, queryPoi, 0).getRawPolygonRDD.count()
            }
            if (parti == Partitioner.NONE) {
              println(s"\t\tfilter contains w/ Index")
              val indexedRddC = load(sc, polyFile, offset, parti, partitions, false).asInstanceOf[PolygonRDD]
              geosparkStats.timing(OP.FILTER, parti, IndexType.LIVE, Predicate.CONTAINS, extra = "poly-point") {
                indexedRddC.buildIndex("rtree")
                RangeQuery.SpatialRangeQueryUsingIndex(indexedRddC, queryPoi, 0).getRawPolygonRDD.count()
              }
            } else {
              println(s"\t\tfilter contains w/ Index (not possible)")
              geosparkStats.timing(OP.FILTER, parti, IndexType.LIVE, Predicate.CONTAINS, extra = "poly-point")(-1L)
            }
          }
        }
      }

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
