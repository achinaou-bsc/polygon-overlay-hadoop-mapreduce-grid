package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid

import scala.io.Source
import scala.util.Using

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry

import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.live.PolygonOverlayGridMapperLive
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.live.PolygonOverlayGridReducerLive
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model.TaggedGeometryWritable
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.util.GeoJSON

class PolygonOverlayGrid

object PolygonOverlayGrid:

  private val jobType: String                      = "polygon-overlay"
  private val jobTypeQualifier: String             = "mapreduce-grid"
  private def jobName(referenceId: String): String = Array(jobType, jobTypeQualifier, referenceId).mkString("_")

  def main(args: Array[String]): Unit =
    val Array(referenceId) = args

    sys.exit:
      if job(referenceId).waitForCompletion(true)
      then 0
      else 1

  private def job(referenceId: String) =
    val workingDirectory: Path = Path(s"/jobs/$jobType", referenceId)
    val inputDirectory: Path   = Path(workingDirectory, "input")

    val baseLayer: Path    = Path(inputDirectory, "a.geojson")
    val overlayLayer: Path = Path(inputDirectory, "b.geojson")

    val output: Path = Path(workingDirectory, "output")

    val job: Job = Job.getInstance(configuration(baseLayer, overlayLayer), jobName(referenceId))

    FileInputFormat.addInputPath(job, baseLayer)
    FileInputFormat.addInputPath(job, overlayLayer)
    FileOutputFormat.setOutputPath(job, output)

    job.setJarByClass(classOf[PolygonOverlayGrid])

    job.setMapperClass(classOf[PolygonOverlayGridMapperLive])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[TaggedGeometryWritable])

    job.setReducerClass(classOf[PolygonOverlayGridReducerLive])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    job

  private def configuration(baseLayer: Path, overlayLayer: Path): Configuration =
    val configuration: Configuration = Configuration()

    configuration.set("baseLayer.path", baseLayer.toString)

    val gridMinimumBoundingRectangle: Envelope =
      val fileSystem: FileSystem = FileSystem.get(configuration)

      new Envelope:
        expandToInclude(getMinimumBoundingRectangle(fileSystem, baseLayer))
        expandToInclude(getMinimumBoundingRectangle(fileSystem, overlayLayer))

    configuration.setDouble("grid.mbr.minX", gridMinimumBoundingRectangle.getMinX)
    configuration.setDouble("grid.mbr.minY", gridMinimumBoundingRectangle.getMinY)
    configuration.setDouble("grid.mbr.maxX", gridMinimumBoundingRectangle.getMaxX)
    configuration.setDouble("grid.mbr.maxY", gridMinimumBoundingRectangle.getMaxY)

    val gridOrder: Int = getGridOrder(configuration)

    configuration.setInt("grid.order", gridOrder)

    configuration

  private def getMinimumBoundingRectangle(fileSystem: FileSystem, layerPath: Path): Envelope =
    calculateMinimumBoundingRectangle(parse(read(fileSystem, layerPath)))

  private def read(fileSystem: FileSystem, layerPath: Path): Array[String] =
    Using
      .Manager(use => use(Source.fromInputStream(use(fileSystem.open(layerPath)))).getLines.to(Array))
      .get

  private def parse(lines: Array[String]): Array[Geometry] =
    lines.map(GeoJSON.parseFeatureGeometry)

  private def calculateMinimumBoundingRectangle(geometries: Array[Geometry]): Envelope =
    geometries.foldLeft(Envelope()): (minimumBoundingRectangle, geometry) =>
      minimumBoundingRectangle.expandToInclude(geometry.getEnvelopeInternal)
      minimumBoundingRectangle

  /** Determines the side length (order) of the grid. The grid is intended to be large enough to map at least one cell
    * to each reduce task.
    *
    * @param configuration
    *   The Hadoop job configuration, from which the number of reducers is obtained.
    * @return
    *   The calculated integer grid order. This value represents the dimension `N` for an `N x N` grid.
    */
  private def getGridOrder(configuration: Configuration): Int =
    math.ceil(math.sqrt(configuration.getInt("mapreduce.job.reduces", 1).max(1))).toInt
