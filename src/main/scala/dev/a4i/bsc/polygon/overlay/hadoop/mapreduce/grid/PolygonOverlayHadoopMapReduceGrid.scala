package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid

import scala.io.Source
import scala.util.Using

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Options
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.JobID
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry

import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.live.PolygonOverlayGridMapperLive
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.live.PolygonOverlayGridReducerLive
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model.TaggedGeometryWritable
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.util.GeoJSON

class PolygonOverlayHadoopMapReduceGrid extends Configured, Tool:

  private val jobType: String          = "polygon-overlay"
  private val jobTypeQualifier: String = "hadoop-mapreduce-grid"

  var jobId: Option[JobID] = None

  override def run(args: Array[String]): Int =
    val options: Options = Options()
      .addRequiredOption(/* opt */ null, "base", /* hasArg */ true, "Base layer GeoJSON")
      .addRequiredOption(/* opt */ null, "overlay", /* hasArg */ true, "Overlay layer GeoJSON")
      .addRequiredOption(/* opt */ null, "output", /* hasArg */ true, "Output directory")
      .addRequiredOption(/* opt */ null, "reference-id", /* hasArg */ true, "Run identifier")
      .addOption(/* opt */ null, "wait-for-completion", /* hasArg */ true, "Wait for the completion of the job")

    val commandLine: CommandLine = DefaultParser().parse(options, args, /* stopAtNonOption = */ false)

    val base: Path                 = Path(commandLine.getOptionValue("base"))
    val overlay: Path              = Path(commandLine.getOptionValue("overlay"))
    val output: Path               = Path(commandLine.getOptionValue("output"))
    val referenceId: String        = commandLine.getOptionValue("reference-id")
    val waitForCompletion: Boolean = commandLine.getOptionValue("wait-for-completion", "true").toBoolean

    val job: Job = getJob(base, overlay, output, referenceId)

    if waitForCompletion
    then if job.waitForCompletion(true) then 0 else 1
    else
      job.submit
      jobId = Some(job.getJobID)
      0

  private def getJob(base: Path, overlay: Path, output: Path, referenceId: String): Job =
    val configuration: Configuration = getConfiguration(base, overlay)
    val jobName: String              = s"${jobType}_${jobTypeQualifier}_${referenceId}"

    val job: Job = Job.getInstance(configuration, jobName)

    FileInputFormat.addInputPath(job, base)
    FileInputFormat.addInputPath(job, overlay)
    FileOutputFormat.setOutputPath(job, output)

    job.setJarByClass(classOf[PolygonOverlayHadoopMapReduceGrid])

    job.setMapperClass(classOf[PolygonOverlayGridMapperLive])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[TaggedGeometryWritable])

    job.setReducerClass(classOf[PolygonOverlayGridReducerLive])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    job

  private def getConfiguration(base: Path, overlay: Path): Configuration =
    val configuration: Configuration = getConf

    configuration.set("baseLayer.path", base.toString)
    configuration.set("overlayLayer.path", overlay.toString)

    val gridMinimumBoundingRectangle: Envelope =
      val fileSystem: FileSystem = FileSystem.get(configuration)

      new Envelope:
        expandToInclude(getMinimumBoundingRectangle(fileSystem, base))
        expandToInclude(getMinimumBoundingRectangle(fileSystem, overlay))

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

object PolygonOverlayHadoopMapReduceGrid:

  def main(args: Array[String]): Unit =
    sys.exit(ToolRunner.run(PolygonOverlayHadoopMapReduceGrid(), args))
