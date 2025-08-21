package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.live

import scala.compiletime.uninitialized

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.index.strtree.STRtree

import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model.Grid
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model.LayerType
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model.TaggedGeometry
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model.TaggedGeometryWritable
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.util.GeoJSON

class PolygonOverlayGridMapperLive extends PolygonOverlayGridMapper:

  private var currentLayerType: LayerType = uninitialized
  private var tree: STRtree               = uninitialized

  override def setup(context: PolygonOverlayGridMapper#Context): Unit =
    val configuration: Configuration = context.getConfiguration
    val fileSystem: FileSystem       = FileSystem.get(configuration)
    val currentLayerFilePath: Path   = context.getInputSplit.asInstanceOf[FileSplit].getPath
    val baseLayerFilePath: Path      = fileSystem.makeQualified(Path(context.getConfiguration.get("baseLayer.path")))

    currentLayerType =
      if currentLayerFilePath.equals(baseLayerFilePath)
      then LayerType.Base
      else LayerType.Overlay

    tree = STRtree()

    val gridMinimumBoundingRectangle: Envelope =
      Envelope(
        Coordinate(
          configuration.getDouble("grid.mbr.minX", -180),
          configuration.getDouble("grid.mbr.minY", -90)
        ),
        Coordinate(
          configuration.getDouble("grid.mbr.maxX", 180),
          configuration.getDouble("grid.mbr.maxY", 90)
        )
      )

    val gridOrder: Int =
      configuration.getInt("grid.order", 1)

    Grid
      .of(gridMinimumBoundingRectangle, gridOrder)
      .foreach(cell => tree.insert(cell.envelope, cell.id))

    tree.build()

  override def map(key: LongWritable, value: Text, context: PolygonOverlayGridMapper#Context): Unit =
    val (_: String, geometry: Geometry)                = GeoJSON.parseFeature(value.toString)
    val taggedGeometry: TaggedGeometry                 = TaggedGeometry(currentLayerType, geometry)
    val taggedGeometryWritable: TaggedGeometryWritable = TaggedGeometryWritable(taggedGeometry)

    tree
      .query(geometry.getEnvelopeInternal)
      .forEach(cellId => context.write(LongWritable(cellId.asInstanceOf[Long]), taggedGeometryWritable))
