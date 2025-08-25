package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.live

import java.lang.Iterable as JavaIterable
import java.util.List as JavaList
import scala.jdk.CollectionConverters.given

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.index.strtree.STRtree

import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model.LayerType
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model.TaggedGeometry
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model.TaggedGeometryWritable
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.util.GeoJSON
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model.Counter

class PolygonOverlayGridReducerLive extends PolygonOverlayGridReducer:

  override def reduce(
      key: LongWritable,
      values: JavaIterable[TaggedGeometryWritable],
      context: PolygonOverlayGridReducer#Context
  ): Unit =
    given PolygonOverlayGridReducer#Context = context

    val taggedGeometries: Array[TaggedGeometry] =
      values.asScala
        .map(_.taggedGeometry)
        .toArray

    val (baseLayerGeometries: Array[Geometry], overlayLayerGeometries: Array[Geometry]) =
      taggedGeometries.partitionMap:
        case TaggedGeometry(LayerType.Base, geometry)    => Left(geometry)
        case TaggedGeometry(LayerType.Overlay, geometry) => Right(geometry)

    val overlayLayerGeometriesTree: STRtree =
      buildTree(overlayLayerGeometries)

    baseLayerGeometries.foreach: baseGeometry =>
      context.getCounter(Counter.SPATIAL_INDEX_QUERIED).increment(1)

      val candidates: JavaList[Geometry] =
        overlayLayerGeometriesTree
          .query(baseGeometry.getEnvelopeInternal)
          .asInstanceOf[JavaList[Geometry]]

      context.getCounter(Counter.SPATIAL_INDEX_MATCHES).increment(candidates.size)

      candidates.iterator.asScala
        .filter(overlaps(baseGeometry))
        .map(overlay(baseGeometry))
        .foreach: overlayGeometry =>
          context.write(NullWritable.get, Text(GeoJSON.serialize(overlayGeometry)))
          context.getCounter(Counter.REDUCE_OUTPUTS_WRITTEN).increment(1)

  private def buildTree(geometries: Iterable[Geometry]): STRtree =
    val tree: STRtree = STRtree()

    geometries.foreach(geometry => tree.insert(geometry.getEnvelopeInternal, geometry))

    tree.build()

    tree

  private def overlaps(a: Geometry)(b: Geometry)(using context: PolygonOverlayGridReducer#Context): Boolean =
    val result: Boolean = a.intersects(b)
    context.getCounter(Counter.INTERSECTIONS_CHECKED).increment(1)
    result

  private def overlay(a: Geometry)(b: Geometry)(using context: PolygonOverlayGridReducer#Context): Geometry =
    val result = a.intersection(b)
    context.getCounter(Counter.INTERSECTIONS_CALCULATED).increment(1)
    result
