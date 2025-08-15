package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.live

import java.lang.Iterable as JavaIterable
import scala.collection.mutable.Buffer
import scala.jdk.CollectionConverters.given

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.index.strtree.STRtree

import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model.LayerType
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model.TaggedGeometry
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model.TaggedGeometryWritable
import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.util.GeoJSON

class PolygonOverlayGridReducerLive extends PolygonOverlayGridReducer:

  override def reduce(
      key: Text,
      values: JavaIterable[TaggedGeometryWritable],
      context: PolygonOverlayGridReducer#Context
  ): Unit =
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
      overlayLayerGeometriesTree
        .query(baseGeometry.getEnvelopeInternal)
        .asScala
        .asInstanceOf[Buffer[Geometry]]
        .map(overlay(baseGeometry))
        .foreach(overlayGeometry => context.write(NullWritable.get, Text(GeoJSON.serialize(overlayGeometry))))

  private def buildTree(geometries: Iterable[Geometry]): STRtree =
    val tree: STRtree = STRtree()

    geometries.foreach(geometry => tree.insert(geometry.getEnvelopeInternal, geometry))

    tree.build()

    tree

  private def overlay(a: Geometry)(b: Geometry): Geometry =
    a.intersection(b)
