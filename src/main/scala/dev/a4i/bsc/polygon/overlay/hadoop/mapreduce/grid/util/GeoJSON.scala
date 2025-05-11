package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.util

import org.geotools.data.geojson.GeoJSONReader
import org.geotools.data.geojson.GeoJSONWriter
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.SimpleFeature
import org.opengis.feature.simple.SimpleFeatureType

object GeoJSON:

  def parseFeatureId(json: String): String =
    GeoJSONReader
      .parseFeature(json)
      .getID

  def parseFeatureGeometry(json: String): Geometry =
    GeoJSONReader
      .parseFeature(json)
      .getDefaultGeometry
      .asInstanceOf[Geometry]

  def parseFeature(json: String): (id: String, geometry: Geometry) =
    val feature: SimpleFeature = GeoJSONReader.parseFeature(json)
    val id: String             = feature.getID
    val geometry: Geometry     = feature.getDefaultGeometry.asInstanceOf[Geometry]

    (id, geometry)

  def serialize(geometry: Geometry): String =
    val simpleFeatureTypeBuilder: SimpleFeatureTypeBuilder = new SimpleFeatureTypeBuilder:
      setName("OverlayGeometryFeature")
      setCRS(DefaultGeographicCRS.WGS84)
      add("the_geom", classOf[Geometry])

    val simpleFeatureType: SimpleFeatureType = simpleFeatureTypeBuilder.buildFeatureType

    val simpleFeatureBuilder: SimpleFeatureBuilder = new SimpleFeatureBuilder(simpleFeatureType):
      add(geometry)

    val simpleFeature: SimpleFeature = simpleFeatureBuilder.buildFeature(null)

    GeoJSONWriter.toGeoJSON(simpleFeature)
