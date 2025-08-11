package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.util

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.geotools.data.geojson.GeoJSONReader
import org.geotools.data.geojson.GeoJSONWriter
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.geojson.GeoJsonReader
import org.opengis.feature.simple.SimpleFeature
import org.opengis.feature.simple.SimpleFeatureType

object GeoJSON:

  private val objectMapper: ObjectMapper   = ObjectMapper()
  private val geoJSONReader: GeoJsonReader = GeoJsonReader()

  def parseFeatureId(json: String): String =
    GeoJSONReader
      .parseFeature(json)
      .getID

  def parseFeatureGeometry(json: String): Geometry =
    val jsonNode: JsonNode = objectMapper.readTree(json)

    val geometryJsonNode: JsonNode =
      if jsonNode.has("type") && jsonNode.get("type").asText == "Feature"
      then jsonNode.get("geometry")
      else jsonNode

    geoJSONReader.read(geometryJsonNode.toString)

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
