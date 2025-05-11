package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model

import java.io.DataInput
import java.io.DataOutput

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.hadoop.io.Writable
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.io.WKTReader
import org.locationtech.jts.io.WKTWriter

class TaggedGeometryWritable(var taggedGeometry: TaggedGeometry) extends Writable:

  override def write(dataOutput: DataOutput): Unit =
    val sourceLayerTypeSerialized: String = taggedGeometry.sourceLayerType.toString
    val geometrySerialized: String        = WKTWriter().write(taggedGeometry.geometry)

    val jsonObject: ObjectNode = TaggedGeometryWritable.objectMapper.createObjectNode
    jsonObject.put(TaggedGeometryWritable.sourceLayerTypeFieldName, sourceLayerTypeSerialized)
    jsonObject.put(TaggedGeometryWritable.geometryFieldName, geometrySerialized)

    val json: String = TaggedGeometryWritable.objectMapper.writeValueAsString(jsonObject)

    dataOutput.writeUTF(json)

  override def readFields(dataInput: DataInput): Unit =
    val json: String = dataInput.readUTF

    val jsonObject: JsonNode = TaggedGeometryWritable.objectMapper.readTree(json)

    val sourceLayerTypeSerialized: String = jsonObject.get(TaggedGeometryWritable.sourceLayerTypeFieldName).asText
    val geometrySerialized: String        = jsonObject.get(TaggedGeometryWritable.geometryFieldName).asText

    val sourceLayerType = LayerType.valueOf(sourceLayerTypeSerialized)
    val geometry        = WKTReader(TaggedGeometryWritable.geometryFactory).read(geometrySerialized)

    taggedGeometry = TaggedGeometry(sourceLayerType, geometry)

  override def toString: String =
    s"TaggedGeometryWritable(taggedGeometry=$taggedGeometry)"

  protected def this() = this(null)

object TaggedGeometryWritable:

  private val geometryFactory: GeometryFactory = GeometryFactory()
  private val objectMapper: ObjectMapper       = ObjectMapper()

  private val sourceLayerTypeFieldName: String = "sourceLayerType"
  private val geometryFieldName: String        = "geometry"
