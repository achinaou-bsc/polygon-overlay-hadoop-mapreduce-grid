package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model

import java.io.DataInput
import java.io.DataOutput

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.WritableUtils
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

    val jsonBytes: Array[Byte] = TaggedGeometryWritable.objectMapper.writeValueAsBytes(jsonObject)
    val jsonBytesLength: Int   = jsonBytes.length

    WritableUtils.writeVInt(dataOutput, jsonBytesLength)

    dataOutput.write(jsonBytes)

  override def readFields(dataInput: DataInput): Unit =
    val jsonBytesLength: Int   = WritableUtils.readVInt(dataInput)
    val jsonBytes: Array[Byte] = Array.ofDim(jsonBytesLength)

    dataInput.readFully(jsonBytes)

    val jsonObject: JsonNode = TaggedGeometryWritable.objectMapper.readTree(jsonBytes)

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
