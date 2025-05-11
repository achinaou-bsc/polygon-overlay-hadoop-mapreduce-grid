package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.live

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model.TaggedGeometryWritable

type PolygonOverlayGridMapper = Mapper[LongWritable, Text, Text, TaggedGeometryWritable]
