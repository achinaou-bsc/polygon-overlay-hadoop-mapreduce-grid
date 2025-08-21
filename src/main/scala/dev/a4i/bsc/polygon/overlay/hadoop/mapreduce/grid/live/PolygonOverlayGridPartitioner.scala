package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.live

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Partitioner

import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model.TaggedGeometryWritable

type PolygonOverlayGridPartitioner = Partitioner[LongWritable, TaggedGeometryWritable]
