package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.live

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model.TaggedGeometryWritable

type PolygonOverlayGridReducer = Reducer[LongWritable, TaggedGeometryWritable, NullWritable, Text]
