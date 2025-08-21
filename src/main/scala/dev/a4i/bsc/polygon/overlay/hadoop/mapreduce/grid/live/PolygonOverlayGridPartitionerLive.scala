package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.live

import scala.compiletime.uninitialized

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable

import dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model.TaggedGeometryWritable

class PolygonOverlayGridPartitionerLive extends PolygonOverlayGridPartitioner, Configurable:

  private var configuration: Configuration = uninitialized

  override def setConf(configuration: Configuration): Unit =
    this.configuration = configuration

  override def getConf: Configuration =
    configuration

  override def getPartition(key: LongWritable, value: TaggedGeometryWritable, numberOfPartitions: Int): Int =
    val gridOrder: Long     = getConf.getInt("grid.order", 1)
    val numberOfCells: Long = gridOrder * gridOrder

    if numberOfPartitions <= 1 || numberOfCells <= 0L
    then 0
    else
      val numberOfCellsPerReducer: Long = math.max(1L, (numberOfCells + numberOfPartitions - 1L) / numberOfPartitions)

      val rawPartitionIndex: Long = key.get / numberOfCellsPerReducer

      val clampedPartitionIndex: Int = rawPartitionIndex match
        case i if i < 0                   => 0
        case i if i >= numberOfPartitions => numberOfPartitions - 1
        case i                            => i.toInt

      clampedPartitionIndex
