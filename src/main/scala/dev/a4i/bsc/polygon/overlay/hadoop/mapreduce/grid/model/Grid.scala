package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model

import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Envelope

opaque type Grid = Vector[Grid.Cell]

object Grid:

  case class Cell(id: Long, envelope: Envelope)

  extension (grid: Grid) def foreach[U](f: Cell => U): Unit = grid.foreach(f)

  def of(minimumBoundingRectangle: Envelope, order: Int): Grid =
    val cellWidth: Double  = minimumBoundingRectangle.getWidth / order
    val cellHeight: Double = minimumBoundingRectangle.getHeight / order

    val grid: Seq[Cell] =
      for
        row <- 0L until order
        col <- 0L until order
      yield
        val aX = minimumBoundingRectangle.getMinX + col * cellWidth
        val bX = aX + cellWidth
        val aY = minimumBoundingRectangle.getMinY + row * cellHeight
        val bY = aY + cellHeight

        Cell(row * order + col, Envelope(Coordinate(aX, aY), Coordinate(bX, bY)))

    grid.to(Vector)
