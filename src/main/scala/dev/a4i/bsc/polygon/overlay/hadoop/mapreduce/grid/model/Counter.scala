package dev.a4i.bsc.polygon.overlay.hadoop.mapreduce.grid.model

enum Counter extends Enum[Counter]:
  case BASE_POLYGONS_READ
  case OVERLAY_POLYGONS_READ
  case MAP_OUTPUTS_WRITTEN
  case SPATIAL_INDEX_QUERIED
  case INTERSECTIONS_CHECKED
  case INTERSECTIONS_CALCULATED
  case REDUCE_OUTPUTS_WRITTEN
