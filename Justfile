default:
  @just --list --unsorted

check-formatting:
  scala fmt --check .

format:
  scala fmt .

run INPUT1 INPUT2 OUTPUT:
  #! /usr/bin/env bash

  set \
    -o errexit \
    -o pipefail \
    -o nounset

  TYPE="polygon-overlay"
  REFERENCE_ID="$(uuidgen)"
  WORKING_DIRECTORY="/jobs/$TYPE/$REFERENCE_ID"
  INPUT_DIRECTORY="$WORKING_DIRECTORY/input"
  BASE_LAYER="$INPUT_DIRECTORY/a.geojson"
  OVERLAY_LAYER="$INPUT_DIRECTORY/b.geojson"
  OUTPUT_DIRECTORY="$WORKING_DIRECTORY/output"

  echo "$(date "+%Y-%m-%d %H:%M:%S.%3N") INFO $REFERENCE_ID: Preparing the working directory..."

  hadoop fs -mkdir -p "$INPUT_DIRECTORY"

  echo "$(date "+%Y-%m-%d %H:%M:%S.%3N") INFO $REFERENCE_ID: http://localhost:9870/explorer.html#$WORKING_DIRECTORY"

  echo "$(date "+%Y-%m-%d %H:%M:%S.%3N") INFO $REFERENCE_ID: Uploading the input files..."

  hadoop fs -put -f {{INPUT1}} "$BASE_LAYER"
  hadoop fs -put -f {{INPUT2}} "$OVERLAY_LAYER"

  echo "$(date "+%Y-%m-%d %H:%M:%S.%3N") INFO $REFERENCE_ID: Running the Job..."

  scala --power \
    run --suppress-experimental-feature-warning --suppress-outdated-dependency-warning --hadoop . \
    -- \
    --base "$BASE_LAYER" --overlay "$OVERLAY_LAYER" --output "$OUTPUT_DIRECTORY" --reference-id "$REFERENCE_ID"

  echo "$(date "+%Y-%m-%d %H:%M:%S.%3N") INFO $REFERENCE_ID: Downloading the output files..."

  hadoop fs -cat "$OUTPUT_DIRECTORY/*" > {{OUTPUT}}

  echo "$(date "+%Y-%m-%d %H:%M:%S.%3N") INFO $REFERENCE_ID: Done."

clean:
  rm -rf dist
  scala clean .

compile:
  scala --power \
    compile --suppress-experimental-feature-warning --suppress-outdated-dependency-warning \
    .

test:


package:
  mkdir --parents dist

  scala --power \
    package --suppress-experimental-feature-warning --suppress-outdated-dependency-warning --assembly \
      --provided org.apache.hadoop:hadoop-client-api \
      --preamble=false \
      --output polygon-overlay-hadoop-mapreduce-grid.jar \
      .

  mv polygon-overlay-hadoop-mapreduce-grid.jar dist
