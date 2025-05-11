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
  OUTPUT_DIRECTORY="$WORKING_DIRECTORY/output"

  echo "$(date "+%Y-%m-%d %H:%M:%S.%3N") INFO $REFERENCE_ID: Preparing the working directory..."

  hadoop fs -mkdir -p "$INPUT_DIRECTORY"

  echo "$(date "+%Y-%m-%d %H:%M:%S.%3N") INFO $REFERENCE_ID: http://localhost:9870/explorer.html#$WORKING_DIRECTORY"

  echo "$(date "+%Y-%m-%d %H:%M:%S.%3N") INFO $REFERENCE_ID: Uploading the input files..."

  hadoop fs -put -f {{INPUT1}} "$INPUT_DIRECTORY/a.geojson"
  hadoop fs -put -f {{INPUT2}} "$INPUT_DIRECTORY/b.geojson"

  echo "$(date "+%Y-%m-%d %H:%M:%S.%3N") INFO $REFERENCE_ID: Running the Job..."

  scala --power \
    run --suppress-experimental-feature-warning --suppress-outdated-dependency-warning --hadoop . -- "$REFERENCE_ID"

  echo "$(date "+%Y-%m-%d %H:%M:%S.%3N") INFO $REFERENCE_ID: Downloading the output files..."

  hadoop fs -cat "$OUTPUT_DIRECTORY/*" | cut -f2 > {{OUTPUT}}

  echo "$(date "+%Y-%m-%d %H:%M:%S.%3N") INFO $REFERENCE_ID: Done."

clean:
  scala clean .

compile:
  scala compile .

test:


package:
