#! /bin/bash

set -o errexit -o nounset
OUTPUT=""
TAG=building
NUM_TAGS=200
MIN_TAG_OCCURANCE=10
DURATION="week"
while getopts "i:o:t:T:O:d:" OPT ; do
	case $OPT in
		i) FILENAME="$(realpath "${OPTARG}")" ;;
		o) OUTPUT="$(realpath "${OPTARG}")" ;;
		t) TAG=$OPTARG ;;
		T) NUM_TAGS=$OPTARG ;;
		O) MIN_TAG_OCCURANCE=$OPTARG ;;
		d) DURATION=$OPTARG ;;
		*) exit 1;;
	esac
done


rm -f output.csv
echo "Converting PBF into tag CSV file..."
osm-tag-csv-history -v -i "$FILENAME"  -o ./raw_csv_tag_changes.csv -t "$TAG" -C old_value,new_value,iso_timestamp

echo "Loading CSV into PostgreSQL..."
psql -c "drop table if exists raw_tag_changes;"
psql -c "CREATE TABLE raw_tag_changes (old_value TEXT, new_value TEXT, iso_timestamp TIMESTAMP);"
psql -c "COPY raw_tag_changes FROM STDIN WITH CSV HEADER;" <raw_csv_tag_changes.csv
psql -c "drop table if exists tag_changes;"
psql -c "CREATE TABLE tag_changes AS ( (SELECT  -1 AS delta, old_value AS value, iso_timestamp FROM raw_tag_changes WHERE old_value <> '') UNION ALL ( SELECT  +1 AS delta, new_value AS value, iso_timestamp FROM raw_tag_changes WHERE new_value <> '' ) );"
psql -c "drop table raw_tag_changes;"
psql -c "CREATE INDEX IF NOT EXISTS tag_changes__timestamp ON tag_changes (iso_timestamp);"
psql -c "CREATE INDEX IF NOT EXISTS tag_changes__value ON tag_changes (value);"

if [ -n "$MIN_TAG_OCCURANCE" ] ; then
	echo "Deleting any tag key=value which doesn't occur at least $MIN_TAG_OCCURANCE times.."
	psql -c "delete from tag_changes where value IN (select value from tag_changes group by value having count(*) <= ${MIN_TAG_OCCURANCE});"
fi
if [ -n "$NUM_TAGS" ] ; then
	echo "Deleting all except the top $NUM_TAGS most popular tags..."
	psql -c "delete from tag_changes where value IN (select value from tag_changes group by value order by count(*) desc offset ${NUM_TAGS});"
fi

psql -c "copy ( with date_changes AS ( select value, date_trunc('$DURATION', iso_timestamp)::date as date, sum(delta) as delta  from tag_changes group by value, date  order by value, date ) select value, date, sum(delta) over (partition by value order by date) as total from date_changes order by value, date) to stdout with csv header;" > results_long.csv
#psql -c "drop table tag_changes;"

if [ -z "$OUTPUT" ] ; then
	OUTPUT="${TAG}.csv"
fi
echo "Crosstab'ing the data into $OUTPUT ..."
~/code/rust/crosstabber/target/release/crosstabber -i results_long.csv -o "$OUTPUT" -v 0
rm results_log.csv
