#! /bin/bash

set -o errexit -o nounset -o pipefail
while getopts "i:" OPT ; do
	case $OPT in
		i) FILENAME="$(realpath "${OPTARG}")" ;;
		*) exit 1;;
	esac
done
if [ ! -s changeset_tags.csv.gz ] ; then
	pv "$FILENAME" | pbzip2 -d | ~/code/rust/anglosaxon/target/release/anglosaxon -S -o $'id\tcreated_at\tuid\tkey\tvalue\n' -s tag -v ../id --tab -v ../created_at --tab -V ../uid NULL --t ab -v k\!tsv --tab -v v\!tsv --nl | xsv input --no-quoting --escape \\ -d $'\t' | gzip > changeset_tags.csv.gz
fi

echo "Loading CSV into PostgreSQL..."
psql -c "drop table if exists changeset_tags;"
psql -c "CREATE TABLE changeset_tags (id INTEGER, created_at TIMESTAMP with time zone, created_at_day date, uid INTEGER, key TEXT, value TEXT);"
pv changeset_tags.csv.gz | zcat | psql -c "COPY changeset_tags (id, created_at, uid, key, value) FROM STDIN WITH CSV HEADER NULL 'NULL'"
psql -c "update changeset_tags set created_at_day = date_trunc('day', created_at) where created_at_day IS NULL;"
psql -c "create index changeset_tags__created_at_day on changeset_tags ( created_at_day );"
#psql -c "delete from changeset_data where uid is NULL;"
#psql -c "delete from changeset_data where num_changes = 0;"
#psql -c "create index changeset_data__iso_timestamp on changeset_data (iso_timestamp);"
#psql -c "create index changeset_data__uid on changeset_data (uid);"
#
#psql -c "copy ( select date_trunc('week', iso_timestamp) as date, count(*) as edit from changeset_data group by date order by date asc ) to stdout csv header;" > edits_per_week.csv
