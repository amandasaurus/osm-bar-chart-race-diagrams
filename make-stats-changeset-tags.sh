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
psql -c "delete from changeset_tags where key = '';"
psql -c "update changeset_tags set created_at_day = date_trunc('day', created_at) where created_at_day IS NULL;"
psql -c "create index changeset_tags__created_at_day on changeset_tags ( created_at_day );"

psql -XAt -c "COPY (
WITH daily_changes AS (
	SELECT
		key as changeset_tag_key,
		created_at_day as date,
		sum(+1) as delta
	from changeset_tags
	where key <> ''
		and key IN ( select key from changeset_tags group by (key) having count(*) > 100 )
	group by (created_at_day, key)
)
Select changeset_tag_key, date, sum(delta) over (partition by changeset_tag_key order by date) as total from daily_changes order by changeset_tag_key, date
) to stdout with csv header;" > changeset_tags_long.csv

~/code/rust/crosstabber/target/release/crosstabber --numbers -l changeset_tags_long.csv -w changeset_tags.csv
