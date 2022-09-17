#! /bin/bash

set -o errexit -o nounset -o pipefail
while getopts "i:" OPT ; do
	case $OPT in
		i) FILENAME="$(realpath "${OPTARG}")" ;;
		*) exit 1;;
	esac
done

( 

	if [ ! -s changesets.csv.gz ] || [ "$FILENAME" -nt changesets.csv.gz ] ; then
		TMP="$(mktemp -p . tmp.changesets.csv.gz.XXXXXX)"
		pv -c -N changesets.csv.gz "$FILENAME" | pbzip2 -d | ~/code/rust/anglosaxon/target/release/anglosaxon -S -o $'id\tcreated_at\tclosed_at\tuser\tuid\tmin_lat\tmin_lon\tmax_lat\tmax_lon\tcomments_count\tnum_changes\n' -s changeset -v id --tab -v created_at --tab -V closed_at NULL --tab -V user\!tsv NULL --tab -V uid NULL --tab -V min_lat NULL --tab -V min_lon NULL --tab -V max_lat NULL --tab -V max_lon NULL --tab -v comments_count --tab -v num_changes --nl | xsv input --no-quoting --escape \\ -d $'\t' | gzip > "$TMP"
		mv "$TMP" changesets.csv.gz
	fi

) &

(
	if [ ! -s changeset_tags.csv.gz ] || [ "$FILENAME" -nt changeset_tags.csv.gz ] ; then
		TMP="$(mktemp -p . tmp.changeset_tags.csv.gz.XXXXXX)"
		pv -c -N changeset_tags.csv.gz "$FILENAME" | pbzip2 -d | ~/code/rust/anglosaxon/target/release/anglosaxon -S -o $'id\tkey\tvalue\n' -s tag -v ../id --tab -v k\!tsv --tab -v v\!tsv --nl | xsv input --no-quoting --escape \\ -d $'\t' | gzip > "$TMP"
		mv "$TMP" changeset_tags.csv.gz
	fi

) &

wait


if [ changeset_tags.csv.gz -nt changesets.db ] || [ changesets.csv.gz -nt changesets.db ] ; then
	rm -f changesets.db

	sqlite3 changesets.db "CREATE TABLE changesets ( id integer, created_at TEXT, closed at text, user text, uid integer, min_lat real, min_lon real, max_lat real, max_lon real, comments_count integer, num_changes integer);"
	sqlite3 changesets.db "CREATE TABLE changeset_tags ( id integer, k TEXT, v TEXT );"
	sqlite3 changesets.db ".import --csv \"|pv changesets.csv.gz | zcat\" changesets"
	sqlite3 changesets.db ".import --csv \"|pv changeset_tags.csv.gz | zcat\" changeset_tags"
	sqlite3 changesets.db "alter TABLE changesets add column created_at_jday REAL;" "update changesets set created_at_jday = julianday(created_at);" "create index changesets__created_at_jday on changesets ( created_at_jday );"

	sqlite3 changesets.db '.header on' '.mode csv' "with periods AS ( select distinct strftime('%Y-%W', datetime(value, 'julianday'), 'weekday 0') as datestr from generate_series((select min(created_at_jday) from changesets), (select max(created_at_jday) from changesets)) ), per_week_total AS (select strftime('%Y-%W', created_at_jday, 'julianday', 'weekday 0') as datestr, count(*) as total from changesets group by datestr) select periods.datestr as date, coalesce(per_week_total.total, 0) as num_changes from periods left join per_week_total using (datestr) order by periods.datestr;" > changeset_per_week.csv


	for SIZE in 1 5 10 14 30 ; do
		sqlite3 changesets.db '.header on' '.mode csv' "with buckets AS (select floor(created_at_jday/${SIZE})*${SIZE} as bucket_label_jday, count(*)/${SIZE} as total from changesets where bucket_label_jday >= julianday('2012-09-15') and bucket_label_jday + ${SIZE} <= julianday('now') group by bucket_label_jday) select strftime('%Y-%m-%d', bucket_label_jday, 'julianday') as date, total as avg_per_day from buckets" > changesets_per_${SIZE}d.csv
	done

fi


# vim: ft=sh
