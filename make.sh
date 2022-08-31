#! /bin/bash
set -o errexit -o nounset
FILENAME="$(realpath "${1:?arg 1 must be input filename}")"

rm -f output.csv
osm-tag-csv-history -v -i "$FILENAME"  -o ./raw_csv_tag_changes.csv -t building -C old_value,new_value,iso_timestamp

psql -c "CREATE TABLE IF NOT EXISTS raw_tag_changes (old_value TEXT, new_value TEXT, iso_timestamp TIMESTAMP);"
psql -c "CREATE INDEX IF NOT EXISTS raw_tag_changes_timestamp ON raw_tag_changes (iso_timestamp);"
psql -c "CREATE INDEX IF NOT EXISTS raw_tag_changes_values ON raw_tag_changes (new_value, old_value);"
psql -c "TRUNCATE TABLE raw_tag_changes;"
psql -c "COPY raw_tag_changes FROM STDIN WITH CSV HEADER;" <raw_csv_tag_changes.csv
psql -c "CREATE TABLE IF NOT EXISTS tag_changes (value TEXT, iso_timestamp TIMESTAMP);"
psql -c "TRUNCATE TABLE tag_changes;"
psql -c "DROP MATERIALIZED VIEW IF EXISTS values CASCADE;"
psql -c "CREATE MATERIALIZED VIEW values AS (select distinct new_value from raw_tag_changes where new_value <> '' order by new_value);"
psql -c "DROP MATERIALIZED VIEW IF EXISTS dates;"
psql -c "CREATE MATERIALIZED VIEW dates AS ( select to_char( generate_series( (select date_trunc('year', min(iso_timestamp))  from raw_tag_changes), now(), '1 month'), 'YYYY-MM-DD') );"
psql -c "DROP MATERIALIZED VIEW IF EXISTS t_to_pivot;"
psql -c "CREATE MATERIALIZED VIEW t_to_pivot AS ( select values.new_value as tag_value, dates.to_char, sum(case when raw_tag_changes.old_value = values.new_value and raw_tag_changes.new_value = values.new_value then 0 when raw_tag_changes.old_value = values.new_value then -1 when raw_tag_changes.new_value = values.new_value then +1 else 0 end) as value_total from values, dates, raw_tag_changes where raw_tag_changes.iso_timestamp <= dates.to_char::timestamp and (raw_tag_changes.new_value = values.new_value OR raw_tag_changes.old_value = values.new_value) group by (values.new_value, dates.to_char) order by 1, 2);"

psql -At -c "COPY (select * from t_to_pivot order by 1,2) TO STDOUT " | sort > table_to_pivot.tsv
datamash -s crosstab 1,2 sum 3 < table_to_pivot.tsv > crosstab_for_flourish.tsv
#
#rm -f tab
#psql -At -c "select distinct new_value from raw_tag_changes where new_value <> '' order by new_value;" | sponge | while read VALUE ; do
#	psql -At -c "select to_char( generate_series( (select date_trunc('year', min(iso_timestamp))  from raw_tag_changes), now(), '1 week'), 'YYYY-MM-DD');" | sponge | while read DATE_COL ; do
#		NUM=$(psql -At -c "select sum(case when raw_tag_changes.old_value = '$VALUE' and raw_tag_changes.new_value = '$VALUE' then 0 when raw_tag_changes.old_value = '$VALUE' then -1 when raw_tag_changes.new_value = '$VALUE' then +1 else 0 end) as value_total from raw_tag_changes where raw_tag_changes.iso_timestamp <= '$DATE_COL'::timestamp;")
#		echo -e "$VALUE\t$DATE_COL\t${NUM:-0}"
#	done	
#done

