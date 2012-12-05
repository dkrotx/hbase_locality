#!/bin/sh -e

REPFILE=locality.txt

if [ $# -ne 1 ]; then
    echo "Usage: $0 tablename" >&2
    exit 64
fi

TABLE=$1

echo "Retrieving HBase server:region mapping"
echo "scan '.META.', { COLUMNS => 'info:server' }" | hbase shell | ./extract_servs.pl $TABLE >.hbase_regmap

echo "Retrieving block-location statistics of table from hdfs (might take few minutes)"
hadoop fsck "/hbase/$TABLE/" -files -blocks -locations >.hbase_blocks

echo "Making locality report"
./local_stat.pl $TABLE .hbase_regmap .hbase_blocks >$REPFILE

echo "OK, see $REPFILE"
