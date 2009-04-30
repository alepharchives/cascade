#! /bin/bash

CURL=`which curl`
URI=http://127.0.0.1:5984
DB=test
DDOC=_design/foo
DESIGN=test1.design
DOCS=docs.json

# Setup test env
$CURL -X DELETE $URI/$DB/
$CURL -X PUT $URI/$DB
$CURL -X PUT -d @$DESIGN $URI/$DB/$DDOC
$CURL -X POST -d @$DOCS $URI/$DB/_bulk_docs

# See what we get
echo "Cascade!"
$CURL $URI/$DB/_cascade/foo/_stage/0
$CURL $URI/$DB/_cascade/foo/_stage/1

echo "Remove doc 6"
./remdoc.py $URI/$DB 6
echo ""
echo ""

echo "Recascade"
$CURL $URI/$DB/_cascade/foo/_stage/0
$CURL $URI/$DB/_cascade/foo/_stage/1
