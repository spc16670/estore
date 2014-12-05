export http_proxy=""

INDEX=kfis
TYPE=staff
ID=1

FNAME=szymon
LNAME=czaja
DOB=19870301
AGE=27

INDEX_INFO="{ \"index\" : { \"_index\" : \"$INDEX\", \"_type\" : \"$TYPE\", \"_id\" : \"$ID\" } }"
DATA_INFO="{ \"ttl\":\"1m\", \"wid\":\"$ID\", \"fname\":\"$FNAME\", \"dob\":\"$DOB\", \"age\":\"$AGE\" }"
DATA=$(echo "$INDEX_INFO"$'\n'"$DATA_INFO"$'\n'"")

echo "$DATA"

curl -X POST "http://test-esb-1:9200/_bulk" --data-binary '{ "index" : { "_index" : "kfis", "_type" : "staff", "_id" : "1" } }
{ "ttl":"1m", "wid":"1", "fname":"szymon", "dob":"19870301", "age":"27" }
'
