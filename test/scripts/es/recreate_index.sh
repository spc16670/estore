export http_proxy=""

ES_URL=http://test-esb-1:9200
INDEX=kfis

JSON=$(curl -s -XGET $ES_URL/$INDEX/_mapping)
ERROR=$(echo $JSON | ./jq 'has("kfis")')

if [ "$ERROR" == false ]
  then
    echo "______PUSHING THE MAPPING... ___"
    JSON=$(curl -s -X PUT "$ES_URL/$INDEX/" -d @mapping.json)
  else
    echo "_______MAPPING EXISTS_______"
fi
echo "$JSON"

#curl -X PUT "http://test-esb-1:9200/kfis/" -d@mapping.json
