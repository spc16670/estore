export http_proxy=""
INDEX=kfis
curl -XPUT "http://test-esb-1:9200/$INDEX/" -d @mapping.json
