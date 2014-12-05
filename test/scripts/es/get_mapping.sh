export http_proxy=""
INDEX=kfis
curl -XGET "http://test-esb-1:9200/$INDEX/_mapping"
