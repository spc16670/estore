export http_proxy=""
INDEX=kfis
curl -XDELETE "http://test-esb-1:9200/$INDEX"
