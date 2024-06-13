#!/bin/bash

docker stop emailtest
docker wait emailtest
docker rm emailtest
docker build -t hcdp_email_api_test .

docker run --name=emailtest -d -p 8443:443 \
-v /mnt/netapp/ikewai/annotated/HCDP:/data \
-v /home/ikewai/hcdp_email_api/logs:/logs \
hcdp_email_api_test