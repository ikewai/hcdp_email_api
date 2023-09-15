#!/bin/bash

docker stop emailtest
docker wait emailtest
docker rm emailtest
docker build -t hcdp_email_api_test .

docker run --name=emailtest -d -p 8443:443 \
-v /mnt/netapp/ikewai/annotated/HCDP:/data \
-v /home/ikewai/hcdp_email_api/logs:/logs \
-v /home/ikewai/hcdp_email_api/api/certs/live/cistore.its.hawaii.edu/fullchain.pem:/usr/src/app/cert.pem \
-v /home/ikewai/hcdp_email_api/api/certs/live/cistore.its.hawaii.edu/privkey.pem:/usr/src/app/key.pem \
hcdp_email_api_test