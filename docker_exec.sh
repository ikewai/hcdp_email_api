#!/bin/bash

docker build -t hcdp_email_api .
docker run -p 443:443 -v /mnt/netapp/ikewai/annotated/Rainfall:/data hcdp_email_api
