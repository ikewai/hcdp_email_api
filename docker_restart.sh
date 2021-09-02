docker stop -t 600 email
docker wait email
docker rm email
docker run --name=email -d -p 443:443 \
-v /mnt/netapp/ikewai/annotated/Rainfall:/data \
-v /home/ikewai/hcdp_email_api/api/certs/live/cistore.its.hawaii.edu/cert.pem:/usr/src/app/cert.pem \
-v /home/ikewai/hcdp_email_api/api/certs/live/cistore.its.hawaii.edu/privkey.pem:/usr/src/app/key.pem \
hcdp_email_api
