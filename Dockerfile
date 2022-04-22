FROM node:16

RUN apt-get update \
&& apt-get install -y zip \
&& apt-get install -y uuid-runtime \
&& apt-get install -y python3 \
&& apt-get install -y python3-pip

RUN pip3 install imagecodecs

# Create app directory
WORKDIR /usr/src/app

# Install app dependencies
COPY package*.json ./
COPY config.json ./
COPY server.js ./
COPY fileIndexer.js ./
COPY csvProcessor.js ./
COPY zipgen.sh ./
COPY zipgen_parts.sh ./
COPY reader.py ./


RUN npm install
# If you are building your code for production
# RUN npm install --only=production

EXPOSE 443
#don't use npm start because signals are handled weird. To get a graceful stop need to run node server.js directly
#https://medium.com/@becintec/building-graceful-node-applications-in-docker-4d2cd4d5d392
CMD [ "node", "server.js" ]
