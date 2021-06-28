FROM node:16

RUN apt-get update \
&& apt-get install -y zip \
&& apt-get install -y uuid-runtime

# Create app directory
WORKDIR /usr/src/app

# Install app dependencies
COPY certs/*.pem ./
COPY package*.json ./
COPY config.json ./
COPY server.js ./
COPY zipgen.sh ./
COPY zipgen_parts.sh ./

RUN npm install
# If you are building your code for production
# RUN npm install --only=production

EXPOSE 443
CMD [ "npm", "start" ]
