FROM node:8

# Create app directory
WORKDIR /usr/src/app

# Install app dependencies
# A wildcard is used to ensure both package.json AND package-lock.json are copied
# where available (npm@5+)
COPY *.pem ./
COPY package*.json ./
COPY config.json ./
COPY server.js ./
COPY zipgen.sh ./

RUN npm install
# If you are building your code for production
# RUN npm install --only=production

EXPOSE 443
CMD [ "npm", "start" ]
