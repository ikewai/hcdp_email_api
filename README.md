Email API

# INSTALLATION
To run the nodejs server after pulling the repository:

1. npm install
2. edit the config.js
3. run the server with >node server.js

OR you can build the docker container and run that:

1. edit the config.js - make sure the port is 443 as this is what is exposed in the dockerfile
2. run >docker build -t email_server .
3. run >docker run -d -p 443:443 email_serversh

The container can be accessed on localhost:443 now.

run >./zipgen.sh <dest_email> <file1> <file2> ...

