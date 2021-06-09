#!/bin/bash

uuid=$(uuidgen)
froot=$1; shift
out_name=$1; shift

#create unique dir for current job
mkdir $froot$uuid
file=$froot$uuid/$out_name

#-m flag deletes source files, should retain by default
zip -qq $file $@

if [ $? -eq 0 ] && [ -f "$file" ]
then
    echo -n $file
else
    rm -r $uuid
    exit 1
fi



# req="{
#         \"from\": \"mcleanj@hawaii.edu\",
#         \"to\": \"$email\",
#         \"subject\": \"HCDP Data\",
#         \"message\": \"Here is your HCDP data package.\",
#         \"attachments\": [{
#             \"path\": \"$file\"
#         }]
#     }"

# curl --cacert cert.pem -X POST https://localhost:443/email \
#     -H "Content-Type: application/json" \
#     -d "$req"

# #cleanup
# rm -r $uuid