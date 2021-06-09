req="{
        \"email\": \"mcleanj@hawaii.edu\",
        \"files\": [\"test_pack/test.txt\", \"test_pack/test/test.txt\"]
    }"

curl --cacert cert.pem -X POST https://localhost:443/genzip/email \
    -H "Content-Type: application/json" \
    -d "$req"