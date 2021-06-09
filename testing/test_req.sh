curl --cacert cert.pem -X POST https://localhost:443/email \
    -H "Content-Type: application/json" \
    -d @req.json