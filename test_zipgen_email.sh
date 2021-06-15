req="{
        \"email\": \"mcleanj@hawaii.edu\",
        \"files\": [\"/data/Master_Sta_List_Meta_2020_11_09.csv\", \"/data/allMonYrData/1990_01/1990_01_bi_anom.tif\"]
    }"

curl -k -X POST https://cistore.its.hawaii.edu:443/genzip/email \
    -H "Content-Type: application/json" \
    -d "$req"