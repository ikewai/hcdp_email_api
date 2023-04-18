


let farrstring = "["

let fstrings = [];
for(let y = 1990; y < 2018; y++) {
    for(let m = 1; m < 13; m++) {
        let year = y.toString();
        let month = m.toString();
        while(month.length < 2) {
            month = "0" + month;
        }
        let ystring = year + "_" + month;
        let fname = "/data/allMonYrData/" + ystring + "/" + ystring + "_statewide_rf_mm.tif"
        let fstring = "\\\"" + fname + "\\\"";
        fstrings.push(fstring);
    }

    "[\"/data/Master_Sta_List_Meta_2020_11_09.csv\", \"/data/allMonYrData/1990_01/1990_01_bi_anom.tif\"]"
}

farrstring += fstrings.join(",");
farrstring += "]";
console.log(farrstring);