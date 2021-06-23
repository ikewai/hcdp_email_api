const csv = require("csv-parser");
const fs = require("fs");

let results = [];


let property2header = {

}



class CSVHandler {
    //constructPropertyFilter()

    constructPropertyFilters(filters) {
        let filterFuncts = {};
        for(let filter of filters) {
            let valueSet = Set(filter.values);
            let header = property2header[filter.field];
            let include = filter.include;
            filterFuncts[header] = (value) => {
                let contains = valueSet.has(value);
                return contains && include;
            };
        }
        
        this.filterFuncts = filterFuncts;
    }

    rowFilter(row) {
        for(let property in row) {
            let value = row[property];
            if(!this.filterFuncts[property](value)) {
                return false
            }
        }
        return true;
    }
    
    filterCSV(infile, outfile, filters) {
        fs.createReadStream(file)
        .pipe(csv())
        .on("data", (data) => {

        })
        .on("end", () => {

        });
    }


}