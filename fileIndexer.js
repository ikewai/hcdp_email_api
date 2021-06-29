const moment = require("moment");

//multiple paths
//THERE IS NOTHING ABOVE DATA TYPE
// top level is data type
//next is file group, e.g. raster, values, metadata, ANYTHING WITH A DIFFERENT PATH LENGTH/HIERARCHY SEPARATE OUT HERE
//that all files will be at bottom, so don't have to worry about anything else

//not exactly... how deal with differences like the legacy stuff? how is that structured? note if have to can add ../ in the middle of paths, though really would be nice if it was a clean hierarchy
//especially between the legacy and new stuff there may be data overlap though
//should this just be stored above that branch?
//maybe put values above, even though no "values" in vis
let fileIndex = {
    root: "/data/",
    datatype: {
        rainfall: {
            raster: {
                values: (opts) => {
                    let files = [];
                    let pathBase = `${root}allMonYrData/`;
                    let fbase;
                    switch(opts.type) {
                        case "raster": {
                            fbase = "_statewide_rf_mm.tif"; 
                            break;
                        }
                        case "stderr": {
                            fbase = "_statewide_rf_mm_SE.tif"
                            break;
                        }
                        case "anomaly": {
                            fbase = "_statewide_anom.tif";
                            break;
                        }
                        case "loocv": {
                            fbase = "_statewide_anom_SE.tif";
                            break;
                        }
                        case "metadata": {
                            fbase = "_statewide_rf_mm_meta.txt";
                            break;
                        }
                    }
                    let period = opts.period;
                    let d1 = opts.dates[0];
                    let d2 = opts.dates[1];
                    let start = moment(d1);
                    let end = moment(d2);
                    while(start.add(1, period).isBefore(end)) {
                        let dateFormat = getFormat(start, period);
                        let fdate = start.format(dateFormat);
                        let file = `${pathBase}${fdate}/${fdate}${fbase}`;
                        files.push(file);
                    }
                    return {
                        files: files,
                        filterHandler: new GeotiffFilterHandler(opts.filterOpts)
                    };
                }
            },
            stations: {
                //array of files
                metadata: (opts) => {
                    return {
                        files: ["Master_Sta_List_Meta_2020_11_09.csv"],
                        filterHandler: new CSVFilterHandler(opts.filterOpts)
                    };
                },
                values: (opts) => {
                    //this stuff needs to move
                    let attributes = ["period", "tier", "fill"];
                    let index = new MultiAttributeMap(attributes);
                    index.setData({
                        period: "month",
                        tier: 0,
                        fill: "partial"
                    }, "monthly_rf_new_data_1990_2020_FINAL_19dec2020.csv");
                    index.setData({
                        period: "day",
                        tier: 0,
                        fill: "partial"
                    }, "Unfilled_Daily_RF_mm_2020_12_31_RF.csv");
                    index.setData({
                        period: "day",
                        tier: 0,
                        fill: "unfilled"
                    }, "Unfilled_Daily_RF_mm_2020_12_31_RF.csv");

                    let data = {
                        period: opts.period,
                        tier: opts.tier,
                        fill: opts.fill
                    }
                    let file = index.getData(data);
                    return {
                        files: [file],
                        filterHandler: new CSVFilterHandler(opts.filterOpts) 
                    }
                }

            }
            
        }
      
    }
}

let getFormat = (date, period) => {
    let dateFormat = "";
    switch(period) {
        case "year": {
            dstring += "YYYY";
        }
        case "month": {
            dstring += "_" + "MM";
        }
        case "day": {
            dstring += "_" + "DD";
            break;
        }
        default: {
            throw Error("Unrecognized period");
        }
    }
    return dateFormat;
}

//file grouping, file information
//file information should have same set of properties




//groups, select, add options group

//mongo records with time series data strips, files for maps
//have to update 400k records every time add data vs inserting one, so maybe an issue

//terminate at string or function returning string
//anything with dates MUST HAVE PERIOD, just have this as funct param, wait, period affects others though
//can switch order here, just have precedence indexing in app

//for filtering and generating new files how integrate? should make 

class Indexer {

    getFiles(path) {
        let files = [];
        let index = fileIndex;
        for(item of path) {
            if(typeof index === "function") {
                index = index(item);
            }
            else {
                index = index[item]
            }
        }
        files.concat
    }
}


class CSVFilterHandler {
    constructor(filterOpts) {

    }
}

class GeotiffFilterHandler {
    constructor(filterOpts) {
        
    }
}


class MultiAttributeMap {
    map = {}

    constructor(precedence) {
        this.precedence = precedence;
    }

    setData(map, value) {
        let root = this.map;
        let i;
        for(i = 0; i < this.precedence.length - 1; i++) {
            let property = this.precedence[i];
            let val = map[property];
            let next = root[val];
            if(!next) {
                next = {};
                root[val] = next;
            }
            root = next;
        }
        let property = this.precedence[i];
        let val = map[property];
        root[val] = value;
    }

    getValue(data) {
        let root = this.map;
        for(let property of this.precedence) {
            let value = data[property];
            let root = root[value];
        }
        return root;
    }
}

//need something to parse index, indexer class should be exported
module.exports = fileIndex;