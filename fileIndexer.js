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
                values: (dates, fileData, filterOpts) => {
                    let files = [];
                    let pathBase = `${fileIndex.root}allMonYrData/`;
                    let fbase;
                    switch(fileData.type) {
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
                        case "stderr_anomaly": {
                            fbase = "_statewide_anom_SE.tif";
                            break;
                        }
                        case "metadata": {
                            fbase = "_statewide_rf_mm_meta.txt";
                            break;
                        }
                    }
                    let period = dates.period;
                    let formatter = new DateFormatter(period);
                    let d1 = dates.start;
                    let d2 = dates.end;
                    let start = moment(d1);
                    let end = moment(d2);
                    while(start.isSameOrBefore(end)) {
                        let fdate = formatter.getDateString(start);
                        let file = `${pathBase}${fdate}/${fdate}${fbase}`;
                        files.push(file);
                        start.add(1, period);
                    }
                    return {
                        files: files,
                        filterHandler: new GeotiffFilterHandler(filterOpts)
                    };
                }
            },
            stations: {
                //array of files
                metadata: (fileData, filterOpts) => {
                    let file = `${fileIndex.root}Master_Sta_List_Meta_2020_11_09.csv`;
                    return {
                        files: [file],
                        filterHandler: new CSVFilterHandler(filterOpts)
                    };
                },
                values: (dates, fileData, filterOpts) => {
                    console.log(fileData);
                    //this stuff needs to move
                    let attributes = ["period", "tier", "fill"];
                    let index = new MultiAttributeMap(attributes);
                    let file = `${fileIndex.root}monthly_rf_new_data_1990_2020_FINAL_19dec2020.csv`;
                    index.setData({
                        period: "month",
                        tier: 0,
                        fill: "partial"
                    }, file);
                    file = `${fileIndex.root}Unfilled_Daily_RF_mm_2020_12_31_RF.csv`;
                    index.setData({
                        period: "day",
                        tier: 0,
                        fill: "partial"
                    }, file);
                    file = `${fileIndex.root}Unfilled_Daily_RF_mm_2020_12_31_RF.csv`;
                    index.setData({
                        period: "day",
                        tier: 0,
                        fill: "unfilled"
                    }, file);

                    let data = {
                        period: dates.period,
                        tier: fileData.tier,
                        fill: fileData.fill
                    };
                    console.log(data);
                    let returnFile = index.getValue(data);
                    return {
                        files: [returnFile],
                        filterHandler: new CSVFilterHandler(filterOpts) 
                    };
                }

            }
            
        }
      
    }
}


class DateFormatter {
    constructor(period) {
        let dateFormat = "";
        switch(period) {
            case "day": {
                dateFormat = "_DD";
            }
            case "month": {
                dateFormat = "_MM" + dateFormat;
            }
            case "year": {
                dateFormat = "YYYY" + dateFormat;
                break;
            }
            default: {
                throw Error("Unrecognized period");
            }
        }
        this.dateFormat = dateFormat;
    }

    getDateString(date) {
        let fdate = date.format(this.dateFormat);
        return fdate;
    }

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

    constructor(index) {
        this.index = index;
    }

    getFiles(fileData) {
        let allFiles = [];
        
        for(let item of fileData) {
            let index = this.index.datatype;
            index = index[item.datatype];
            let groupData = item.group;
            let indexer = index[groupData.group][groupData.type];
            let files = indexer(item.dates, item.data, item.filterOpts);
            allFiles.push(files);
        }
        return allFiles
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
            root = root[value];
        }
        return root;
    }
}

//need something to parse index, indexer class should be exported
let indexer = new Indexer(fileIndex);
module.exports = indexer.getFiles.bind(indexer);