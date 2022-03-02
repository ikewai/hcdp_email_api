const moment = require("moment");
const fs = require("fs");
const path = require("path");


//data root
const root = "/data";
//property hierarchy (followed by file and date parts)
const hierarchy = ["datatype", "production", "aggregation", "period", "extent", "fill"];
//details on file name period aggregations and file extensions
const fileDetails = {
    metadata: {
        agg: 0,
        ext: "txt"
    },
    data_map: {
        agg: 0,
        ext: "tif"
    },
    se: {
        agg: 0,
        ext: "tif"
    },
    anom: {
        agg: 0,
        ext: "tif"
    },
    anom_se: {
        agg: 0,
        ext: "tif"
    },
    station_metadata: {
        agg: null,
        ext: "csv"
    },
    station_data: {
        agg: 1,
        ext: "csv"
    }
}
//empty file index
const emptyIndex = {
    statewide: "/data/empty/statewide_hi_NA.tif"
};

async function getFiles(data) {
    let files = [];
    for(let item of data) {
        let fdir = root;
        let fname = ""
        let period = item.period;
        let range = item.range;
        let ftypes = item.files;
        //add properties to path in order of hierarchy
        for(let property of hierarchy) {
            let value = item[property];
            if(value !== undefined) {
                path.join(fdir, value);
                fname = `${fname}_${value}`;
            }
        }
        if(period && range) {
            //expand out dates
            dates = expandDates(period, range);
            for(date of dates) {
                for(let ftype of ftypes) {
                    //add file and date part of fdir
                    let fdirComplete = path.join(fdir, ftype, createDateString(date, period, "/"));
                    //add fname end to fname
                    let fnameComplete = `${fname}_${getFnameEnd(ftype, period, date)}`;
                    //construct complete file path
                    let fpath = path.join(fdirComplete, fnameComplete);
                    //validate file exists and push to file list if it does
                    if(await validateFile(fpath)) {
                        files.push(fpath);
                    }
                }
            } 
        }
        //no date component
        else {
            for(let ftype of ftypes) {
                //add file part to path
                let fdirComplete = path.join(fdir, ftype);
                //add fname end to fname
                let fnameComplete = `${fname}_${getFnameEnd(ftype, undefined, undefined)}`;
                //construct complete file path
                let fpath = path.join(fdirComplete, fnameComplete);
                //validate file exists and append to file list if it does
                if(await validateFile(fpath)) {
                    files.push(fpath);
                }
            }
        }
        
    }
}

//add folder with empty geotiffs for extents
function getEmpty(extent) {
    let emptyFile = emptyIndex.get(extent) || null;
    return emptyFile;
}

/////////////////////////////////////////////////
///////////////// helper functs /////////////////
/////////////////////////////////////////////////

//shift period by diff levels
function shiftPeriod(period, diff) {
    let periodOrder = ["day", "month", "year"];
    let periodIndex = periodOrder.indexOf(period);
    let shiftedPeriodIndex = periodIndex + diff;
    let shiftedPeriod = periodOrder[shiftedPeriodIndex]
    return shiftedPeriod;
}

//format date string using period and delimeter
function createDateString(date, period, delim) {
    let dateFormat = "";
    switch(period) {
        case "day": {
            dateFormat = `${delim}DD`;
        }
        case "month": {
            dateFormat = `${delim}MM${dateFormat}`;
        }
        case "year": {
            dateFormat = `YYYY${dateFormat}`;
            break;
        }
        default: {
            throw Error("Unrecognized period");
        }
    }

    let fdate = date.format(dateFormat);
    return fdate;
}

//get the end portion of file name
function getFnameEnd(file, period, date) {
    console.log(file);
    let details = fileDetails[file];
    let fnameEnd = file;
    let agg = details.agg;
    if(agg !== null) {
        aggPeriod = agg == 0 ? period : shiftPeriod(period, agg);
        datePart = createDateString(date, aggPeriod, "_")
        fnameEnd += `_${datePart}`;
    }
    fnameEnd += `.${details.ext}`;
    return fnameEnd;
}

//expand a group of date strings and wrap in moments
function expandDates(period, range) {
    let dates = [];
    let start = range.start;
    let end = range.end;
    let date = new moment(start);
    let endDate = new moment(end);
    while(date.isSameOrBefore(endDate)) {
        let clone = date.clone();
        dates.push(clone);
        date.add(1, period);
    }
    return dates;
}

//validate file exists
async function validateFile(file) {
    return new Promise((resolve, reject) => {
        fs.lstat(file, (e, stats) => {
            if(e) {
                resolve(false);
            }
            else if(stats.isFile()) {
                resolve(true);
            }
            else {
                resolve(false);
            }
        });
    });
}

exports.getFiles = getFiles;
exports.getEmpty = getEmpty;