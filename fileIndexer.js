const moment = require("moment");
const fs = require("fs");
const path = require("path");


//data root
const root = "/data/production";
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


async function getFilesWildcard(data) {

}

function createDateGroups(period, range) {
    let dateGroups = {};
    let start = range.start;
    let end = range.end;
    let startDate = new moment(start);
    let endDate = new moment(end);

    uncoveredDates = [startDate, endDate, null, null];

    periods = ["year", "month", "day"];
    group = periods[0];
    for(let i = 0; i < periods.length; i++) {
        
        let data = getGroupsBetween(uncoveredDates[0], uncoveredDates[1], group);
        dateGroups[group] = data.periods;
        uncoveredDates[1] = data.coverage[0];

        if(uncoveredDates[2]) {
            data = getGroupsBetween(uncoveredDates[2], uncoveredDates[3], group);
            dateGroups[group] = dateGroups[group].concat(data.periods);
        }

        uncoveredDates[2] = data.coverage[1];
    }

    return dates;
}

function getGroupsBetween(start, end, period) {
    periods = [];
    coverage = [];
    date = start.clone();
    //move to start of period
    date.startOf(period);
    //if start of period is same as start date start there, otherwise advance by one period (initial not fully covered)
    if(!date.isSame(start, period)) {
        date.add(1, year);
    }
    coverage.push(date.clone());
    //need to see if period completely enclosed, so go to end of period
    date.endOf(period);
    //get 
    while(date.isSameOrBefore(end)) {
        let clone = date.clone();
        //go to the start for simplicity (zero out lower properties)
        clone.startOf(period);
        periods.push(clone);
        date.add(1, period);
    }
    //end of coverage (exclusive)
    date.startOf(period);
    coverage.push(date);
    //note coverage is [)
    data = {
        periods,
        coverage
    };
    return covered;
}




async function getFiles(data) {
    let files = [];
    //at least for now just catchall and return files found before failure, maybe add more catching/skipping later, or 400?
    try {
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
                    fdir = path.join(fdir, value);
                    fname = `${fname}_${value}`;
                }
            }
            if(period && range) {
                //expand out dates
                dates = expandDates(period, range);
                for(date of dates) {
                    for(let ftype of ftypes) {
                        let dirPeriod = shiftPeriod(period, 1);
                        //add file and date part of fdir
                        let fdirComplete = path.join(fdir, ftype, createDateString(date, dirPeriod, "/"));
                        //add fname end to fname
                        let fnameComplete = `${fname}_${getFnameEnd(ftype, period, date)}`;
                        //strip leading underscore
                        fnameComplete = fnameComplete.substring(1);
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
                    //strip leading underscore
                    fnameComplete = fnameComplete.substring(1);
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
    catch(e) {}
    return files;
}

//add folder with empty geotiffs for extents
function getEmpty(extent) {
    let emptyFile = emptyIndex[extent] || null;
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