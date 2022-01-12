const moment = require("moment");
const fs = require("fs");
const path = require("path");




class DateFormatter {
    constructor() {}

    getDateString(period, date, delimiter = "_") {
        let dateFormat = "";
        switch(period) {
            case "day": {
                dateFormat = `${delimiter}DD`;
            }
            case "month": {
                dateFormat = `${delimiter}MM` + dateFormat;
            }
            case "year": {
                dateFormat = "YYYY" + dateFormat;
                break;
            }
            default: {
                throw Error("Unrecognized period");
            }
        }
        let dateFormat = dateFormat;

        let fdate = date.format(dateFormat);
        return fdate;
    }

}









//need something to parse index, indexer class should be exported
let indexer = new Indexer(fileIndex);
module.exports = indexer.getFiles.bind(indexer);

//datatype
//file type (raster, station_data, loocv, etc)

const root = "/data";

async function getFiles(data) {
    let fpaths = [];
    for(let item of data) {
        let datatype = item.datatype;
        let files = item.files;
        let properties = item.properties;
        //period and date range, dates inclusive both ends
        let range = item.range;
        for(let file of files) {
            let pathSet = getPaths(datatype, file, properties, range);
            fpaths = fpaths.concat(pathSet);
        }
    }
    //strip duplicates if exist
    return new Set(fpaths);
}


async function getPaths(datatype, file, properties, range) {
    let fpaths = [];

    let path = await getFpath(datatype, file, properties, range);
    if(path) {
        //expand dates
        let period = range.period;
        let start = range.start;
        let end = range.end;
        let date = new moment(start);
        let endDate = new moment(end);
        while(date.isSameOrBefore(endDate)) {
            let name = getFname(datatype, file, properties, date, period);
            if(name) {
                let fpath = path.join(path, name);
                try {
                    let type = await validateFile(fpath);
                    //make sure it is a file
                    if(type == "file") {
                        //append to list
                        fpaths.push(fpath);
                    }
                }
                //make sure the file exists
                catch {}
            }
            date.add(1, period);
        }
    }
    
    return fpaths;
}

//NEED TO CHECK IF DATE MATTERS

async function getFpath(datatype, file, properties, range) {
    let fpath = null;
    switch(datatype) {
        case "rainfall": {
            getRainfallFpath(file, properties, range);
            break;
        }
        case "temperature": {
            getTemperatureFpath(file, properties, range);
            break;
        }
    }
    return fpath;
}

async function getFname(datatype, file, properties, range) {
    let fname = null;
    switch(datatype) {
        case "rainfall": {
            getRainfallFname(file, properties, range);
            break;
        }
        case "temperature": {
            getTemperatureFname(file, properties, range);
            break;
        }
    }
    return fname;
}

async function getRainfallFpath(file, properties, range) {
    let fpath = path.join(root, "rainfall");
}

async function getTemperatureFpath(file, properties, range) {
    let fpath = path.join(root, "temperature");
}

async function getRainfallFname(file, properties, range) {
    switch(file) {

    }
}

async function getTemperatureFname(file, properties, range) {

}





{

    let fpath = path.join(root, datatype);

    //station metadata should be global to a whole datatype, place at top level (may have to change)
    if(properties.file == "station_metadata") {
        fpath = path.join(fpath, "Master_Sta_List_Meta_2020_11_09.csv");
    }
    else {
        switch(datatype) {
            case "rainfall": {
                fpath = getRainfallPath(fpath, properties);
                break;
            }
            case "temperature": {
                fpath = getTemperaturePath(fpath, properties);
                break;
            }
            default: {
                fpath = null;
            }
        }
    }

    //validate path is a valid file otherwise set to null
    if(fpath) {
        try {
            let type = await validateFile(fpath);
            if
            break;
        }
        //path is invalid, set to null
        catch {
            fpath = null;
        }
    }
   

    return fpath;
}




///////////////////////////////////////////////////////////////////
///////////////// Path Computations ///////////////////////////////
///////////////////////////////////////////////////////////////////

function getLegacyRainfallFname(period, date) {
    //note all the legacy data is monthly, all need is date
    let formatter = new DateFormatter("month");
    let date_s = formatter.getDateString(date);
    let fname = `MoYrRF_${date_s}.tif`;
    return fname;
}



async function getRainfallPath(basePath, properties) {
    let fpath = null;
    basePath = path.join(basePath, properties.production);
    switch(properties.production) {
        case "new": {
            fpath = await getNewRainfallPath(basePath, properties);
            break;
        }
        case "legacy": {
            fpath = basePath;
            break;
        }
    }
    return fpath;
}




function getNewRainfallTiffSubpath(fileSegment, extent) {
    let subpath = "tiffs";
    if(extent == "statewide") {
        subpath = path.join(subpath, "statewide", fileSegment);
    }
    else {
        //extents in path are uppercased
        extent = extent.toUpperCase();
        subpath = path.join(subpath, "county", fileSegment, extent);
    }
    return subpath;
}

function getNewRainfallTiffFname(fileSegment, extent, period, date) {
    let formatter = new DateFormatter(period);
    //date should be a moment
    let date_s = formatter.getDateString(date);
    //extent should be the same (all lowercase in this case for some reason)
    fname = `${date_s}_${extent}_${fileSegment}.tif`;
    return fname;
}

function getNewRainfallKrigingSubpath(extent) {
    let subpath = "tables/kriging_input/county";
    //note there is no statewide kriging input
    //extents in path are uppercased
    extent = extent.toUpperCase();
    subpath = path.join(subpath, extent);
    return subpath;
}

function getNewRainfallKrigingFname(extent, period, date) {
    let formatter = new DateFormatter(period);
    //date should be a moment
    let date_s = formatter.getDateString(date);
    //extent should be the same (all lowercase in this case for some reason)
    fname = `${date_s}_${extent}_rf_krig_input.csv`;
    return fname;
}

function getNewRainfallRasterMetadataSubpath(extent) {
    let subpath = "metadata";
    if(extent == "statewide") {
        subpath = path.join(subpath, "statewide");
    }
    else {
        //extents in path are uppercased
        extent = extent.toUpperCase();
        subpath = path.join(subpath, "county", extent);
    }
    return subpath;
}

function getNewRainfallRasterMetadataFname(extent, period, date) {
    let formatter = new DateFormatter(period);
    //date should be a moment
    let date_s = formatter.getDateString(date);
    //extent should be the same (all lowercase in this case for some reason)
    fname = `${date_s}_${extent}_rf_mm_meta.txt`;
    return fname;
}

function getNewRainfallStationDataSubpath(extent, fill, period) {
    let periodMap = {
        day: "daily",
        month: "monthly"
    };
    let fillMap = {
        partial: "partial_filled",
        unfilled: "raw"
    };
    period = periodMap[period];
    fill = fillMap[fill];
    subpath = path.join("tables/station_data", period, fill);
    if(extent == "statewide") {
        subpath = path.join(subpath, "statewide");
    }
    else {
        //extents in path are uppercased
        extent = extent.toUpperCase();
        subpath = path.join(subpath, "county", extent);
    }
    return subpath;
}

//remember to chunk this by the next time period up
function getNewRainfallStationDataFname(extent, fill, period, periodGroup, date) {
    // let periods = ["day", "month", "year"];
    // let periodGroupIndex = periods.indexOf(period) + 1;
    // let periodGroup = periods[periodGroupIndex];

    let formatter = new DateFormatter(periodGroup);
    //date should be a moment
    let date_s = formatter.getDateString(date);

    let periodMap = {
        day: "Daily",
        month: "Monthly"
    };
    let fillMap = {
        partial: "Partial_Filled",
        unfilled: "Raw"
    };
    let extentMap = {
        statewide: "Statewide",
        bi: "BI",
        ka: "KA",
        mn: "MN",
        oa: "OA"
    };

    period = periodMap[period];
    fill = fillMap[fill];
    extent = extentMap[extent];
    
    //extent should be the same (all lowercase in this case for some reason)
    fname = `${extent}_${fill}_${period}_RF_mm_${date_s}.csv`;
    return fname;
}





async function getNewRainfallPath(basePath, properties) {
    let fpath = null;
    //allow data tier to be specified, if not specified get latest tier
    const tiers = properties.tier ? [properties.tier] : ["archival"];
    //construct subpath
    let subpath = "";
    switch(properties.file) {
        case "station_data": {
            subpath = getNewRainfallStationDataSubpath(properties.extent, properties.fill, properties.period);
            break;
        }
        case "kriging_input": {
            subpath = getNewRainfallKrigingSubpath(properties.extent);
            break;
        }
        //note there's no reference to period, yay
        case "raster": {
            subpath = getNewRainfallTiffSubpath("rf_mm", properties.extent);
            break;
        }
        case "anom": {
            subpath = getNewRainfallTiffSubpath("anom", properties.extent);
            break;
        }
        case "anom_se": {
            subpath = getNewRainfallTiffSubpath("anom_SE", properties.extent);
            break;
        }
        case "se": {
            subpath = getNewRainfallTiffSubpath("rf_mm_SE", properties.extent);
            break;
        }
        case "raster_metadata": {
            subpath = getNewRainfallRasterMetadataSubpath(properties.extent);
            break;
        }
    }
    fpath = await getLinkWithTier(basePath, subpath, tiers);
    return fpath;
}



//////////////////////////////////////////////////////////////////////
////////////////////// temperature ///////////////////////////////////
//////////////////////////////////////////////////////////////////////

function getTemperatureTiffSubpath(fileSegment, extent) {
    let subpath = "tiffs";
    if(extent == "statewide") {
        subpath = path.join(typeSegment, "statewide", fileSegment);
    }
    else {
        subpath = path.join(typeSegment, "county", fileSegment);
    }
    return subpath;
}

function getTemperatureTableSubpath(fileSegment, extent) {
    let subpath = "tables";
    if(extent == "statewide") {
        subpath = path.join(fileSegment, "statewide");
    }
    else {
        subpath = path.join(fileSegment, "county");
    }
    return subpath;
}

function getTemperatureStationDataSubpath(fill) {
    let subpath = "input/stations/aggregated";
    let fillMap = {
        filled: "serial_complete",
        partial: "partial_filled",
        unfilled: "unfilled"
    };
    fill = fillMap[fill];
    subpath = path.join(subpath, fill);
    return subpath;
}


async function getTemperaturePath(basePath, properties) {
    let periodMap = {
        day: "daily",
        month: "monthly"
    };
    let period = properties.period;
    period = periodMap[period];
    basePath = path.join(basePath, period);
    let fpath = null;
    //allow data tier to be specified, if not specified get latest tier
    const tiers = properties.tier ? [properties.tier] : ["archival"];
    //construct subpath
    let subpath = "";
    switch(properties.file) {
        case "station_data": {
            //apparently station data has no tiers? This is well designed
            subpath = getTemperatureStationDataSubpath(properties.extent, properties.fill, properties.period);
            fpath = path.join(basePath, subpath);
            break;
        }
        case "loocv": {
            //will the counter on this change??? What is this...
            basePath = path.join(basePath, "finalRunOutputs01");
            subpath = getTemperatureTableSubpath("loocv", properties.extent);
            fpath = await getLinkWithTier(basePath, subpath, tiers);
            break;
        }
        case "raster": {
            basePath = path.join(basePath, "finalRunOutputs01");
            subpath = getTemperatureTiffSubpath("temp", properties.extent);
            fpath = await getLinkWithTier(basePath, subpath, tiers);
            break;
        }
        case "se": {
            basePath = path.join(basePath, "finalRunOutputs01");
            subpath = getTemperatureTiffSubpath("rf_mm_SE", properties.extent);
            fpath = await getLinkWithTier(basePath, subpath, tiers);
            break;
        }
        case "raster_metadata": {
            basePath = path.join(basePath, "finalRunOutputs01");
            subpath = getTemperatureTableSubpath("metadata", properties.extent);
            fpath = await getLinkWithTier(basePath, subpath, tiers);
            break;
        }
    }
    return fpath;
}

///////////////////////////////////////////////////////////////////
///////////// End Path Computations ///////////////////////////////
///////////////////////////////////////////////////////////////////



async function getLinkWithTier(basePath, subpath, tiers) {
    let fpath = null
    for(let tier in tiers) {
        let fpath = `${basePath}${tier}/${subpath}`;
        //check if file exists, set and break if it does
        try {
            await validateFile(file);
            fpath = fpath
            break;
        }
        //continue if file does not exist
        catch { }
    }

    return fpath;
}

//reject if invalid path, otherwise resolve with type
async function validateFile(file) {
    return new Promise((resolve, reject) => {
        fs.lstat(file, (e, stats) => {
            if(e) {
                reject(e);
            }
            else if(stats.isFile()) {
                resolve("file");
            }
            else if(stats.isDirectory()) {
                resolve("dir");
            }
            else {
                resolve("other");
            }
        });
    });
}


const emptyIndex = {
    statewide: "/data/empty/statewide_hi_NA.tif"
};
//add folder with empty geotiffs for extents
function getEmpty(extent) {
    let emptyFile = emptyIndex.get(extent);
    return emptyFile;
}