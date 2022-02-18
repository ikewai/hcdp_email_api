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


exports.getFiles = getFiles;
exports.getEmpty = getEmpty;


const root = "/data";
const dateFormatter = new DateFormatter();

//then file, date parts
let hierarchy = ["datatype", "production", "aggregation", "period", "extent", "fill", "file"];

function getFiles(data) {
    files = [];
    //DEAL WITH DATES, file types
    for(let item of data) {
        let fpath = root;
        let fname = ""
        let period = item.period;
        let dates = item.range;
        let files = item.files;
        for(let property of hierarchy) {
            let value = item[property];
            if(value !== undefined) {
                path.join(fpath, value);
                fname = `${fname}_${value}`;
            }
        }
        for(let file of files) {
            if(period && dates) {
                
                //add date part of fpath
                //add date part of fname (note separate by file, some aggregated)
            }
        }
    }
}

// helper functs
function shiftPeriod(period, diff) {
    let periodOrder = ["day", "month", "year"];
    let periodIndex = periodOrder.indexOf(period);
    let shiftedPeriodIndex = periodIndex + agg;
    let shiftedPeriod = periodOrder[shiftedPeriodIndex]
    return shiftedPeriod;
}
fu

let fileDetails = {
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

function getFnameEnd(file, period, date) {
    switch(file) {
        case "metadata": {
            break;
        }
    }
}

function getExpandedFnames(datatype, file, properties, range) {
    let fnames = [];
    //expand dates
    let period = range.period;
    let start = range.start;
    let end = range.end;
    let date = new moment(start);
    let endDate = new moment(end);
    while(date.isSameOrBefore(endDate)) {
        let dateData = {
            date: date,
            period: period
        };
        let fname = getFname(datatype, file, properties, dateData);
        if(fname) {
            fnames.append(fname);
        }
        date.add(1, period);
    }
    return fnames;
}
























async function getFiles(data) {
    let fpaths = [];
    for(let item of data) {
        let datatype = item.datatype;
        let files = item.files;
        let properties = item.properties;
        //period and date range, dates inclusive both ends
        let range = item.range;
        for(let file of files) {
            let pathSet = await getPaths(datatype, file, properties, range);
            fpaths = fpaths.concat(pathSet);
        }
    }
    //strip duplicates if exist
    return new Set(fpaths);
}

async function getPaths(datatype, file, properties, range) {
    let fpaths = [];
    //if a tier is specified then set that to theonly item in the check list, otherwise use tier progression
    let tiers = properties.tier ? [properties.tier] : ["archival"];
    let tieredPaths = {};
    for(let tier of tiers) {
        properties.tier = tier;
        let fdir = getFpath(datatype, file, properties, range.period);
        tieredPaths[tier] = fdir;
    }
    let fnames = getFnames(datatype, file, properties, range)
    for(fname of fnames) {
        for(let tier of tiers) {
            let fdir = tieredPaths[tier];
            let fpath = path.join(fdir, fname);
            if(await validateFile(fpath)) {
                fpaths.push(fpath);
                break;
            }
        }     
    }
    return fpaths;
}

////////////////////////////////////////////////////////////////
////////////////// File Path Computations //////////////////////
////////////////////////////////////////////////////////////////

function getFpath(datatype, file, properties, period) {
    let fpath = null;
    basePath = path.join(root, datatype);
    switch(datatype) {
        case "rainfall": {
            fpath = getRainfallFpath(basePath, file, properties, period);
            break;
        }
        case "temperature": {
            fpath = getTemperatureFpath(basePath, file, properties, period);
            break;
        }
    }
    return fpath;
}

function getRainfallFpath(basePath, file, properties, period) {
    let fpath = null;
    basePath = path.join(basePath, properties.production);
    switch(properties.production) {
        case "new": {
            fpath = await getNewRainfallFpath(basePath, file, properties, period);
            break;
        }
        case "legacy": {
            fpath = basePath;
            break;
        }
    }
    return fpath;
}

function getNewRainfallFpath(basePath, file, properties, period) {
    let fpath = null;
    let tier = properties.tier;
    let basePath = path.join(basePath, tier);
    switch(file) {
        case "station_data": {
            fpath = getNewRainfallStationDataFpath(basePath, properties, period);
            break;
        }
        case "kriging_input": {
            fpath = getNewRainfallKrigingFpath(basePath, properties);
            break;
        }
        //note there's no reference to period, yay
        case "raster": {
            fpath = getNewRainfallTiffFpath(basePath, "rf_mm", properties);
            break;
        }
        case "anom": {
            fpath = getNewRainfallTiffFpath(basePath, "anom", properties);
            break;
        }
        case "anom_se": {
            fpath = getNewRainfallTiffFpath(basePath, "anom_SE", properties);
            break;
        }
        case "se": {
            fpath = getNewRainfallTiffFpath(basePath, "rf_mm_SE", properties);
            break;
        }
        case "raster_metadata": {
            fpath = getNewRainfallRasterMetadataFpath(basePath, properties);
            break;
        }
    }
    return fpath;
}

function getNewRainfallStationDataFpath(basePath, properties, period) {
    let extent = properties.extent;
    let fill = properties.fill;
    let fpath = basePath;
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
    fpath = path.join(fpath, "tables/station_data", period, fill);
    if(extent == "statewide") {
        fpath = path.join(fpath, "statewide");
    }
    else {
        //extents in path are uppercased
        extent = extent.toUpperCase();
        fpath = path.join(fpath, "county", extent);
    }
    return fpath;
}

function getNewRainfallKrigingFpath(basePath, properties) {
    let extent = properties.extent;
    let fpath = path.join(basePath, "tables/kriging_input/county");
    //note there is no statewide kriging input
    //extents in path are uppercased
    extent = extent.toUpperCase();
    fpath = path.join(fpath, extent);
    return fpath;
}

function getNewRainfallTiffFpath(basePath, fileSegment, properties) {
    let extent = properties.extent;
    let fpath = path.join(basePath, "tiffs");
    if(extent == "statewide") {
        fpath = path.join(fpath, "statewide", fileSegment);
    }
    else {
        //extents in path are uppercased
        extent = extent.toUpperCase();
        fpath = path.join(fpath, "county", fileSegment, extent);
    }
    return fpath;
}

function getNewRainfallRasterMetadataFpath(basePath, properties) {
    let extent = properties.extent
    let fpath = path.join(basePath, "metadata");
    if(extent == "statewide") {
        fpath = path.join(fpath, "statewide");
    }
    else {
        //extents in path are uppercased
        extent = extent.toUpperCase();
        fpath = path.join(fpath, "county", extent);
    }
    return fpath;
}

function getTemperatureFpath(basePath, file, properties, period) {
    let periodMap = {
        day: "daily",
        month: "monthly"
    };
    let mappedPeriod = periodMap[period];
    basePath = path.join(basePath, mappedPeriod);
    let fpath = null;
    switch(file) {
        case "station_data": {
            //apparently station data has no tiers? This is well designed
            fpath = getTemperatureStationDataFpath(basePath, properties);
            break;
        }
        case "loocv": {
            fpath = getTemperatureTableFpath(basePath, "loocv", properties);
            break;
        }
        case "raster": {
            fpath = getTemperatureTiffFpath(basePath, "temp", properties);
            break;
        }
        case "se": {
            fpath = getTemperatureTiffFpath(basePath, "temp_SE", properties);
            break;
        }
        case "raster_metadata": {
            fpath = getTemperatureTableFpath(basePath, "metadata", properties);
            break;
        }
    }
    return fpath;
}

function getTemperatureStationDataFpath(basePath, properties) {
    let fill = properties.fill;
    let fpath = path.join(basePath, "input/stations/aggregated");
    let fillMap = {
        filled: "serial_complete",
        partial: "partial_filled",
        unfilled: "unfilled"
    };
    fill = fillMap[fill];
    fpath = path.join(fpath, fill);
    return fpath;
}

function getTemperatureTableFpath(basePath, fileSegment, properties) {
    let extent = properties.extent;
    let tier = properties.tier;
    //will the counter on finalRunOutputs01 change??? What is this...
    let fpath = path.join(basePath, "finalRunOutputs01", tier, "tables", fileSegment);
    if(extent == "statewide") {
        fpath = path.join(fpath, "statewide");
    }
    else {
        extent = extent.toUpperCase();
        fpath = path.join(fpath, "county", extent);
    }
    return fpath;
}

function getTemperatureTiffFpath(basePath, fileSegment, properties) {
    let extent = properties.extent;
    let tier = properties.tier;
    let fpath = path.join(basePath, "finalRunOutputs01", tier, "tiffs");
    //no individual county folders here, who needs consistency?
    let extentDir = extent == "statewide" ? "statewide" : "county";
    fpath = path.join(fpath, extentDir, fileSegment);
    return subpath;
}

////////////////////////////////////////////////////////////////
//////////////// End File Path Computations ////////////////////
////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////
////////////////// File Name Computations //////////////////////
////////////////////////////////////////////////////////////////

//expansion types
const Single = Symbol("single");
const Aggregate = Symbol("aggregate");
const Expand = Symbol("expand");

//shouldn't change between datatypes
function getFileExpansion(file) {
    let nonExpandMap = {
        station_metadata: Single,
        station_data: Aggregate
    }
    let expansion = nonExpandMap[file] || Expand;
    return expansion;
}

function getFnames(datatype, file, properties, range) {
    let expansion = getFileExpansion(file);
    let fnames = [];
    switch(expansion) {
        case Expand: {
            fnames = getExpandedFnames(datatype, file, properties, range);
            break;
        }
        case Aggregate: {
            fnames = getAggregatedFnames(datatype, file, properties, range);
            break;
        }
        case Single: {
            let fname = getFname(datatype, file, properties, range);
            if(fname) {
                fnames = [fname];
            }
            break;
        }
    }
    return fnames;
}

function getExpandedFnames(datatype, file, properties, range) {
    let fnames = [];
    //expand dates
    let period = range.period;
    let start = range.start;
    let end = range.end;
    let date = new moment(start);
    let endDate = new moment(end);
    while(date.isSameOrBefore(endDate)) {
        let dateData = {
            date: date,
            period: period
        };
        let fname = getFname(datatype, file, properties, dateData);
        if(fname) {
            fnames.append(fname);
        }
        date.add(1, period);
    }
    return fnames;
}

function getAggregatedFnames(datatype, file, properties, range) {
    let fnames = [];
    let periodOrder = ["day", "month", "year"];
    //get aggregation period
    let period = range.period;
    let periodIndex = periodOrder.indexOf(period);
    let aggregatePeriodIndex = periodIndex + 1;
    aggregatePeriod = periodOrder[aggregatePeriodIndex]
    range.aggregatePeriod = aggregatePeriod;
    //expand dates to the aggregation period
    let start = range.start;
    let end = range.end;
    let date = new moment(start);
    let endDate = new moment(end);
    while(date.isSameOrBefore(endDate)) {
        let dateData = {
            date: date,
            period: period,
            aggregatePeriod: aggregatePeriod
        };
        //differentiation
        let fname = getFname(datatype, file, properties, dateData);
        if(fname) {
            fnames.append(fname);
        }
        date.add(1, aggregatePeriod);
    }
    return fnames;
}


function getFname(datatype, file, properties, dateData) {
    let fname = null;
    switch(datatype) {
        case "rainfall": {
            getRainfallFname(file, properties, dateData);
            break;
        }
        case "temperature": {
            getTemperatureFname(file, properties, dateData);
            break;
        }
    }
    return fname;
}

function getRainfallFname(file, properties, dateData) {
    let fname = null;
    let production = properties.production;
    switch(production) {
        case "legacy": {
            fname = getLegacyRainfallFname(file, properties, dateData);
            break;
        }
        case "new": {
            fname = getNewRainfallFname(file, properties, dateData);
            break;
        }
    }
    return fname;
}

function getLegacyRainfallFname(file, properties, dateData) {
    let fname = null;
    switch(file) {
        case "raster": {
            fname = getLegacyRainfallRasterFname(properties, dateData);
            break;
        }
    }
    return fname;
}

function getLegacyRainfallRasterFname(properties, dateData) {
    let date = dateData.date;
    let period = dateData.period;
    //note all the legacy data is monthly, all need is date
    let formatter = new DateFormatter(period);
    let date_s = formatter.getDateString(date);
    let fname = `MoYrRF_${date_s}.tif`;
    return fname;
}

function getNewRainfallFname(file, properties, dateData) {
    let fname = null;
    switch(file) {
        case "station_metadata": {
            fname = getNewRainfallStationMetadataFname();
        }
        case "station_data": {
            fname = getNewRainfallStationDataFname(properties, dateData);
            break;
        }
        case "kriging_input": {
            fname = getNewRainfallKrigingFname(properties, dateData);
            break;
        }
        case "raster": {
            fname = getNewRainfallTiffFname("rf_mm", properties, dateData);
            break;
        }
        case "anom": {
            fname = getNewRainfallTiffFname("anom", properties, dateData);
            break;
        }
        case "anom_se": {
            fname = getNewRainfallTiffFname("anom_SE", properties, dateData);
            break;
        }
        case "se": {
            fname = getNewRainfallTiffFname("rf_mm_SE", properties, dateData);
            break;
        }
        case "raster_metadata": {
            fname = getNewRainfallRasterMetadataFname(properties, dateData);
            break;
        }
    }

    return fname;
}

function getNewRainfallStationMetadataFname() {
    return "Master_Sta_List_Meta_2020_11_09.csv";
}

function getNewRainfallStationDataFname(properties, dateData) {
    let period = dateData.period;
    let date = dateData.date;
    let aggregatePeriod = dateData.aggregatePeriod;
    let extent = properties.extent;
    let fill = properties.fill;
    let date_s = dateFormatter.getDateString(aggregatePeriod, date);
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
    let fname = `${extent}_${fill}_${period}_RF_mm_${date_s}.csv`;
    return fname;
}

function getNewRainfallKrigingFname(properties, dateData) {
    let period = dateData.period;
    let date = dateData.date;
    let extent = properties.extent;
    let date_s = dateFormatter.getDateString(period, date);
    //extent should be the same (all lowercase in this case for some reason)
    fname = `${date_s}_${extent}_rf_krig_input.csv`;
    return fname;
}

function getNewRainfallTiffFname(fileSegment, properties, dateData) {
    let period = dateData.period;
    let date = dateData.date;
    let extent = properties.extent;
    let date_s = dateFormatter.getDateString(period, date);
    //extent should be the same (all lowercase in this case for some reason)
    let fname = `${date_s}_${extent}_${fileSegment}.tif`;
    return fname;
}

function getNewRainfallRasterMetadataFname(properties, dateData) {
    let period = dateData.period;
    let date = dateData.date;
    let extent = properties.extent;
    let date_s = dateFormatter.getDateString(period, date);
    //extent should be the same (all lowercase in this case for some reason)
    let fname = `${date_s}_${extent}_rf_mm_meta.txt`;
    return fname;
}

function getTemperatureFname(file, properties, dateData) {
    let fname = null;
    switch(file) {
        case "station_metadata": {
            fname = getTemperatureStationMetadataFname();
        }
        case "station_data": {
            fname = getTemperatureStationDataFname(properties.extent, properties.fill, properties.period);
            break;
        }
        case "loocv": {
            fname = getTemperatureLOOCVFname(properties, dateData);
            break;
        }
        case "raster": {
            fname = getTemperatureRasterFname(properties, dateData);
            break;
        }
        case "se": {
            fname = getTemperatureSEFname(properties, dateData);
            break;
        }
        case "raster_metadata": {
            fname = getTemperatureRasterMetadataFname(properties, dateData);
            break;
        }
    }
    return fname;
}

function getTemperatureStationMetadataFname() {
    return "Master_Sta_List_Meta_2020_11_09.csv";
}

function getTemperatureStationDataFname(properties, dateData) {
    let period = dateData.period;
    let date = dateData.date;
    let aggregatePeriod = dateData.aggregatePeriod;
    let aggregation = properties.aggregation;
    let date_s = dateFormatter.getDateString(aggregatePeriod, date);
    let periodMap = {
        day: "daily",
        month: "montly"
    };
    period = periodMap[period];
    let fname = `${period}_T${aggregation}_${date_s}.csv`;
    return fname;
}

function getTemperatureLOOCVFname(properties, dateData) {
    let period = dateData.period;
    let date = dateData.date;
    let aggregation = properties.aggregation;
    let extent = properties.extent;
    let date_s = dateFormatter.getDateString(period, date, "");
    let extentMap = {
        statewide: "state",
        bi: "BI",
        ka: "KA",
        mn: "MN",
        oa: "OA"
    };
    extent = extentMap[extent];

    let fname = `${date_s}_T${aggregation}_${extent}_loocv.csv`;
    return fname;
}

function getTemperatureRasterMetadataFname(properties, dateData) {
    let period = dateData.period;
    let date = dateData.date;
    let aggregation = properties.aggregation;
    let extent = properties.extent;
    let date_s = dateFormatter.getDateString(period, date, "");
    let extentMap = {
        statewide: "state",
        bi: "BI",
        ka: "KA",
        mn: "MN",
        oa: "OA"
    };
    extent = extentMap[extent];
    let fname = `${date_s}_T${aggregation}_${extent}_meta.txt`;
    return fname;
}

function getTemperatureRasterFname(properties, dateData) {
    let period = dateData.period;
    let date = dateData.date;
    let aggregation = properties.aggregation;
    let extent = properties.extent;
    let date_s = dateFormatter.getDateString(period, date, "");
    let extentMap = {
        statewide: "state",
        bi: "BI",
        ka: "KA",
        mn: "MN",
        oa: "OA"
    };
    extent = extentMap[extent];
    let fname = `T${aggregation}_map_${extent}_${date_s}.tif`;
    return fname;
}

function getTemperatureSEFname(properties, dateData) {
    let period = dateData.period;
    let date = dateData.date;
    let aggregation = properties.aggregation;
    let extent = properties.extent;
    let date_s = dateFormatter.getDateString(period, date, "");
    let extentMap = {
        statewide: "state",
        bi: "BI",
        ka: "KA",
        mn: "MN",
        oa: "OA"
    };
    extent = extentMap[extent];
    let fname = `T${aggregation}_map_${extent}_${date_s}_se.tif`;
    return fname;
}


////////////////////////////////////////////////////////////////
/////////////// End File Name Computations /////////////////////
////////////////////////////////////////////////////////////////


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


const emptyIndex = {
    statewide: "/data/empty/statewide_hi_NA.tif"
};
//add folder with empty geotiffs for extents
function getEmpty(extent) {
    let emptyFile = emptyIndex.get(extent) || null;
    return emptyFile;
}