const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');

class MesonetDataPackager {
    constructor(root, varData, stationData, combine, ftype, csvMode) {
        const uuid = uuidv4();
        const packageDir = path.join(root, uuid);
        this.packageDir = packageDir;
        this.combine = combine === undefined ? false : combine;
        this.ftype = ftype || "csv";
        this.csvMode = csvMode || "matrix";
        this.files = [];
        this.varData = varData;
        this.stationData = stationData;

        if(!fs.existsSync(packageDir)) {
            fs.mkdirSync(packageDir, { recursive: true });
        }
    }

    async createHandle(fname, varData) {
        let outFile = path.join(this.packageDir, fname);
        if(this.handle) {
            await this.handle.complete();
        }
        if(this.ftype === "json") {
            this.handle = new JSONFileHandle(outFile);
        }
        else {
            this.handle = this.csvMode == "table" ? new CSVTableFileHandle(outFile, varData, this.stationData) : new CSVMatrixFileHandle(outFile, varData, this.stationData);
        }
        
        this.files.push(outFile);
    }

    async write(stationID, measurements) {
        if(!this.combine && (this.stationID === undefined || this.stationID != stationID)) {
            this.stationID = stationID;
            let fname = `${stationID}.${this.ftype}`;
            await this.createHandle(fname, this.varData[stationID]);
        }
        else if(this.handle === undefined) {
            let fname = `data.${this.ftype}`;
            await this.createHandle(fname, this.varData);
        }
        return this.handle.write(stationID, measurements);
    }

    async complete() {
        if(this.handle) {
            await this.handle.complete();
        }
        return this.files;
    }
}

class PackageFileHandle {
    constructor(outFile) {
        this.stream = fs.createWriteStream(outFile)
        this.ready = new Promise((resolve) => {
            this.stream.once("open", (fd) => {
                resolve();
            });
        });
    }

    async write(data) {
        await this.ready;
        if(!this.stream.write(data)) {
            this.ready = new Promise((resolve) => {
                this.stream.once("drain", () => {
                    resolve();
                });
            });
        }
        return;
    }

    async close(final) {
        await this.ready;
        return new Promise((resolve) => {
            this.stream.end(final, () => {
                resolve();
            });
        })
    }
}


class JSONFileHandle extends PackageFileHandle {
    constructor(outFile) {
        super(outFile);
        super.write("{");
        this.first = true;
    }

    //needs to be provided in chunks of same id to be combined
    async write(stationID, measurements) {
        if(stationID === this.currentStationID) {
            this.measurements = this._addMeasurements(measurements);
        }
        else {
            await this._dumpStationMeasurements();
            this.first = false;
            this.currentStationID = stationID;
            this.measurements = measurements;
        }
    }

    async complete() {
        return this.close("}");
    }

    _dumpStationMeasurements(stationID) {
        let measurements = {};
        measurements[stationID] = this.measurements;
        let data = JSON.stringify(measurements, null, 4);
        data.replace("\n", "\n    ");
        data = "\n    " + data;
        if(!this.first) {
            data = "," + data
        }
        return super.write(data);
    }

    _addMeasurements(measurements) {
        for(let variable in measurements) {
            if(this.measurements[variable] === undefined) {
                this.measurements[variable] = {};
            }
            for(let timestamp in measurements[variable]) {
                this.measurements[variable][timestamp] = measurements[variable][timestamp];
            }
        }
    }
}

class CSVMatrixFileHandle extends PackageFileHandle {
    constructor(outFile, varData, stationData) {
        super(outFile);
        this.stationData = stationData;
        this.varData = varData;
        this.writeHeader();
    }

    async writeHeader() {
        let varIDs = new Object.keys(this.varData);
        let varNames = this.varIDs.map((id) => {
          return this.varData[id].var_name;
        });
        let units = varIDs.map((id) => {
          return this.varData[id].unit;
        });
        await super.write(`,,,"${varIDs.join("\",\"")}"\n`);
        await super.write(`,,,"${units.join("\",\"")}"\n`);
        await super.write(`station_name,station_id,timestamp,"${varNames.join("\",\"")}"\n`);
    }

    async write(stationID, measurements) {
        let stationName = this.stationData[station].site_name;
        let rows = {};
        for(let i = 0; i < varIDs.length; i++) {
            let variable = varIDs[i];
            let values = measurements[station][variable] || {};
            for(let timestamp in values) {
                let row = rows[timestamp];
                if(row === undefined) {
                    row = [stationName, stationID, timestamp].concat(new Array(varIDs.length).fill("NA"));
                    rows[timestamp] = row;
                }
                row[i + 3] = (values[timestamp] || "NA").toString();
            }
        }
        let rowData = Object.values(rows).sort((a, b) => {
            return a[2] < b[2] ? -1 : 1;
        });
        for(let row of rowData) {
            await super.write(`"${row.join("\",\"")}"\n`);
        }
    }

    async complete() {
        return this.close();
    }
}

class CSVTableFileHandle extends PackageFileHandle {
    constructor(outFile, varData, stationData) {
        super(outFile);
        this.stationData = stationData;
        this.varData = varData;
        this.writeHeader();
    }

    async writeHeader() {
        await super.write("station_name,station_id,timestamp,variable_name,variable_id,unit,value\n");
    }

    async write(stationID, measurements) {
        for(let variable in measurements) {
            for(let timestamp in measurements[variable]) {
                await super.write(`${this.stationData[stationID].site_name},${stationID},${timestamp},${this.varData[variable].var_name},${variable},${this.varData[variable].unit},${measurements[variable][timestamp] || "NA"}\n`);
            }
        }
    }

    async complete() {
        return this.close();
    }
}

export default MesonetDataPackager;