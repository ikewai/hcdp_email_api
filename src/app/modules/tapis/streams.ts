const https = require("https");
const moment = require("moment-timezone");
import { TapisManager } from "../tapisHandlers";
import { TapisV3Auth } from "./auth";

class RequestHandler {
    private retryLimit: number;
    private auth: TapisV3Auth;

    constructor(retryLimit: number, auth: TapisV3Auth) {
        this.retryLimit = retryLimit;
        this.auth = auth;
    }

    public async submitRequest(url: string, options = {}, body: any = null, retries = this.retryLimit) {
        if(retries === undefined) {
            retries = this.retryLimit;
        }
        //get token from auth promise
        let token = await this.auth.getToken();

        // Set options for request
        options = {
            method: 'GET',
            headers: {
                'X-Tapis-Token': token,
            },
            ...options
        };

        // Return a promise for asynchronous measurements retrieval
        return new Promise((resolve, reject) => {
            const retry = (code: number, err) => {
                if(retries < 1) {
                    const errorOut = {
                        status: code,
                        reason: `The query resulted in an error: ${err}`
                    }
                    reject(errorOut);
                }
                else {
                    resolve(this.submitRequest(url, options, body, retries - 1));
                }
            }

            // Initiate the measurements request
            const req = https.request(url, options, (res) => {
                res.setEncoding('utf8');
                let data = '';

                // Accumulate response data
                res.on('data', (chunk) => {
                    data += chunk;
                });

                // When response is complete
                res.on('end', () => {
                    try {
                        // If retrieval is successful resolve with data parsed as JSON
                        if(res.statusCode === 200) {
                            const parsedResponse = JSON.parse(data);
                            resolve(parsedResponse.result);
                        }
                        //if failed but the query was just empty return null and let caller fill default
                        else if(res.statusCode == 500 && (data.includes("Unrecognized exception type: <class 'KeyError'>") || data.includes("Unrecognized exception type: <class 'pandas.errors.EmptyDataError'>"))) {
                            resolve(null);
                        }
                        else {
                            throw new Error();
                        }
                        
                    }
                    catch(error) {
                        retry(res.statusCode || 500, data);
                    }
                });
            });

            // Handle errors in the measurements request
            req.on('error', (error) => {
                retry(500, error);
            });

            if(body !== null) {
                req.write(body);
            }
            req.end();
        });
    }
}


export class ProjectHandler {
    private tz: string;
    private projectID: string;
    private requestHandler: RequestHandler;
    private tenantURL: string;
    private v2Manager: TapisManager;

    constructor(projectID: string, timezone: string, tenantURL: string, requestHandler: RequestHandler, v2Manager: TapisManager) {
        this.tz = timezone;
        this.projectID = projectID;
        this.tenantURL = tenantURL;
        this.requestHandler = requestHandler;
        this.v2Manager = v2Manager;
    }

    private localizeTimestamp(timestamp: string) {
        let converted = new moment(timestamp).tz(this.tz);
        return converted.format();
    }

    private getInstID(stationID: string, type: string) {
        return `${this.projectID}_${stationID}_${type}`;
    }

    private encodeURLParams(params: {[key: string]: string}) {
        let encoded = "";
        const queryParams: string[] = [];
        for(const key in params) {
            queryParams.push(`${key}=${encodeURIComponent(params[key])}`);
        }
        if(queryParams.length > 0) {
            encoded = `?${queryParams.join('&')}`;
        }
        return encoded;
    }

    async listMeasurements(stationID: string, options: {[key: string]: string}) {
        let instID = this.getInstID(stationID, "measurements");
        return this.listValues(instID, stationID, options);
    }

    async listVariables(stationID: string) {
        let instID = this.getInstID(stationID, "measurements");
        // Construct URL for measurements request
        let url = `${this.tenantURL}/v3/streams/projects/${this.projectID}/sites/${stationID}/instruments/${instID}/variables`;
        let res = await this.requestHandler.submitRequest(url) || [];
        return res;
    } 

    async listStations(): Promise<any[]> {
        // Construct URL for request
        let url = `${this.tenantURL}/v3/streams/projects/${this.projectID}/sites`;
        let res: any[] = <any[]>(await this.requestHandler.submitRequest(url)) || [];
        return res;
    }


    //register the flag, then when set a flag check if var exists (only need id), if doesn't create
    async registerFlag(id: string, name: string, description: string, defaultValue: number = 0, relatedVars: string[] = [], update: boolean = true) {
        await this.v2Manager.checkCreate("mesonet_flag", {
            id
        }, {
            id,
            name,
            description,
            defaultValue,
            relatedVars
        }, update);
    }

    //maybe make one flag instrument and each flag as a variable actually
    //need to create flag instruments when station created
    async setFlag(stationID: string, flagID: string, datetime: string, value: number) {
        let instID = this.getInstID(stationID, "flags");
        let url = `${this.tenantURL}/v3/streams/projects/${this.projectID}/sites/${stationID}/instruments/variables`;

        let flagRecord: any = await this.v2Manager.getMatches(flagID, {
            id: flagID
        });
        if(flagRecord.length > 0) {
            flagRecord = flagRecord[0].value;
        }
        else {
            //flag has not been registered, throw some kind of error
            return Promise.reject({
                status: 404,
                reason: "No flag with the provided ID was found. Please register the flag."
            });
        }

        let hasFlag = false;
        //get flags in flag instrument
        let res: any = await this.requestHandler.submitRequest(url) || [];
        //if variable for this flag has not been created then create it
        for(let flag of res) {
            if(flag.var_id == flagID) {
                hasFlag = true;
                break;
            }
        }
        //make sure to create flag instruments
        //get flag info
        if(!hasFlag) {
            let { id, name, description, ...metadata } = flagRecord;
            let options = {
                method: 'POST'
            }
            //
            let body = [{
                "var_name": id,
                "var_id": name,
                "var_description": description,
                "metadata": metadata
            }];
            await this.requestHandler.submitRequest(url, options, body);
        }

        //test this, need inst in body?
        url = `${this.tenantURL}/v3/streams/projects/${this.projectID}/sites/${stationID}/instruments/${instID}/measurements`;
        let options = {
            method: 'POST'
        }
        let varData = {
            datetime
        };
        varData[flagID] = value;
        let body = {
            vars: [varData]
        }
        await this.requestHandler.submitRequest(url, options, body);
    }

    //list flag values
    async listFlagValues(stationID: string, options: {[key: string]: string}) {
        let instID = this.getInstID(stationID, "flags");
        return this.listValues(instID, stationID, options);
    }

    //list flags
    async listFlags(flagID?: string) {
        let keys: any = {};
        if(flagID !== undefined) {
            keys.id = flagID;
        }
        return this.v2Manager.getMatches("mesonet_flag", keys);
    }

    private async listValues(instID: string, stationID: string, options: {[key: string]: string}) {
        // Construct URL for measurements request
        let url = `${this.tenantURL}/v3/streams/projects/${this.projectID}/sites/${stationID}/instruments/${instID}/measurements${this.encodeURLParams(options)}`;
        let res: any = await this.requestHandler.submitRequest(url) || {};
        if(res.measurements_in_file !== undefined) {
            delete res.measurements_in_file;
        }
        //transform timestamps
        for(let variable in res) {
            let transformedVarData = {};
            for(let timestamp in res[variable]) {
                let localizedTimestamp = this.localizeTimestamp(timestamp);
                transformedVarData[localizedTimestamp] = res[variable][timestamp];
            }
            res[variable] = transformedVarData;
        }
        return res;
    }
}





export class TapisV3Streams {
    private tenantURL: string;
    private requestHandler: RequestHandler;
    private v2Manager: TapisManager;

    constructor(tenantURL: string, retryLimit: number, auth: TapisV3Auth, v2Manager: TapisManager) {
        this.tenantURL = tenantURL;
        this.requestHandler = new RequestHandler(retryLimit, auth);
        this.v2Manager = v2Manager;
    }

    public getProjectHandler(projectID: string, timezone: string): ProjectHandler {
        return new ProjectHandler(projectID, timezone, this.tenantURL, this.requestHandler, this.v2Manager);
    }
}


// class FlagHandler {
// //create all variables
//     //also mesonet workflow will need to create vars for all instruments
//     //and create all flag instruments for new stations
//     async createFlagInstrument(projectID: string, stationID: string, flagID: string, flagName, defaultValue) {
//         let instrumentID = this.getFlagInstID(stationID, flagID);
//         let url = `${this.tenantURL}/v3/streams/projects/${projectID}/sites/${stationID}/instruments`;
//         let options = {
//             method: "POST"
//         };
//         let metadata = {
//             id: flagID,
//             name: flagName,
//             default: defaultValue
//         };

//         let body = JSON.stringify([{
//             inst_id: instrumentID,
//             inst_name: instrumentID,
//             inst_description: `Instrument for logging ${flagID} flag values for station ${stationID}`,
//             metadata
//         }]);

//         await this.submitRequest(url, options, body);
//         return instrumentID;
//     }

//     //update?
//     async createFlag(stationID, flagID, flagName, variableIDs, defaultValue) {
//         let instrumentID = await this.createFlagInstrument(stationID, flagID, flagName, defaultValue);
//         //can mass create variables
//         //note units and stuff don't matter for flags
//         for(let variableID of variableIDs) {
//             await this.createFlagVariable(flagID, variableID);
//         }
//     }

//     async createFlagVariable(flagID, variableID) {
//         //this.createFlagVariables();
//     }

//     async getFlags() {
//         let stations = this.listStations();
//         let flags = stations[0].instruments.map((instrument) => {
//             return {
//                 flag_id: instrument.inst_id,
//                 flag_name: instrument.inst_name
//             };
//         });
//         return flags;
//     }


//     //////////////////////////////////////////////////////////
//     //////////////////////////////////////////////////////////
//     //////////////////////////////////////////////////////////

//     async registerFlag(flagID, flagName, defaultValue) {
//         if(defaultValue === undefined) {
//             defaultValue = 0;
//         }
//         this.v2Manager.create({
//             name: "mesonet_flag",
//             value: {
//                 id: flagID,
//                 name: flagName,
//                 defaultValue
//             }
//         });
//     }

//     async getFlag(flagID) {
//         let flag = null;
//         let flagData = await this.v2Manager.queryData({
//             name: "mesonet_flag",
//             "value.id": flagID
//         });
//         if(flagData.length > 0) {
//             flag = flagData[0];
//         }
//     }

//     async listFlags() {
//         return this.v2Manager.queryData({
//             name: "mesonet_flag"
//         });
//     }

//     //////////////////////////////////////////////////////////
//     //////////////////////////////////////////////////////////
//     //////////////////////////////////////////////////////////

//     getFlagInstID(projectID: string, stationID: string, flag: string) {
//         return `${projectID}_${stationID}_${flag}`;
//     }
// }