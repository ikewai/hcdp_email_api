const https = require("https");
const moment = require("moment");
import { TapisV3Auth } from "./auth";
import { TapisManager } from "../tapisHandlers";

export class TapisV3Streams {
    private tenantURL: string;
    private retryLimit: number;
    private auth: TapisV3Auth;
    private v2Manager: TapisManager;

    constructor(tenantURL: string, retryLimit: number, v2Manager: TapisManager, auth: TapisV3Auth) {
        this.tenantURL = tenantURL;
        this.auth = auth;
        this.retryLimit = retryLimit;
        this.v2Manager = v2Manager;
    }

    private convertTimestampToHST(timestamp: string) {
        let converted = new moment(timestamp).subtract(10, "hours");
        return converted.toISOString().slice(0, -1) + "-10:00";
    }

    private getInstID(projectID: string, stationID: string, type: string) {
        return `${projectID}_${stationID}_${type}`;
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

    async listMeasurements(projectID: string, stationID: string, options: {[key: string]: string}) {
        let instID = this.getInstID(projectID, stationID, "measurements");
        // Construct URL for measurements request
        let url = `${this.tenantURL}/v3/streams/projects/${projectID}/sites/${stationID}/instruments/${instID}/measurements${this.encodeURLParams(options)}`;
        let res: any = await this.submitRequest(url) || {};
        if(res.measurements_in_file !== undefined) {
            delete res.measurements_in_file;
        }
        //transform timestamps
        for(let variable in res) {
            let transformedVarData = {};
            for(let timestamp in res[variable]) {
                let hstTimestamp = this.convertTimestampToHST(timestamp);
                transformedVarData[hstTimestamp] = res[variable][timestamp];
            }
            res[variable] = transformedVarData;
        }
        return res;
    }

    async listVariables(projectID: string, stationID: string) {
        let instID = this.getInstID(projectID, stationID, "measurements");
        // Construct URL for measurements request
        let url = `${this.tenantURL}/v3/streams/projects/${projectID}/sites/${stationID}/instruments/${instID}/variables`;
        let res = await this.submitRequest(url) || [];
        return res;
    } 

    async listStations(projectID: string): Promise<any[]> {
        // Construct URL for request
        let url = `${this.tenantURL}/v3/streams/projects/${projectID}/sites`;
        let res: any[] = <any[]>(await this.submitRequest(url)) || [];
        return res;
    }

    private async submitRequest(url: string, options = {}, body: any = null, retries = this.retryLimit) {
        if(retries === undefined) {
            retries = this.retryLimit;
        }
        //get token from auth promise
        let token = await this.auth;

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
            const retry = (code, err) => {
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