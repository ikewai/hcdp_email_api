const { MongoClient } = require("mongodb");
const querystring = require('querystring');
const https = require("https");
const moment = require("moment");

export class DBManager {
    dbName: string;
    collectionName: string;
    connectionRetryLimit: number;
    queryRetryLimit: number;
    client: any;
    connection: Promise<void>;
    db: any;
    collection: any;

    constructor(server, port, username, password, dbName, collectionName, connectionRetryLimit, queryRetryLimit) {
        const encodedUsername = encodeURIComponent(username);
        const encodedPassword = encodeURIComponent(password);
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.connectionRetryLimit = connectionRetryLimit;
        this.queryRetryLimit = queryRetryLimit;

        const dbURI = `mongodb://${encodedUsername}:${encodedPassword}@${server}:${port}/${dbName}`;
        const dbConfig = {
            useNewUrlParser: true,
            useUnifiedTopology: true,
            keepAlive: true
        };
        this.client = new MongoClient(dbURI, dbConfig);
        this.connection = this.createConnection(connectionRetryLimit, 0);
    }

    async wait(delay): Promise<void> {
        return new Promise((resolve, reject) => {
            setTimeout(() => {
                resolve();
            }, delay);
        });
    }
    
    getBackoff(delay) {
        let backoff = 0;
        //if first failure backoff of 0.25-0.5 seconds
        if(delay == 0) {
            backoff = 0.25 + Math.random() * 0.25;
        }   
        //otherwise 2-3x current backoff
        else {
            backoff = 2 * delay + Math.random() * delay;
        }
        return backoff;
    }
    
    async createConnection(retries, delay) {
        await this.wait(delay);
        //maight have to have this return connection and set this.connection to result of this function
        //or definitely have to do that, because if retrying connection and something's waiting it will throw the original connection error, want to wait until this whole functions finished
        let connection = this.client.connect();
        try {
            await connection;
        }
        catch(err) {
            if(retries-- > 0) {
                delay = this.getBackoff(delay);
                this.createConnection(retries, delay);
            }
            else {
                throw err;
            }
        }
        this.db = this.client.db(this.dbName);
        this.collection = this.db.collection(this.collectionName);
    }
    
    async executeDbQuery(action, params, retries, retryConnection) {
        let res: any = null;
        try {
            await this.connection;
            res = await this.collection[action](...params);
        }
        catch(err) {
            //if retrying the connection don't count against number of retries
            //only retry connection once to make sure it isn't a connection issue
            if(retryConnection) {
                this.connection = this.createConnection(this.connectionRetryLimit, 0);
                res = this.executeDbQuery(action, params, retries, false);
            }
            //if connection already retried assume not a connection error and just retry query
            else if(retries-- > 0) {
                res = this.executeDbQuery(action, params, retries, false);
            }
            else {
                throw err;
            }
        }
        return res;
    }
    
    async deleteRecord(uuid) {
        const query = { uuid };
        const params = [ query ];
        let res = await this.executeDbQuery("deleteOne", params, this.queryRetryLimit, true);
        let deleted = res.deletedCount;
        return deleted;
    }
    
    async replaceRecord(uuid, newRecord) {
        const query = { uuid };
        //only update value field
        const update = {
            $set: {
                value: newRecord
            }
        };
        const params = [ query, update ];
        let res = await this.executeDbQuery("updateOne", params, this.queryRetryLimit, true);
        let modified = res.modifiedCount;
        return modified;
    }

    async disconnect() {
        this.client.close();
    }
}





export class TapisManager {
    //should refine types
    //also add access modifiers
    header: any;
    tenantURL: URL;
    retryLimit: number;
    dbManager: DBManager;

    constructor(tenantURL, token, retryLimit, dbManager) {
        this.header = {
            "Authorization": `Bearer ${token}`,
            "Content-Type": "application/json"
        }
        if(tenantURL.host) {
            this.tenantURL = tenantURL;
        }
        else {
            this.tenantURL = new URL(tenantURL);
        }
        this.retryLimit = retryLimit;
        this.dbManager = dbManager;
    }

    async request(data, retries, errors: any[] = [], lastCode = null) {
        let { options, body } = data;
        return new Promise((resolve, reject) => {
            if(retries < 0) {
                reject({
                    status: lastCode,
                    reason: errors
                });
            }
            else {
                const req = https.request(options, (res) => {
                    let responseData = "";
                    res.on("data", (chunk) => {
                        responseData += chunk;
                    });
                    res.on("end", () => {
                        let codeGroup = Math.floor(res.statusCode / 100);
                        if(codeGroup != 2) {
                            let e = `Request responded with code ${res.statusCode}; message: ${responseData}`;
                            errors.push(e);
                            return this.request(data, retries - 1, errors, res.statusCode);
                        }
                        else {
                            resolve({
                                code: res.statusCode,
                                data: responseData
                            });
                        }
                    });
                    res.on("error", (e) => {
                        errors.push(e);
                        return this.request(data, retries - 1, errors, res.statusCode);
                    });
                });
                if(body) {
                    req.write(body);
                }
                req.end();
            } 
        });
    }

    async queryData(query, limit = 1000000, offset = 0) {
        let params = {
            q: JSON.stringify(query),
            limit: limit,
            offset: offset
        };
        const paramStr = querystring.stringify(params);
        const options: any = {
            protocol: this.tenantURL.protocol,
            hostname: this.tenantURL.hostname,
            path: "/meta/v2/data?" + paramStr,
            method: "GET",
            headers: this.header
        };
        if(this.tenantURL.port) {
            options.port = this.tenantURL.port;
        }
        let data = {
            options
        };
        return this.request(data, this.retryLimit).then((res: any) => {
            let parsed = JSON.parse(res.data);
            return parsed;
        }, (e) => {
            return Promise.reject(e);
        });
    }

    async create(doc) {
        const options: any = {
            protocol: this.tenantURL.protocol,
            hostname: this.tenantURL.hostname,
            path: "/meta/v2/data",
            method: "POST",
            headers: this.header
        };
        if(this.tenantURL.port) {
            options.port = this.tenantURL.port;
        }
        let data = {
            options,
            body: JSON.stringify(doc)
        };
        return this.request(data, this.retryLimit);
    }

    //get all metadata docs, index by id, more efficient than pulling one at a time
    async getMetadataDocs() {
        let query = {
            name: "hcdp_station_metadata"
        };
        let metadataDocs = (await this.queryData(query)).result;
        let indexedMetadata = {};
        for(let doc of metadataDocs) {
            let idField = doc.value.id_field;
            let stationGroup = doc.value.station_group;
            let id = doc.value[idField];
            if(!indexedMetadata[stationGroup]) {
                indexedMetadata[stationGroup] = {};
            }
            indexedMetadata[stationGroup][id] = doc;
        }
        return indexedMetadata;
    }

    async createMetadataDocs(docs) {
        let existingMetadata = await this.getMetadataDocs();
        for(let doc of docs) {
            let idField = doc.value.id_field;
            let stationGroup = doc.value.station_group;
            let id = doc.value[idField];
            //check if metadata doc with group and id already exists
            let existingDocGroup = existingMetadata[stationGroup];
            let existingDoc;
            if(existingDocGroup) {
                existingDoc = existingDocGroup[id];
            }
            if(existingDoc) {
                let identical = true;
                //check if all properties are the same
                if(Object.keys(existingDoc.value).length == Object.keys(doc.value).length) {
                    for(let property in existingDoc.value) {
                        if(existingDoc.value[property] !== doc.value[property]) {
                            identical = false;
                            break;
                        }
                    }
                }
                else {
                    identical = false;
                }
                //if they are not identical replace doc with the new one (otherwise do nothing)
                if(!identical) {        
                    await this.dbManager.replaceRecord(existingDoc.uuid, doc.value);
                }
            }
            else {     
                await this.create(doc);
            }
        }
    }
}


export class TapisV3Manager {
    username: string;
    password: string;
    tenantURL: string;
    projectID: string;
    instExt: string;
    retryLimit: number;
    v2Manager: TapisManager;
    auth!: Promise<string>;
    authRefresh!: NodeJS.Timeout;

    constructor(username, password, tenantURL, projectID, instExt, retryLimit, v2Manager) {
        // Initialize class properties with provided values
        this.username = username;
        this.password = password;
        this.tenantURL = tenantURL;
        this.projectID = projectID;
        this.instExt = instExt;
        this.retryLimit = retryLimit;
        this.v2Manager = v2Manager;
        this.authenticate();
    }

    convertTimestampToHST(timestamp) {
        let converted = new moment(timestamp).subtract(10, "hours");
        return converted.toISOString().slice(0, -1) + "-10:00";
    }

    authenticate() {
        // Construct the authentication URL
        const authUrl = `${this.tenantURL}/v3/oauth2/tokens`;

        // Construct the payload for authentication
        const authPayload = `username=${encodeURIComponent(this.username)}&password=${encodeURIComponent(this.password)}&grant_type=password&scope=user`;

        // Set options for the authentication request
        const authOptions = {
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
            },
        };

        // set auth promise to authentication funct
        this.auth = new Promise((resolve, reject) => {
            // Initiate the authentication request
            const authReq = https.request(authUrl, authOptions, (authRes) => {
                authRes.setEncoding('utf8');
                let authData = '';

                // Accumulate response data
                authRes.on('data', (chunk) => {
                    authData += chunk;
                });

                // When response is complete
                authRes.on('end', () => {
                    try {
                        const parsedResponse = JSON.parse(authData);
                        // If authentication is successful
                        if(authRes.statusCode === 200 && parsedResponse.result?.access_token?.access_token) {
                            //reauth one minute before token goes stale
                            this.authRefresh = setTimeout(() => {
                                this.authenticate();
                            }, (parsedResponse.result.access_token.expires_in - 60) * 1000);
                            resolve(parsedResponse.result.access_token.access_token);
                        }
                        else {
                            throw new Error('Authentication failed');
                        }
                    }
                    catch (error) {
                        reject(error);
                    }
                });
            });

            // Handle errors in the authentication request
            authReq.on('error', (error) => {
                reject(error);
            });

            // Send authentication payload
            authReq.write(authPayload);
            authReq.end();
        });
    }

    encodeURLParams(params) {
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

    async listMeasurements(stationID, options) {
        // Construct URL for measurements request
        let url = `${this.tenantURL}/v3/streams/projects/${this.projectID}/sites/${stationID}/instruments/${stationID}${this.instExt}/measurements${this.encodeURLParams(options)}`;
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

    async listVariables(stationID) {
        // Construct URL for measurements request
        let url = `${this.tenantURL}/v3/streams/projects/${this.projectID}/sites/${stationID}/instruments/${stationID}${this.instExt}/variables`;
        let res = await this.submitRequest(url) || [];
        return res;
    } 

    async listStations(): Promise<any[]> {
        // Construct URL for request
        let url = `${this.tenantURL}/v3/streams/projects/${this.projectID}/sites`;
        let res: any[] = <any[]>(await this.submitRequest(url)) || [];
        return res;
    }

    //create all variables
    //also mesonet workflow will need to create vars for all instruments
    //and create all flag instruments for new stations
    async createFlagInstrument(stationID, flagID, flagName, defaultValue) {
        let instrumentID = this.getFlagInstID(stationID, flagID);
        let url = `${this.tenantURL}/v3/streams/projects/${this.projectID}/sites/${stationID}/instruments`;
        let options = {
            method: "POST"
        };
        let metadata = {
            id: flagID,
            name: flagName,
            default: defaultValue
        };

        let body = JSON.stringify([{
            inst_id: instrumentID,
            inst_name: instrumentID,
            inst_description: `Instrument for logging ${flagID} flag values for station ${stationID}`,
            metadata
        }]);

        await this.submitRequest(url, options, body);
        return instrumentID;
    }

    //update?
    async createFlag(stationID, flagID, flagName, variableIDs, defaultValue) {
        let instrumentID = await this.createFlagInstrument(stationID, flagID, flagName, defaultValue);
        //can mass create variables
        //note units and stuff don't matter for flags
        for(let variableID of variableIDs) {
            await this.createFlagVariable(flagID, variableID);
        }
    }

    async createFlagVariable(flagID, variableID) {
        //this.createFlagVariables();
    }

    async createStation() {
    }

    async getFlags() {
        let stations = this.listStations();
        let flags = stations[0].instruments.map((instrument) => {
            return {
                flag_id: instrument.inst_id,
                flag_name: instrument.inst_name
            };
        });
        return flags;
    }


    //////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////

    async registerFlag(flagID, flagName, defaultValue) {
        if(defaultValue === undefined) {
            defaultValue = 0;
        }
        this.v2Manager.create({
            name: "mesonet_flag",
            value: {
                id: flagID,
                name: flagName,
                defaultValue
            }
        });
    }

    async getFlag(flagID) {
        let flag = null;
        let flagData = await this.v2Manager.queryData({
            name: "mesonet_flag",
            "value.id": flagID
        });
        if(flagData.length > 0) {
            flag = flagData[0];
        }
    }

    async listFlags() {
        return this.v2Manager.queryData({
            name: "mesonet_flag"
        });
    }

    //////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////

    getFlagInstID(stationID, flag) {
        return `${stationID}${this.instExt}_${flag}`;
    }

    async submitRequest(url, options = {}, body: any = null, retries = this.retryLimit) {
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

    close() {
        clearTimeout(this.authRefresh);
    }
}