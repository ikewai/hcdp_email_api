const { MongoClient } = require("mongodb");
const querystring = require('querystring');
const https = require("https");

import { TapisV3Auth } from "./tapis/auth";
import { TapisV3Streams, ProjectHandler } from "./tapis/streams";

export { ProjectHandler };

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

    async getMatches(name: string, keys: {[key: string]: string}): Promise<any[]> {
        let query = {
            name
        };
        for(let key in keys) {
            let queryKey = `value.${key}`;
            let queryValue = keys[key];
            query[queryKey] = queryValue;
        }
        let matches = (await this.queryData(query)).result;
        return matches;
    }

    async replace(name: string, keys: {[key: string]: string}, newValue: {[key: string]: any}): Promise<boolean> {
        let replaced = false;
        let matches = await this.getMatches(name, keys);
        let match: any = null;
        if(matches.length > 0) {
            match = matches[0];
        }
        if(match) {
            match.uuid;
            await this.dbManager.replaceRecord(match.uuid, newValue)
            replaced = true;
        }
        return replaced;
    }

    async checkCreate(name: string, keys: {[key: string]: string}, value: {[key: string]: any}, replace: boolean = true): Promise<'c' | 'r' | 'n'> {
        let happened: 'c' | 'r' | 'n' = 'n';
        let create = async () => {
            await this.create({
                name,
                value
            });
            happened = 'c';
        }
        
        //if should replace if exists then try replace
        //if not replaced then create
        if(replace) {
            let replaced = await this.replace(name, keys, value);
            if(replaced) {
                happened = 'r';
            }
            else {
                await create();
            };
        }
        //otherwise check if any matches
        //if no matches then create
        else {
            let matches = await this.getMatches(name, keys);
            if(matches.length <= 0) {
                await create();
            }
        }
        return happened;
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
    private auth: TapisV3Auth;
    private _streams: TapisV3Streams;

    constructor(username: string, password: string, tenantURL: string, retryLimit: number, v2Manager: TapisManager) {
        this.auth = new TapisV3Auth(username, password, tenantURL);
        this._streams = new TapisV3Streams(tenantURL, retryLimit, this.auth, v2Manager);
    }

    get streams(): TapisV3Streams {
        return this._streams;
    }

    
    close() {
        this.auth.close();
    }
}