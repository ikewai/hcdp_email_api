const { MongoClient } = require("mongodb");
const querystring = require('querystring');
const https = require("https");


class DBManager {
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

    async wait(delay) {
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
        let res = null;
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

class TapisManager {
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

    async request(data, retries, errors) {
        console.log("retries remaining", retries);
        if(!errors) {
            errors = [];
        }
        let { options, body } = data;
        return new Promise((resolve, reject) => {
            if(retries < 0) {
                console.log("error!", errors);
                reject(errors);
            }
            else {
                console.log("submit request");
                const req = https.request(options, (res) => {
                    let responseData = "";
                    res.on("data", (chunk) => {
                        console.log("request got data");
                        responseData += chunk;
                    });
                    res.on("end", () => {
                        console.log("request complete", res.statusCode, responseData);
                        let codeGroup = Math.floor(res.statusCode / 100);
                        if(codeGroup != 2) {
                            let e = `Request responded with code ${res.statusCode}; message: ${responseData}`;
                            errors.push(e);
                            return this.request(data, retries - 1, errors);
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
                        return this.request(data, retries - 1, errors);
                    });
                });
                if(body) {
                    req.write(body);
                }
                req.end();
            } 
        });
    }

    //error handling
    async queryData(query, retries) {
        let params = {
            q: query
        }
        const paramStr = querystring.stringify(params);
        const options = {
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
        console.log("call request");
        return this.request(data, retries);
    }

    create(doc, retries) {
        const options = {
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
        return this.request(data, retries);
    }

    //get all metadata docs, index by id, more efficient than pulling one at a time
    async getMetadataDocs() {
        let query = {
            name: "hcdp_station_metadata"
        };
        console.log("call query");
        let metadataDocs = await this.queryData(query);
        console.log("query complete");
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
        console.log("called create");
        let existingMetadata = await this.getMetadataDocs();
        console.log("got existing");
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
                    console.log("Replace!");
                    //await this.dbManager.replaceRecord(existingDoc.uuid, doc.value);
                }
                else {
                    console.log("Already Exists!");
                }
            }
            else {
                console.log("Create!");
                //await this.create(doc);
            }
        }
    }
}

exports.DBManager = DBManager;
exports.TapisManager = TapisManager;