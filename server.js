const express = require("express");
const compression = require("compression");
const nodemailer = require("nodemailer");
const https = require("https");
const fs = require("fs");
const config = require("./config.json");
const child_process = require("child_process");
const indexer = require("./fileIndexer");
const moment = require("moment");
const path = require("path");
//add timestamps to output
require("console-stamp")(console);

const port = config.port;
const smtp = config.smtp;
const smtpPort = config.smtpPort;
const keyFile = config.key;
const certFile = config.cert;
const hskey = fs.readFileSync(keyFile);
const hscert = fs.readFileSync(certFile);
const mailOptionsBase = config.email;
const defaultZipName = config.defaultZipName;

const dataRoot = config.dataRoot;
const urlRoot = config.urlRoot;
const rawDataDir = config.rawDataDir;
const sourceDataDir = config.sourceDataDir;
const downloadDir = config.downloadDir;
const userLog = config.userLog;

const rawDataRoot = `${dataRoot}${rawDataDir}`;
const rawDataURLRoot = `${urlRoot}${rawDataDir}`;
const sourceDataRoot = `${dataRoot}${sourceDataDir}`;
const sourceDataURLRoot = `${urlRoot}${sourceDataDir}`;
const downloadRoot = `${dataRoot}${downloadDir}`;
const downloadURLRoot = `${urlRoot}${downloadDir}`;


const transporterOptions = {
  host: smtp,
  port: smtpPort,
  secure: false
}

//gmail attachment limit
const ATTACHMENT_MAX_MB = 25;

process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = 0;
process.env["NODE_ENV"] = "production";

////////////////////////////////
//////////server setup//////////
////////////////////////////////

const app = express();

let options = {
    key: hskey,
    cert: hscert
};

const server = https.createServer(options, app)
.listen(port, (err) => {
  if(err) {
    console.error(error);
  }
  else {
    console.log("Server listening at port " + port);
  }
});

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
//compress all HTTP responses
app.use(compression());

app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST");
  res.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization, Range, Content-Range, Cache-Control");
  //pass to next layer
  next();
});

////////////////////////////////
////////////////////////////////

/////////////////////////////
///////signal handling///////
/////////////////////////////

const signals = {
  "SIGHUP": 1,
  "SIGINT": 2,
  "SIGTERM": 15
};

function shutdown(code) {
  //stops new connections and completes existing ones before closing
  server.close(() => {
    console.log(`Server shutdown.`);
    process.exit(code);
  });
}

for(let signal in signals) {
  let signalVal = signals[signal];
  process.on(signal, () => {
    console.log(`Received ${signal}, shutting down server...`);
    shutdown(128 + signalVal);
  });
}

/////////////////////////////
/////////////////////////////


async function sendEmail(transporterOptions, mailOptions) {

  combinedMailOptions = Object.assign({}, mailOptionsBase, mailOptions);

  let transporter = nodemailer.createTransport(transporterOptions);

  //have to be on uh netork
  return transporter.sendMail(combinedMailOptions)
  .then((info) => {
    //should parse response for success (should start with 250)
    res = {
      success: true,
      result: info,
      error: null
    };
    return res;
  })
  .catch((error) => {
    return {
      success: false,
      result: null,
      error: error
    };
  });
}

function logUser(user, files) {
  let data = `${user}: ${files}`
  fs.appendFile(userLog, data, (err) => {
    if(err) {
      console.error(`Failed to write userlog.\nError: ${err}`);
    }
  });
}

async function handleReq(req, res, handler) {
  try {
    let status = await handler();
    //log email address and success status
    console.log(status.user + ":" + status.code + ":" + status.success);
  }
  catch(e) {
    let errorMsg = "An error has occured: \n\
      method:" + req.method + "\n\
      endpoint:" + req.path + "\n\
      error: " + e;
    //should also add email to admin for bug reporting?
    console.error(errorMsg);
    res.status(500)
    .send("An unexpected error occurred");

    let mailOptions = {
      to: ["mcleanj@hawaii.edu", "seanbc@hawaii.edu"],
      subject: "HCDP API error",
      text: `An unexpected error occured in the HCDP API:\n${errorMsg}`,
      html: `<p> An error occured in the HCDP API:\n${errorMsg}</p>`
    };
    //attempt to send email to the administrators
    sendEmail(transporterOptions, mailOptions);
  }
}


app.get("/raster", async (req, res) => {
  return handleReq(req, res, async () => {
    let status = {
      user: null,
      code: 200,
      success: true
    };

    //destructure query
    let {date, returnEmptyNotFound, ...properties} = req.query;

    let data = [{
      files: ["data_map"],
      range: {
        start: date,
        end: date
      },
      ...properties
    }];
    
    let files = await indexer.getFiles(data);
    let file = null;
    //should only be exactly one file
    if(files.length == 0 && returnEmptyNotFound) {
      file = indexer.getEmpty(properties.extent);
    }
    else {
      file = files[0];
    }
    
    if(!file) {
      //set failure and code in status
      status.success = false;
      status.code = 404;
  
      //resources not found
      res.status(404)
      .send("The requested file could not be found");
    }
    else {
      res.status(200)
      .sendFile(file);
    }
    return status;
  });

});




//should move file indexing
app.post("/genzip/email", async (req, res) => {
  return handleReq(req, res, async () => {
    let status = {
      user: null,
      code: 202,
      success: true
    };

    let email = req.body.email;
    let data = req.body.data;
    let zipName = req.body.name || defaultZipName;

    status.user = email;
    //make sure required parameters exist and data is an array
    if(!Array.isArray(data) || !email) {
      //set failure and code in status
      status.success = false;
      status.code = 400;

      //send error
      res.status(400)
      .send(
        `Request body should include the following fields: \n\
        data: An array of file data objects describing a non-empty set of files to zip \n\
        email: The email to send the package to \n\
        zipName (optional): What to name the zip file. Default: ${defaultZipName}`
      );
    }
    else {
      //response should be sent immediately after file check, don't wait for email to finish
      //202 accepted indicates request accepted but non-commital completion
      res.status(202)
      .send("Request received. Generating download package");

      //note no good way to validate email address, should have something in app saying that if email does not come to verify spelling
      //email should arrive in the next few minutes, if email does not arrive within 2 hours we may have been unable to send the email, check for typos, try again, or contact the site administrators

      /////////////////////////////////////
      // generate package and send email //
      /////////////////////////////////////

      let handleError = async (clientError) => {
        //set failure in status
        status.success = false;

        clientError += " We appologize for the inconvenience. The site administrators will be notified of the issue. Please try again later.";
        let mailOptions = {
          to: email,
          text: clientError,
          html: "<p>" + clientError + "</p>"
        };
        //try to send the error email, last try to actually notify user
        sendEmail(transporterOptions, mailOptions);
      }
      
      //get files
      let files = await indexer.getFiles(data);

      let zipPath = "";
      let zipProc = child_process.spawn("sh", ["./zipgen.sh", downloadRoot, zipName, ...files]);

      //write stdout (should be file name) to output accumulator
      zipProc.stdout.on("data", (data) => {
        zipPath += data.toString();
      });

      //handle result on end
      zipProc.on("exit", async (code) => {
        if(code !== 0) {
          serverError = `Failed to generate download package for user ${email}. Zip process failed with code ${code}.`
          clientError = "There was an error generating your HCDP download package.";
          handleError(clientError);
          throw new Error(serverError);
        }
        else {
          let zipDec = zipPath.split("/");
          let zipRoot = zipDec.slice(0, -1).join("/");
          let zipExt = zipDec.slice(-2).join("/");

          let fstat = fs.statSync(zipPath);
          let fsizeB = fstat.size;
          let fsizeMB = fsizeB / (1024 * 1024);

          let attachFile = fsizeMB < ATTACHMENT_MAX_MB;

          let mailRes;

          if(attachFile) {
            attachments = [{
              filename: zipName,
              content: fs.createReadStream(zipPath)
            }];
            let mailOptions = {
              to: email,
              attachments: attachments,
              text: "Your HCDP data package is attached.",
              html: "<p>Your HCDP data package is attached.</p>"
            };
            
            mailOptions = Object.assign({}, mailOptionsBase, mailOptions);
            mailRes = await sendEmail(transporterOptions, mailOptions);
            //if an error occured fall back to link
            if(!mailRes.success) {
              attachFile = false;
            }
          }

          //recheck, state may change if fallback on error
          if(!attachFile) {
            //create download link and send in message body
            let downloadLink = downloadURLRoot + zipExt;
            let mailOptions = {
              to: email,
              text: "Your HCDP download package is ready. Please go to " + downloadLink + " to download it. This link will expire in three days, please download your data in that time.",
              html: "<p>Your HCDP download package is ready. Please click <a href=\"" + downloadLink + "\">here</a> to download it. This link will expire in three days, please download your data in that time.</p>"
            };
            mailRes = await sendEmail(transporterOptions, mailOptions);
          }

          //if unsuccessful attempt to send error email
          if(!mailRes.success) {
            let serverError = "Failed to send message to user " + email + ". Error: " + mailRes.error.toString();
            let clientError = "There was an error sending your HCDP download package to this email address.";
            handleError(clientError);
            //failed to send email, clear data
            child_process.exec("rm -r " + zipRoot);
            throw new Error(serverError);
          }
          //cleanup file if attached
          //otherwise should be cleaned by chron task
          //no need error handling, if error chron should handle later
          else if(attachFile) {
            child_process.exec("rm -r " + zipRoot);
          }
        }
      });
      logUser(email, files.length);
    }
    return status;
  });
});


app.post("/genzip/instant/content", async (req, res) => {
  return handleReq(req, res, async () => {
    let status = {
      user: null,
      code: 200,
      success: true
    };

    let email = req.body.email;
    let data = req.body.data;

    status.user = email;

    if(!Array.isArray(data)) {
      //set failure and code in status
      status.success = false;
      status.code = 400;

      res.status(400)
      .send(
        "Request body should include the following fields: \n\
        data: An array of file data objects describing a non-empty set of files to zip."
      );
    }
    else {
      let files = await indexer.getFiles(data);
      if(files.length > 0) {
        res.contentType("application/zip");
  
        let zipProc = child_process.spawn("zip", ["-qq", "-", ...files]);
    
        //write content to res
        zipProc.stdout.on("data", (data) => {
            res.write(data);
        });
    
        //handle errors and end res on completion
        zipProc.on("exit", (code) => {
          if(code !== 0) {
            //set failure and code in status
            status.success = false;
            status.code = 500;
    
            res.status(500)
            .end();
            throw new Error("Zip process failed with code " + code);
          }
          else {
            res.status(200)
            .end();
          }
        });
      }
      //just send empty if no files
      else {
        res.status(200)
        .end();
      }
      logUser(email, files.length);
    }
    return status;
  });
});


app.post("/genzip/instant/link", async (req, res) => {
  return handleReq(req, res, async () => {
    let status = {
      user: null,
      code: 200,
      success: true
    };

    let zipName = defaultZipName;
    let email = req.body.email;
    let data = req.body.data;

    status.user = email;

    //if not array then leave files as 0 length to be picked up by error handler
    if(!Array.isArray(data)) {
      //set failure and code in status
      status.success = false;
      status.code = 400;

      res.status(400)
      .send(
        `Request body should include the following fields: \n\
        data: An array of file data objects describing a non-empty set of files to zip. \n\
        zipName (optional): What to name the zip file. Default: ${defaultZipName}`
      );
    }
    else {
      let files = await indexer.getFiles(data);
      res.contentType("application/zip");

      let zipProc = child_process.spawn("sh", ["./zipgen.sh", downloadRoot, zipName, ...files]);
      let zipPath = "";
      //write stdout (should be file name) to output accumulator
      zipProc.stdout.on("data", (data) => {
        zipPath += data.toString();
      });
    
      //handle result on end
      zipProc.on("exit", async (code) => {
        if(code !== 0) {
          //set failure and code in status
          status.success = false;
          status.code = 500;

          let serverError = "Failed to generate download package. Zip process failed with code " + code;
          res.status(500)
          .send(serverError);
          throw new Error(serverError);
        }
        else {
          let zipDec = zipPath.split("/");
          let zipExt = zipDec.slice(-2).join("/");
          let downloadLink = downloadURLRoot + zipExt;
          res.status(200)
          .send(downloadLink);
          logUser(email, files.length);
        }
      });
    }
    return status;
  });
});





app.post("/genzip/instant/splitlink", async (req, res) => {
  return handleReq(req, res, async () => {
    let status = {
      user: null,
      code: 200,
      success: true
    };

    let email = req.body.email;
    let data = req.body.data;

    status.user = email;

    if(!Array.isArray(data)) {
      //set failure and code in status
      status.success = false;
      status.code = 400;

      res.status(400)
      .send(
        `Request body should include the following fields: \n\
        data: An array of file data objects describing a non-empty set of files to zip.`
      );
    }
    else {
      let files = await indexer.getFiles(data);
      res.contentType("application/zip");
      let zipProc = child_process.spawn("sh", ["./zipgen_parts.sh", downloadRoot, ...files]);
      let zipOutput = "";
      //write stdout (should be file name) to output accumulator
      zipProc.stdout.on("data", (data) => {
        zipOutput += data.toString();
      });
    
      //handle result on end
      zipProc.on("exit", async (code) => {
        if(code !== 0) {
          //set failure and code in status
          status.success = false;
          status.code = 500;

          let serverError = "Failed to generate download package. Zip process failed with code " + code;
          res.status(500)
          .send(serverError);
          throw new Error(serverError);
        }
        else {
          let parts = zipOutput.split(" ");
          let fileParts = [];
          let uuid = parts[0];
          for(let i = 1; i < parts.length; i++) {
            let fpart = parts[i];
            let fname = downloadURLRoot + uuid + "/" + fpart;
            fileParts.push(fname);
          }

          let data = {
            files: fileParts
          }
          res.status(200)
          .json(data);
          logUser(email, files.length);
        }
      });
    }
    return status;
  });
});



app.get("/raw/list", async (req, res) => {
  return handleReq(req, res, async () => {
    let status = {
      user: null,
      code: 200,
      success: true
    };

    let date = req.query.date;

    if(!date) {
      //set failure and code in status
      status.success = false;
      status.code = 400;

      res.status(400)
      .send(
        "Request must include the following parameters: \n\
        date: An ISO 8601 formatted date string representing the date you would like the data for."
      );
    }

    let parsedDate = moment(date);
    let year = parsedDate.format("YYYY");
    let month = parsedDate.format("MM");
    let day = parsedDate.format("DD");

    let dataDir = path.join(year, month, day);
    let sysDir = path.join(rawDataRoot, dataDir);
    let linkDir = path.join(rawDataURLRoot, dataDir);

    fs.readdir(sysDir, (err, files) => {
      //no dir for requested date, just return empty
      if(err && err.code == "ENOENT") {
        files = [];
      }
      else if(err) {
        //set failure and code in status
        status.success = false;
        status.code = 500;
        //resources not found
        return res.status(500)
        .send("An error occured while retrieving the requested data.");
      }

      files = files.map((file) => {
        let fileLink = path.join(linkDir, file);
        return fileLink;
      });
;
      return res.status(200)
      .json(files);
    });
    return status;
  });
});


app.get("/source_data/data", async (req, res) => {
  return handleReq(req, res, async () => {
    let status = {
      user: null,
      code: 200,
      success: true
    };

    let date = req.query.date;
    let source = req.query.source;
    let tier = req.query.tier;

    //need to have these three parameters
    if(!(date && source && tier)) {
      //set failure and code in status
      status.success = false;
      status.code = 400;

      res.status(400)
      .send(
        "Request must include the following parameters: \n\
        date: An ISO 8601 formatted date string representing the date you would like the data for. \n\
        source: The data source you would like the data for. \n\
        tier: The data tier you would like the data for."
      );
    }

    let parsedDate = moment(date);
    let year = parsedDate.format("YYYY");
    let month = parsedDate.format("MM");
    let day = parsedDate.format("DD");
    let dataDir = path.join(tier, source, "parse", year, month, day);

    let failureHandler = (code) => {
      if(code == 404) {
        //set failure and code in status and
        status.success = false;
        status.code = 404;
        //resources not found
        return res.status(404)
        .send("The requested data does not exist.");
      }
      else if(code == 404) {
        //set failure and code in status
        status.success = false;
        status.code = 500;
        //resources not found
        return res.status(500)
        .send("An error occured while retrieving the requested data.");
      }
    }

    fs.readdir(dataDir, (err, files) => {
      //no dir for requested date, return 404
      if(err && err.code == "ENOENT") {
        return failureHandler(404);
      }
      else if(err) {
        return failureHandler(500);
      }
      //should only be one file in directory (if this is incorrect then switch this to a list function)
      if(files.length < 1) {
        return failureHandler(404);
      }
      file = path.join(dataDir, files[0]);

      res.status(200)
      .sendFile(file);
    });
    return status;
  });
});


app.get("/source_data/list", async (req, res) => {
  return handleReq(req, res, async () => {
    let status = {
      user: null,
      code: 200,
      success: true
    };

    let date = req.query.date;
    let source = req.query.source;
    let tier = req.query.tier;

    //need to have these three parameters
    if(!(date && source && tier)) {
      //set failure and code in status
      status.success = false;
      status.code = 400;

      res.status(400)
      .send(
        "Request must include the following parameters: \n\
        date: An ISO 8601 formatted date string representing the date you would like the data for. \n\
        source: The data source you would like the data for. \n\
        tier: The data tier you would like the data for."
      );
    }

    let parsedDate = moment(date);
    let year = parsedDate.format("YYYY");
    let month = parsedDate.format("MM");
    let day = parsedDate.format("DD");
    let dataDir = path.join(tier, source, "parse", year, month, day);

    fs.readdir(dataDir, (err, files) => {
      //no dir for requested date, return 404
      if(err && err.code == "ENOENT") {
        files = [];
      }
      else if(err) {
        //set failure and code in status
        status.success = false;
        status.code = 500;
        //resources not found
        return res.status(500)
        .send("An error occured while retrieving the requested data.");
      }

      files = files.map((file) => {
        let fileLink = path.join(linkDir, file);
        return fileLink;
      });

      return res.status(200)
      .json(files);
    });
    return status;
  });
});