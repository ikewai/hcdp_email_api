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
const whitelist = config.whitelist;
const administrators = config.administrators;

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


async function handleSubprocess(subprocess, dataHandler) {
  return new Promise((resolve, reject) => {
    //write content to res
    subprocess.stdout.on("data", (data) => {
      dataHandler(data);
    });
    subprocess.on("exit", (code) => {
      resolve(code);
    })
  });
}


async function sendEmail(transporterOptions, mailOptions) {

  combinedMailOptions = Object.assign({}, mailOptionsBase, mailOptions);

  let transporter = nodemailer.createTransport(transporterOptions);

  //have to be on uh netork
  return transporter.sendMail(combinedMailOptions)
  .then((info) => {
    //should parse response for success (should start with 250) 
    return {
      success: true,
      result: info,
      error: null
    };
  })
  .catch((error) => {
    return {
      success: false,
      result: null,
      error: error
    };
  });
}


function logReq(data) {
  const { user, code, success, files, method, endpoint, token } = data;
  let dataString = `${method}:${endpoint}:${token}:${code}:${success}:${user}:${files}\n`
  fs.appendFile(userLog, dataString, (err) => {
    if(err) {
      console.error(`Failed to write userlog.\nError: ${err}`);
    }
  });
}

async function handleReq(req, res, handler) {
  //note include success since 202 status might not indicate success in generating downloa package
  let reqData = {
    user: "",
    code: 0,
    success: true,
    files: 0,
    method: req.method,
    endpoint: req.path,
    token: ""
  };
  try {
    authorized = false;
    let auth = req.get("authorization");
    if(auth) {
      let authPattern = /^Bearer (.+)$/;
      let match = auth.match(authPattern);
      if(match) {
        reqData.token = match[1];
        authorized = whitelist.includes(reqData.token);
      }
    }
    if(authorized) {
      await handler(reqData);
    }
    else {
      reqData.code = 401;
      res.status(401)
      .send("User not authorized. Please provide a valid API token in the request header. If you do not have an API token one can be requested from the administrators.");
    }
  }
  catch(e) {
    //set failure occured in request
    reqData.success = false;
    let errorMsg = `method: ${reqData.method}\n\
      endpoint: ${reqData.path}\n\
      error: ${e}`;
    let htmlErrorMsg = errorMsg.replace(/\n/g, "<br>");
    console.error(`An unexpected error occured:\n${errorMsg}`);
    //if request code not set by handler set to 500 and send response (otherwise response already sent and error was in post-processing)
    if(reqData.code == 0) {
      reqData.code = 500;
      res.status(500)
      .send("An unexpected error occurred");
    }
    //send the administrators an email logging the error
    if(administrators.length > 0) {
      let mailOptions = {
        to: administrators,
        subject: "HCDP API error",
        text: `An unexpected error occured in the HCDP API:\n${errorMsg}`,
        html: `<p>An error occured in the HCDP API:<br>${htmlErrorMsg}</p>`
      };
      try {
        //attempt to send email to the administrators
        let emailStatus = await sendEmail(transporterOptions, mailOptions);
        //if email send failed throw error for logging
        if(!emailStatus.success) {
          throw emailStatus.error;
        }
      }
      //if error while sending admin email erite to stderr
      catch(e) {
        console.error(`Failed to send administrator notification email: ${e}`);
      }
    }
  }
  logReq(reqData);
}


app.get("/raster", async (req, res) => {
  await handleReq(req, res, async (reqData) => {
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
      reqData.success = false;
      reqData.code = 404;
  
      //resources not found
      res.status(404)
      .send("The requested file could not be found");
    }
    else {
      reqData.code = 200;
      res.status(200)
      .sendFile(file);
    }
  });

});


//should move file indexing
app.post("/genzip/email", async (req, res) => {
  await handleReq(req, res, async (reqData) => {
    let email = req.body.email;
    let data = req.body.data;
    let zipName = req.body.name || defaultZipName;

    if(email) {
      reqData.user = email;
    }

    //make sure required parameters exist and data is an array
    if(!Array.isArray(data) || !email) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      //send error
      res.status(400)
      .send(
        `Request body should include the following fields: \n\
        data: An array of file data objects describing a set of files to zip \n\
        email: The email to send the package to \n\
        zipName (optional): What to name the zip file. Default: ${defaultZipName}`
      );
    }
    else {
      reqData.code = 202;
      //response should be sent immediately after file check, don't wait for email to finish
      //202 accepted indicates request accepted but non-commital completion
      res.status(202)
      .send("Request received. Generating download package");

      //note no good way to validate email address, should have something in app saying that if email does not come to verify spelling
      //email should arrive in the next few minutes, if email does not arrive within 2 hours we may have been unable to send the email, check for typos, try again, or contact the site administrators

      /////////////////////////////////////
      // generate package and send email //
      /////////////////////////////////////

      let handleError = async (clientError, serverError) => {
        //set failure in status
        reqData.success = false;
        //attempt to send an error email to the user, ignore any errors
        try {
          clientError += " We appologize for the inconvenience. The site administrators will be notified of the issue. Please try again later.";
          let mailOptions = {
            to: email,
            text: clientError,
            html: "<p>" + clientError + "</p>"
          };
          //try to send the error email, last try to actually notify user
          await sendEmail(transporterOptions, mailOptions);
        }
        catch(e) {}
        //throw server error to be handled by main error handler
        throw new Error(serverError);
      }
      
      //get files
      let files = await indexer.getFiles(data);

      let zipPath = "";
      let zipProc = child_process.spawn("sh", ["./zipgen.sh", downloadRoot, zipName, ...files]);

      let code = await handleSubprocess(zipProc, (data) => {
        zipPath += data.toString();
      });

      if(code !== 0) {
        serverError = `Failed to generate download package for user ${email}. Zip process failed with code ${code}.`
        clientError = "There was an error generating your HCDP download package.";
        handleError(clientError, serverError);
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
          //if an error occured fall back to link and try one more time
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
        //cleanup file if attached
        //otherwise should be cleaned by chron task
        //no need error handling, if error chron should handle later
        else {
          child_process.exec("rm -r " + zipRoot);
        }

        //if unsuccessful attempt to send error email
        if(!mailRes.success) {
          let serverError = "Failed to send message to user " + email + ". Error: " + mailRes.error.toString();
          let clientError = "There was an error sending your HCDP download package to this email address.";
          handleError(clientError, serverError);
        }
      }
    }
  });
});


app.post("/genzip/instant/content", async (req, res) => {
  await handleReq(req, res, async (reqData) => {
    let email = req.body.email;
    let data = req.body.data;

    if(email) {
      reqData.user = email;
    }

    if(!Array.isArray(data) || !email) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      res.status(400)
      .send(
        "Request body should include the following fields: \n\
        data: An array of file data objects describing a set of files to zip. \n\
        email: The requestor's email address for logging"
      );
    }
    else {
      let files = await indexer.getFiles(data);
      if(files.length > 0) {
        res.contentType("application/zip");
  
        let zipProc = child_process.spawn("zip", ["-qq", "-", ...files]);

        let code = await handleSubprocess(zipProc, (data) => {
          res.write(data);
        });
        //if zip process failed throw error for handling by main error handler
        if(code !== 0) {
          throw new Error("Zip process failed with code " + code);
        }
        else {
          reqData.code = 200;
          res.status(200)
          .end();
        }
      }
      //just send empty if no files
      else {
        reqData.code = 200;
        res.status(200)
        .end();
      }
    }
  });
});


app.post("/genzip/instant/link", async (req, res) => {
  await handleReq(req, res, async (reqData) => {
    let zipName = defaultZipName;
    let email = req.body.email;
    let data = req.body.data;

    if(email) {
      reqData.user = email;
    }

    //if not array then leave files as 0 length to be picked up by error handler
    if(!Array.isArray(data) || !email) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      res.status(400)
      .send(
        `Request body should include the following fields: \n\
        data: An array of file data objects describing a set of files to zip. \n\
        email: The requestor's email address for logging \n\
        zipName (optional): What to name the zip file. Default: ${defaultZipName}`
      );
    }
    else {
      let files = await indexer.getFiles(data);
      res.contentType("application/zip");

      let zipProc = child_process.spawn("sh", ["./zipgen.sh", downloadRoot, zipName, ...files]);
      let zipPath = "";

      //write stdout (should be file name) to output accumulator
      let code = await handleSubprocess(zipProc, (data) => {
        zipPath += data.toString();
      });
      //if zip process failed throw error for handling by main error handler  
      if(code !== 0) {
        throw new Error("Zip process failed with code " + code);
      }
      else {
        let zipDec = zipPath.split("/");
        let zipExt = zipDec.slice(-2).join("/");
        let downloadLink = downloadURLRoot + zipExt;
        reqData.code = 200;
        res.status(200)
        .send(downloadLink);
      }
    }
  });
});


app.post("/genzip/instant/splitlink", async (req, res) => {
  await handleReq(req, res, async (reqData) => {
    let email = req.body.email;
    let data = req.body.data;

    if(email) {
      reqData.user = email;
    }

    if(!Array.isArray(data) || !email) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      res.status(400)
      .send(
        `Request body should include the following fields: \n\
        data: An array of file data objects describing a set of files to zip. \n\
        email: The requestor's email address for logging`
      );
    }
    else {
      let files = await indexer.getFiles(data);
      res.contentType("application/zip");
      let zipProc = child_process.spawn("sh", ["./zipgen_parts.sh", downloadRoot, ...files]);
      let zipOutput = "";

      //write stdout (should be file name) to output accumulator
      let code = await handleSubprocess(zipProc, (data) => {
        zipOutput += data.toString();
      });

      if(code !== 0) {
        throw new Error("Zip process failed with code " + code);
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
        reqData.code = 200;
        res.status(200)
        .json(data);
      }
    }
  });
});


app.get("/production/list", async (req, res) => {
  await handleReq(req, res, async (reqData) => {
    let data = req.query.data;
    data = JSON.parse(data);
    if(!Array.isArray(data)) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

      res.status(400)
      .send(
        "Request must include the following parameters: \n\
        data: A string encoded JSON query representing an array of file data objects describing a set of files to zip."
      );
    }
    else {
      let linkDir = path.join(rawDataURLRoot, dataDir);
      let files = await indexer.getFiles(data);
      files = files.map((file) => {
        let fileLink = path.join(linkDir, file);
        return fileLink;
      });
      reqData.code = 200;
      res.status(200)
      .json(files);
    }
  });
});


app.get("/raw/list", async (req, res) => {
  await handleReq(req, res, async (reqData) => {
    let date = req.query.date;

    if(!date) {
      //set failure and code in status
      reqData.success = false;
      reqData.code = 400;

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
        reqData.success = false;
        reqData.code = 500;
        //resources not found
        return res.status(500)
        .send("An error occured while retrieving the requested data.");
      }

      files = files.map((file) => {
        let fileLink = path.join(linkDir, file);
        return fileLink;
      });
      reqData.code = 200;
      res.status(200)
      .json(files);
    });
  });
});