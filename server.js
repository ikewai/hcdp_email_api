const express = require("express");
const compression = require("compression");
const nodemailer = require("nodemailer");
const https = require("https");
const fs = require("fs");
const config = require("./config.json");
const child_process = require("child_process");
const indexer = require("./fileIndexer");
const fileIndexer = require("./fileIndexer");
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
const genRoot = config.downloadGenRoot;
const linkRoot = config.downloadLinkRoot;

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

async function validateFile(file) {
  return new Promise((resolve, reject) => {
    fs.access(file, fs.F_OK, (e) => {
      if(e) {
        reject(e);
      }
      else {
        resolve();
      }
    });
  });
}

//all or nothing?
async function validateFiles(files) {
  let validators = [];
  for(let file of files) {
    validators.push(validateFile(file));
  }
  return new Promise((resolve, reject) => {
    Promise.all(validators).then(() => {
      resolve(true);
    }, (e) => {
      console.error(e);
      resolve(false);
    });
  });
}

async function handleReq(req, handler) {
  return handler
  .then((status) => {
    //log email address and success status
    console.log(status.user + ":" + status.code + ":" + status.success);
  })
  .catch((e) => {
    console.error(
      "An error has occured: \n\
      method:" + req.method + "\n\
      endpoint:" + req.path + "\n\
      error: " + e.toString()
    );
  });
}


app.get("/raster", async (req, res) => {

  let resourceData = {
    type: "raster"
  }

  let resourceInfo = {
    datatype: req.query.datatype,
    dates: {
      period: req.query.period,
      start: req.query.date,
      end: req.query.date
    },
    group: {
      group: "raster",
      type: "values"
    },
    data: Object.assign(resourceData, req.query),
    filterOpts: {}
  }
  //delete values not needed in data values
  delete resourceInfo.data.datatype;
  delete resourceInfo.data.period;
  delete resourceInfo.data.date;

  try {
    //should only be one result and one file
    file = indexer([resourceInfo])[0].files[0];
  }
  catch(error) {
    console.error(error);
    //if there was an error in the file indexer set files to a junk file to be picked up by file validator
    file = "/error.error";
  }
  
  try {
    await validateFile(file);
  }
  catch {
    //set failure and code in status and resolve for logging
    status.success = false;
    status.code = 404;
    resolve(status);

    //resources not found
    return res.status(404)
    .send("The requested file could not be found");
  }

  res.status(200)
  .sendFile(file);

});




//should move file indexing
app.post("/genzip/email", async (req, res) => {
  return handleReq(req, new Promise(async (resolve, reject) => {
    let status = {
      user: null,
      code: 202,
      success: true
    }

    let email = req.body.email || null;
    let zipName = req.body.name || defaultZipName;
    let fileData = req.body.fileData;

    let files = [];
    //if not array then leave files as 0 length to be picked up by error handler
    if(Array.isArray(fileData)) {
      try {
        let fileGroup = indexer(fileData);
        //reduce to just files, how deal with filtering?
        //should add file staging and write out files there
        files = fileGroup.reduce((acc, item) => {
          return acc.concat(item.files);
        }, []);
      }
      catch(error) {
        //if there was an error in the file indexer set files to a junk file to be picked up by file validator
        files = ["/error.error"];
      }
    }

    status.user = email;
    if(files.length < 1 || !email) {
      //set failure and code in status and resolve for logging
      status.success = false;
      status.code = 400;
      resolve(status);

      //send error
      res.status(400)
      .send(
        "Request body should include the following fields: \n\
        files: An array of file data objects describing a non-empty set of files to zip \n\
        email: The email to send the package to \n\
        zipName (optional): What to name the zip file. Default: " + defaultZipName
      );
    }
    //validate files
    else if(!(await validateFiles(files))) {
      //set failure and code in status and resolve for logging
      status.success = false;
      status.code = 404;

      //send error
      resolve(status);
      //resources not found
      res.status(404)
      .send("Some of the files requested could not be found");
    }
    else {
      //note no good way to validate eamil address, should have something in app saying that if email does not come to verify spelling
      //email should arrive in the next few minutes, if email does not arrive within 2 hours we may have been unable to send the email, check for typos, try again, or contact the site administrators
    
      //response should be sent immediately, don't wait for email to finish
      //202 accepted indicates request accepted but non-commital completion
      res.status(202)
      .send("Request received. Generating download package");
  
      ///////////////////////////////////
      //generate package and send email//
      ///////////////////////////////////
    
      //child_process.exec("sh ./zipgen.sh " + email + " " + files, (error, stdout, stderr) => {
      let zipProc = child_process.spawn("sh", ["./zipgen.sh", genRoot, zipName, ...files]);
      //let zipProc = child_process.spawn("zip", ["-qq", "-", ...files]);

      let handleError = async (clientError, serverError) => {
        //set failure in status and resolve for logging
        status.success = false;
        resolve(status);

        //should also add email to admin for bug reporting?
        console.error(serverError);
    
        message += " We appologize for the inconvenience. The site administrators will be notified of the issue. Please try again later.";
        let mailOptions = {
          to: email,
          text: clientError,
          html: "<p>" + clientError + "</p>"
        };
        mailRes = await sendEmail(transporterOptions, mailOptions);
        return mailRes;
      }

      let zipOutput = "";
    
      //write stdout (should be file name) to output accumulator
      zipProc.stdout.on("data", (data) => {
        zipOutput += data.toString();
      });
    
      //handle result on end
      zipProc.on("exit", async (code) => {
        if(code !== 0) {
          let serverError = "Failed to generate download package for user " + email + ". Zip process failed with code " + code;
          let clientError = "There was an error generating your HCDP download package.";
          console.error(serverError);
          handleError(clientError);
        }
        else {
          let zipPath = zipOutput;
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
            let downloadLink = linkRoot + zipExt;
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
            handleError(clientError, serverError);
          }
          //success, resolve with status for logging
          else {
            resolve(status);
          }
    
          //cleanup file if attached
          //otherwise should be cleaned by chron task
          //no need error handling, if error chron should handle later
          if(attachFile) {
            child_process.exec("rm -r " + zipRoot);
          }
        }
      });
    }
  }));
});


app.post("/genzip/instant/content", async (req, res) => {
  return handleReq(req, new Promise(async (resolve, reject) => {
    let status = {
      user: "instant",
      code: 200,
      success: true
    }

    let fileData = req.body.fileData;

    let files = [];
    //if not array then leave files as 0 length to be picked up by error handler
    if(Array.isArray(fileData)) {
      try {
        let fileGroup = indexer(fileData);
        //reduce to just files, how deal with filtering?
        //should add file staging and write out files there
        files = fileGroup.reduce((acc, item) => {
          return acc.concat(item.files);
        }, []);
      }
      catch(error) {
        //if there was an error in the file indexer set files to a junk file to be picked up by file validator
        files = ["/error.error"];
      }
    }

    if(files.length < 1) {
      //set failure and code in status and resolve for logging
      status.success = false;
      status.code = 400;
      resolve(status);

      res.status(400)
      .send(
        "Request body should include the following fields: \n\
        files: An array of file data objects describing a non-empty set of files to zip."
      );
    }
    //validate files
    else if(!(await validateFiles(files))) {
      //set failure and code in status and resolve for logging
      status.success = false;
      status.code = 404;
      resolve(status);

      //resources not found
      res.status(404)
      .send("Some of the files requested could not be found");
    }
    else {
      res.contentType("application/zip");

      let zipProc = child_process.spawn("zip", ["-qq", "-", ...files]);

      //write content to res
      zipProc.stdout.on("data", (data) => {
          res.write(data);
      });

      //handle errors and end res on completion
      zipProc.on("exit", (code) => {
        if(code !== 0) {
          //set failure and code in status and resolve for logging
          status.success = false;
          status.code = 500;
          resolve(status);

          res.status(500)
          .end();
          console.error("Zip process failed with code " + code);
        }
        else {
          //resolve for logging
          resolve(status)

          res.status(200)
          .end();
        }
      });
    }
  }));

});



app.post("/genzip/instant/link", async (req, res) => {
  return handleReq(req, new Promise(async (resolve, reject) => {
    let status = {
      user: "instant",
      code: 200,
      success: true
    }

    let zipName = defaultZipName;
    let fileData = req.body.fileData;

    let files = [];
    //if not array then leave files as 0 length to be picked up by error handler
    if(Array.isArray(fileData)) {
      try {
        let fileGroup = indexer(fileData);
        //reduce to just files, how deal with filtering?
        //should add file staging and write out files there
        files = fileGroup.reduce((acc, item) => {
          return acc.concat(item.files);
        }, []);
      }
      catch(error) {
        //if there was an error in the file indexer set files to a junk file to be picked up by file validator
        files = ["/error.error"];
      }
    }

    if(files.length < 1) {
      //set failure and code in status and resolve for logging
      status.success = false;
      status.code = 400;
      resolve(status);

      res.status(400)
      .send(
        "Request body should include the following fields: \n\
        files: An array of file data objects describing a non-empty set of files to zip."
      );
    }
    //validate files
    else if(!(await validateFiles(files))) {
      //set failure and code in status and resolve for logging
      status.success = false;
      status.code = 404;
      resolve(status);

      //resources not found
      res.status(404)
      .send("Some of the files requested could not be found");
    }
    else {
      res.contentType("application/zip");

      let zipProc = child_process.spawn("sh", ["./zipgen.sh", genRoot, zipName, ...files]);
      let zipOutput = "";
      //write stdout (should be file name) to output accumulator
      zipProc.stdout.on("data", (data) => {
        zipOutput += data.toString();
      });
    
      //handle result on end
      zipProc.on("exit", async (code) => {
        if(code !== 0) {
          //set failure and code in status and resolve for logging
          status.success = false;
          status.code = 500;
          resolve(status);

          let serverError = "Failed to generate download package. Zip process failed with code " + code;
          res.status(500)
          .send(serverError);
          console.error(serverError);
        }
        else {
          resolve(status);
          let zipPath = zipOutput;
          let zipDec = zipPath.split("/");
          let zipExt = zipDec.slice(-2).join("/");
          let downloadLink = linkRoot + zipExt;
          res.status(200)
          .send(downloadLink);
        }
      });
    }
  }));
});





app.post("/genzip/instant/splitlink", async (req, res) => {
  return handleReq(req, new Promise(async (resolve, reject) => {
    let status = {
      user: "instant",
      code: 200,
      success: true
    }

    let fileData = req.body.fileData;

    let files = [];
    //if not array then leave files as 0 length to be picked up by error handler
    if(Array.isArray(fileData)) {
      try {
        let fileGroup = indexer(fileData);
        //reduce to just files, how deal with filtering?
        //should add file staging and write out files there
        files = fileGroup.reduce((acc, item) => {
          return acc.concat(item.files);
        }, []);
      }
      catch(error) {
        console.error(error);
        //if there was an error in the file indexer set files to a junk file to be picked up by file validator
        files = ["/error.error"];
      }
    }

    if(files.length < 1) {
      //set failure and code in status and resolve for logging
      status.success = false;
      status.code = 400;
      resolve(status);

      res.status(400)
      .send(
        "Request body should include the following fields: \n\
        files: An array of file data objects describing a non-empty set of files to zip."
      );
    }
    //validate files
    else if(!(await validateFiles(files))) {
      //set failure and code in status and resolve for logging
      status.success = false;
      status.code = 404;
      resolve(status);

      //resources not found
      res.status(404)
      .send("Some of the files requested could not be found");
    }
    else {
      res.contentType("application/zip");

      let zipProc = child_process.spawn("sh", ["./zipgen_parts.sh", genRoot, ...files]);
      let zipOutput = "";
      //write stdout (should be file name) to output accumulator
      zipProc.stdout.on("data", (data) => {
        zipOutput += data.toString();
      });
    
      //handle result on end
      zipProc.on("exit", async (code) => {
        if(code !== 0) {
          //set failure and code in status and resolve for logging
          status.success = false;
          status.code = 500;
          resolve(status);

          let serverError = "Failed to generate download package. Zip process failed with code " + code;
          res.status(500)
          .send(serverError);
          console.error(serverError);
        }
        else {
          resolve(status);
          let parts = zipOutput.split(" ");
          let fileParts = [];
          let uuid = parts[0];
          for(let i = 1; i < parts.length; i++) {
            let fpart = parts[i];
            let fname = linkRoot + uuid + "/" + fpart;
            fileParts.push(fname);
          }

          let data = {
            files: fileParts
          }

          //wait for a couple seconds and hope file permissions update, should link into perms for verification
          setTimeout(() => {
            res.status(200)
            .json(data);
          }, 2000);
          
        }
      });
    }
  }));
});