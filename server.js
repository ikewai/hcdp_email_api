// server.js
const express = require("express");
const compression = require("compression");
const bodyParser = require("body-parser");
const nodemailer = require("nodemailer");
const https = require("https");
const fs = require("fs");
const config = require("./config.json");
const child_process = require("child_process");
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


async function sendEmail(transporterOptions, mailOptions) {
  process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = 0;

  combinedMailOptions = Object.assign({}, mailOptionsBase, mailOptions);

  let transporter = nodemailer.createTransport(transporterOptions);

  //have to be on uh netork
  return transporter.sendMail(combinedMailOptions)
  .then((info) => {
    //should parse response for success (shoudl start with 250)
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

const server = express();

let options = {
    key: hskey,
    cert: hscert
};

https.createServer(options, server)
.listen(port, (err) => {
  if(err) {
    console.error(error);
  }
  else {
    console.log("Server listening at port " + port);
  }
});

server.use(bodyParser.json());
server.use(bodyParser.urlencoded({ extended: true }));
//compress all HTTP responses
server.use(compression());

server.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST");
  res.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization, Range, Content-Range");
  //pass to next layer
  next();
});

//should move file indexing
server.post("/genzip/email", async (req, res) => {
  return handleReq(req, new Promise(async (resolve, reject) => {
    let status = {
      user: null,
      code: 202,
      success: true
    }

    let email = req.body.email || null;
    let zipName = req.body.name || defaultZipName;
    let files = req.body.files;
    status.user = email;
    if(!Array.isArray(files) || files.length < 1 || !email) {
      //set failure and code in status and resolve for logging
      status.success = false;
      status.code = 400;
      resolve(status);

      //send error
      res.status(400)
      .send(
        "Request body should include the following fields: \n\
        files: A non-empty array of files to zip \n\
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
          // console.log(zipPath);
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


server.post("/genzip/instant/content", async (req, res) => {
  return handleReq(req, new Promise(async (resolve, reject) => {
    let status = {
      user: "instant",
      code: 200,
      success: true
    }

    let files = req.body.files;
    if(!Array.isArray(files) || files.length < 1) {
      //set failure and code in status and resolve for logging
      status.success = false;
      status.code = 400;
      resolve(status);

      res.status(400)
      .send(
        "Request body should include the following fields: \n\
        files: A non-empty array of files to zip"
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



server.post("/genzip/instant/link", async (req, res) => {
  return handleReq(req, new Promise(async (resolve, reject) => {
    let status = {
      user: "instant",
      code: 200,
      success: true
    }

    let files = req.body.files;
    let zipName = defaultZipName;

    if(!Array.isArray(files) || files.length < 1) {
      //set failure and code in status and resolve for logging
      status.success = false;
      status.code = 400;
      resolve(status);

      res.status(400)
      .send(
        "Request body should include the following fields: \n\
        files: A non-empty array of files to zip"
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











server.post("/genzip/instant/parallel/fref", async (req, res) => {
  return handleReq(req, new Promise(async (resolve, reject) => {
    let status = {
      user: "instant",
      code: 200,
      success: true
    }

    let files = req.body.files;
    let zipName = defaultZipName;

    if(!Array.isArray(files) || files.length < 1) {
      //set failure and code in status and resolve for logging
      status.success = false;
      status.code = 400;
      resolve(status);

      res.status(400)
      .send(
        "Request body should include the following fields: \n\
        files: A non-empty array of files to zip"
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

          //get file size
          let fstat = fs.statSync(zipPath);
          let fsizeB = fstat.size;

          let data = {
            fref: zipExt,
            fsizeB: fsizeB
          }

          //201 resource created
          res.status(201)
          .json(data);
        }
      });
    }
  }));
});


server.post("/genzip/instant/parallel/chunk", async (req, res) => {
  return handleReq(req, new Promise(async (resolve, reject) => {
    let status = {
      user: "instant",
      code: 200,
      success: true
    }

    let fref = req.body.fref;
    //this is causing cors issues, lets just use body params, maybe its get only or something
    //let range = req.range();
    let range = req.body.range;
    let fpath = genRoot + fref;

    //change what this is checking
    // if(!Array.isArray(files) || files.length < 1) {
    //   //set failure and code in status and resolve for logging
    //   status.success = false;
    //   status.code = 400;
    //   resolve(status);

    //   res.status(400)
    //   .send(
    //     "Request body should include the following fields: \n\
    //     files: A non-empty array of files to zip"
    //   );
    // }
    //validate files
    if(!(await validateFiles([fpath]))) {
      //set failure and code in status and resolve for logging
      status.success = false;
      status.code = 404;
      resolve(status);

      //resources not found
      res.status(404)
      .send("Some of the files requested could not be found");
    }
    else {
      res.contentType("application/octet-stream");

      fs.open(fpath, "r", (err, fd) => {
        if(err) {

        }
        else {
          //should make sure type is bytes
          let size = range.end - range.start;
          let buff = Buffer.alloc(size);
          fs.read(fd, buff, 0, size, range.start, (err, bytes, filledBuff) => {
            if(err) {

            }
            else {
              resolve(status);
              //send buffer and number of bytes read
              let data = {
                content: filledBuff,
                size: bytes
              }
              //206 partial content
              res.status(200)
              .json(data);
            }
          });
        }
      });

      

    }
  }));
});















server.post("/genzip/instant/splitlink", async (req, res) => {
  return handleReq(req, new Promise(async (resolve, reject) => {
    let status = {
      user: "instant",
      code: 200,
      success: true
    }

    let files = req.body.files;

    if(!Array.isArray(files) || files.length < 1) {
      //set failure and code in status and resolve for logging
      status.success = false;
      status.code = 400;
      resolve(status);

      res.status(400)
      .send(
        "Request body should include the following fields: \n\
        files: A non-empty array of files to zip"
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
          console.log(zipOutput);
          let files = [];
          let uuid = parts[0];
          for(let i = 1; i < parts.length; i++) {
            let fpart = files[i];
            let fname = linkRoot + uuid + "/" + fpart;
            files.push(fname);
          }

          let data = {
            files: files
          }

          res.status(200)
          .json(data);
        }
      });
    }
  }));
});