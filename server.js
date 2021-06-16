// server.js
const express = require("express");
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
      resolve(false);
    });
  });
}

const server = express();

let options = {
    key: hskey,
    cert: hscert
};

https.createServer(options, server)
.listen(port);

console.log("Server listening at port " + port);

server.use(bodyParser.json());
server.use(bodyParser.urlencoded({ extended: true }));

//should move file indexing
server.post("/genzip/email", async (req, res) => {
  let success = true;
  
  let email = req.body.email;
  let zipName = req.body.name || defaultZipName;
  let files = req.body.files;
  if(!Array.isArray(files) || files.length < 1 || !email) {
    success = false;
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
    success = false;
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

    let handleError = async (clientError, serverError) => {
      success = false;
  
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
  
    //child_process.exec("sh ./zipgen.sh " + email + " " + files, (error, stdout, stderr) => {
    let zipProc = child_process.spawn("sh", ["./zipgen.sh", genRoot, zipName, ...files]);
    //let zipProc = child_process.spawn("zip", ["-qq", "-", ...files]);
    
    let zipOutput = "";
  
    // Keep writing stdout to res
    zipProc.stdout.on("data", (data) => {
      zipOutput += data.toString();
    });
  
    // End the response on zip exit
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
  
        //what is the attachment limit?
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
  
        //cleanup file if attached
        //otherwise should be cleaned by chron task
        if(attachFile) {
          child_process.exec("rm -r " + zipRoot);
        }
      }
    });
  }

  //log email address and success status
  console.log(email + ":" + res.statusCode + ":" + success);
});


server.post("/genzip/instant", async (req, res) => {
  let success = true;

  let files = req.body.files;
  if(!Array.isArray(files) || files.length < 1) {
    success = false;
    res.status(400)
    .send(
      "Request body should include the following fields: \n\
      files: A non-empty array of files to zip"
    );
  }
  //validate files
  else if(!(await validateFiles(files))) {
    success = false;
    //resources not found
    res.status(404)
    .send("Some of the files requested could not be found");
  }
  else {
    let zipProc = child_process.spawn("zip", ["-qq", "-", ...files]);

    res.contentType("application/zip");
  
    // Keep writing stdout to res
    zipProc.stdout.on("data", (data) => {
        res.write(data);
    });
  
    // End the response on zip exit
    zipProc.on("exit", (code) => {
      if(code !== 0) {
        success = false
        res.statusCode = 500;
        console.error("Zip process failed with code " + code);
        res.end();
      }
      else {
          res.end();
      }
    });
  }

  //log successful
  console.log("instant:" + res.statusCode + ":" + success);

});

