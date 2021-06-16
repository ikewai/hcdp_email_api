// server.js
const express = require("express");
const bodyParser = require("body-parser");
const nodemailer = require("nodemailer");
const https = require("https");
const fs = require("fs");
const config = require("./config.json");
const child_process = require("child_process");

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
//gmail attachment limit
const ATTACHMENT_MAX_MB = 999;


async function sendEmail(transporterOptions, mailOptions) {
  process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = 0;

  combinedMailOptions = Object.assign({}, mailOptionsBase, mailOptions);

  let transporter = nodemailer.createTransport(transporterOptions);

  //have to be on uh vpn
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

server.post("/genzip/email", async (req, res) => {
  
  let email = req.body.email;
  let zipName = req.body.name || defaultZipName;
  let files = req.body.files;

  let success = true;

  //note no good way to validate eamil address, should have something in app saying that if email does not come to verify spelling
  //email should arrive in the next few minutes, if email does not arrive within 2 hours we may have been unable to send the email, check for typos, try again, or contact the site administrators

  //response should be sent immediately, don't wait for email to finish
  res.send("Request received. Generating download package");

  //child_process.exec("sh ./zipgen.sh " + email + " " + files, (error, stdout, stderr) => {
  let zipProc = child_process.spawn("sh", ["./zipgen.sh", genRoot, zipName, ...files]);
  //let zipProc = child_process.spawn("zip", ["-qq", "-", ...files]);
  
  let zipOutput = "";

  // Keep writing stdout to res
  zipProc.stdout.on("data", (data) => {
    zipOutput += data.toString();
  });

  // zipProc.stderr.on("data", function (data) {

  // });

  // End the response on zip exit
  zipProc.on("exit", async (code) => {

    let transporterOptions = {
      host: smtp,
      port: smtpPort,
      secure: false
    }

    let handleError = async (clientError, serverError) => {
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


    if(code !== 0) {
      let serverError = "Failed to generate download package for user " + email + ". Zip process failed with code " + code;
      let clientError = "There was an error generating your HCDP download package.";
      console.error(serverError);
      handleError(clientError);
    }
    else {
      let zipPath = zipOutput;
      let zipDec = zipPath.split("/");
      console.log(zipPath);
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
          text: "Your HCDP download package is ready. Please go to" + downloadLink + "to download it. This link will expire in three days, please download your data in that time.",
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
    
    //log email address and success status
    console.log(email + ":" + success);

  });
});


server.post("/genzip/instant", async (req, res) => {
  let files = req.body.files;

  let zipProc = child_process.spawn("zip", ["-qq", "-", ...files]);

  res.contentType("application/zip");

  // Keep writing stdout to res
  zipProc.stdout.on("data", (data) => {
      res.write(data);
  });

  // zipProc.stderr.on("data", function (data) {

  // });

  // End the response on zip exit
  zipProc.on("exit", (code) => {
    if(code !== 0) {
        res.statusCode = 500;
        console.error("Zip process failed with code " + code);
        res.end();
    }
    else {
        res.end();
    }
  });

});



// server.post("/email", (req, res) => {
//   console.log(req.body);
//   //ignore SSL validation in case tenant uses self-signed cert
//   process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = 0;

//   let transporter = nodemailer.createTransport({
//       host: smtp,
//       port: smtpPort,
//       secure: false
//   });
//   let mailOptions = {
//       from: req.body.from,
//       to: req.body.to,
//       subject: req.body.subject,
//       text: req.body.message,
//       attachments: req.body.attachments
//   };

//   //have to be on uh vpn
//   transporter.sendMail(mailOptions, function(error, info) {
//     if (error) {
//       res.json({error: error});
//       console.log(error);
//     }
//     else {
//         res.json({success: info.response});
//         console.log("Email sent: " + info.response);
//     }
//   });
// })
