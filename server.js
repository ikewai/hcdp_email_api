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
const linkRoot = downloadLinkRoot;
//gmail attachment limit
const ATTACHMENT_MAX_MB = 25;


async function sendEmail(transporterOptions, mailOptions) {
  process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = 0;

  let transporter = nodemailer.createTransport(transporterOptions);

  //have to be on uh vpn
  return transporter.sendMail(mailOptions)
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
  console.log(req.body);
  let email = req.body.email;
  let zipName = req.body.name || defaultZipName;
  let files = req.body.files;

  //response should be sent immediately, don't wait for email to finish
  res.send("Generating download package");

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
    if(code !== 0) {
      console.error(zipOutput);
      console.error("zip process failed with code " + code);
    }
    else {
      let zipPath = zipOutput;
      console.log(zipPath);
      let zipRoot = zipPath.substring(0, zipPath.lastIndexOf("/"));

      //for very large files probably have to have download link, how to set this up?
      //what is the attachment limit?
      let fstat = fs.statSync(zipPath);
      let fsizeB = fstat.size;
      let fsizeMB = fsizeB / (1024 * 1024);

      let attachFile = fsizeMB < ATTACHMENT_MAX_MB;

      let mailOptions = {
        to: email
      };

      if(attachFile) {
        attachments = [{
          filename: zipName,
          content: fs.createReadStream(zipPath)
        }];
        mailOptions.attachments = attachments;
        mailOptions.message = "Your HCDP data package is attached.";
      }
      else {
        //create download link and send in message body
        let downloadLink = linkRoot + zipPath;
        mailOptions.message = "Here is a link to your HCDP download package:\n\n" + downloadLink + "\n\nThis link will expire in three days. Please download your data in that time.";
      }

      let transporterOptions = {
        host: smtp,
        port: smtpPort,
        secure: false
      }

      mailOptions = Object.assign({}, mailOptionsBase, mailOptions);
      let mailRes = await sendEmail(transporterOptions, mailOptions);

      // if(!mailRes.success) {
      //   console.error(error);
      //   //attempt to send failure message
      // }

      //attempt to email error on failure?
      console.log(mailRes);

      //cleanup file if attached
      //otherwise should be cleaned by chron task
      if(attachFile) {
        child_process.exec("rm -r " + zipRoot);
      }
      
    }
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
        console.error("zip process failed with code " + code);
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
