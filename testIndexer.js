
const https = require("https");

let types = [{
    datatype: "rainfall",
    properties: {
        
    },
    files: [],
    range: {
        start: "2010-12-01",
        end: "2010-12-01",
        period: null
    }
}];


let periods = ["day", "month"];
let datatypes = ["rainfall", "temperature"];
let fills = ["filled", "partial", "unfilled"];
let aggregations = ["min", "max", "avg"];
let extents = ["statewide", "bi", "ka", "mn", "oa"];
let tiers = ["archival"];
let productions = ["new", "legacy"];