const fs = require("fs");

const d3 = require("d3");
const Chain = require("stream-chain");
const { parser: StreamJsonParser } = require("stream-json");
const { pick: StreamJsonPicker } = require("stream-json/filters/Pick");
const { stringer: StreamJsonStringer } = require("stream-json/Stringer");
const { Collect } = require("stream-collect");
const { Transform: CSVTransform } = require("json2csv");

const request = require("request");
const dateFormat = require("dateformat");

const dataURI =
  "https://w3qa5ydb4l.execute-api.eu-west-1.amazonaws.com/prod/finnishCoronaData";

function getCacheFilepath() {
  return `./cache/data-${dateFormat(new Date(), "yyyy-mm-dd-hh")}.dat`;
}

async function getRawDataStream() {
  if (!process.env.USE_CACHE) return request.get(dataURI);

  const filename = getCacheFilepath();

  if (!fs.existsSync(filename)) {
    await new Promise(resolve =>
      request
        .get(dataURI)
        .pipe(fs.createWriteStream(filename))
        .on("finish", resolve)
    );
  }

  return fs.createReadStream(filename);
}

async function getParsedDataStream(path, type) {
  const pipeline = [
    StreamJsonParser(), // Parse json byte stream
    StreamJsonPicker({ filter: path }), // Pick the parts we want
    StreamJsonStringer() // Convert back to byte stream
  ];

  if (type === "csv") {
    pipeline.push(
      new CSVTransform(
        {
          fields: [
            "id",
            "date",
            "healthCareDistrict",
            "infectionSourceCountry",
            "infectionSource"
          ],
          withBOM: true
        },
        { encoding: "utf-8" }
      )
    );
  }

  const chain = new Chain(pipeline);

  (await getRawDataStream()).pipe(chain.input);

  return chain;
}

async function getBinnedData(path) {
  const data = await (await getParsedDataStream(path, "json"))
    .pipe(new Collect({ encoding: "utf-8" }))
    .collect()
    .then(jsonStr => JSON.parse(jsonStr))
    .then(data => data.map(d => new Date(d.date)));

  const ticks = d3
    .scaleTime()
    .domain(d3.extent(data))
    .ticks(d3.timeDay.every(1));

  const hist = d3.histogram().thresholds(ticks)(data);
  const result = hist.map(h => ({ x: h.x0, y: h.length }));

  return result;
}

module.exports = { getRawDataStream, getParsedDataStream, getBinnedData };
