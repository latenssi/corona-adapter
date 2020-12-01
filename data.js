const fs = require("fs");
const d3 = require("d3");
const Chain = require("stream-chain");
const { parser: StreamJsonParser } = require("stream-json");
const { pick: StreamJsonPicker } = require("stream-json/filters/Pick");
const { stringer: StreamJsonStringer } = require("stream-json/Stringer");
const { filter: StreamJsonFilter } = require("stream-json/filters/Filter");
const { Collect } = require("stream-collect");
const { parse: csvParse, Transform: CSVTransform } = require("json2csv");
const request = require("request");
const dateFormat = require("dateformat");
const { group } = require("console");

const dataURIv1 =
  "https://w3qa5ydb4l.execute-api.eu-west-1.amazonaws.com/prod/finnishCoronaData";

const dataURIv2 =
  "https://w3qa5ydb4l.execute-api.eu-west-1.amazonaws.com/prod/finnishCoronaData/v2";

const getCacheFilepath = () => {
  return `./cache/data-${dateFormat(new Date(), "yyyy-mm-dd-HH")}.dat`;
};

const csvOptions = [{ withBOM: true }, { encoding: "utf-8" }];

async function getRawDataStream() {
  if (process.env.NODE_ENV !== "development") return request.get(dataURIv2);

  const filename = getCacheFilepath();

  if (!fs.existsSync(filename)) {
    await new Promise((resolve) =>
      request
        .get(dataURIv2)
        .pipe(fs.createWriteStream(filename))
        .on("finish", resolve)
    );
  }

  return fs.createReadStream(filename);
}

async function getFilteredDataStream(datum = null, fields = null) {
  const pipeline = [StreamJsonParser()];

  if (datum) pipeline.push(StreamJsonPicker({ filter: datum }));

  if (fields) {
    const validFields = fields.filter(
      (f) =>
        [
          "id",
          "date",
          "healthCareDistrict",
          "infectionSourceCountry",
          "infectionSource",
        ].indexOf(f) > -1
    );
    if (validFields) {
      const fieldStr = `^.*(${validFields.join("|")})\\b`;
      pipeline.push(StreamJsonFilter({ filter: new RegExp(fieldStr) }));
    }
  }

  pipeline.push(StreamJsonStringer());

  const chain = new Chain(pipeline);

  (await getRawDataStream()).pipe(chain.input);

  return chain;
}

async function getGroupedData(type) {
  const data = await (await getFilteredDataStream(null, ["date"]))
    .pipe(new Collect({ encoding: "utf-8" }))
    .collect()
    .then((str) => JSON.parse(str));

  const getDateStr = (d) => d.date.substring(0, 10);
  const getDatum = (d) => d.datum;
  const getLength = (v) => v.length;

  // datums: [ 'confirmed', 'deaths', 'recovered' ]
  // data: { confirmed: [...], deaths: [...], recovered: [...]}

  // Transpose from { datum: [{ ...value },], } to [ { ...value, datum  }, ]
  const transposed = Object.entries(data).reduce(
    (acc, [datum, cases]) => [...acc, ...cases.map((c) => ({ ...c, datum }))],
    []
  );

  const result = d3
    .rollups(transposed, getLength, getDateStr, getDatum)
    .sort((a, b) => d3.ascending(a[0], b[0]))
    .map(([date, values]) => ({ date, ...Object.fromEntries(values) }));

  if (type === "csv") return csvParse(result, ...csvOptions);
  return result;
}

function getFormattedDataStream(stream, type) {
  if (type === "csv") {
    const csvTransform = new CSVTransform(...csvOptions);
    return stream.pipe(csvTransform);
  }
  return stream;
}

module.exports = {
  getRawDataStream,
  getFormattedDataStream,
  getFilteredDataStream,
  getGroupedData,
};
