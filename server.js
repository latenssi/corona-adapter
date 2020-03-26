const fs = require("fs");

const KoaCors = require("@koa/cors");
const router = require("koa-router")();
const server = new (require("koa"))();

const Chain = require("stream-chain");
const { parser: StreamJsonParser } = require("stream-json");
const { pick: StreamJsonPicker } = require("stream-json/filters/Pick");
const { stringer: StreamJsonStringer } = require("stream-json/Stringer");
const { Transform: CSVTransform } = require("json2csv");

const request = require("request");
const dateFormat = require("dateformat");

const dataURI =
  "https://w3qa5ydb4l.execute-api.eu-west-1.amazonaws.com/prod/finnishCoronaData";

function getCacheFilename() {
  return `./cache/data-${dateFormat(new Date(), "yyyy-mm-dd-hh")}.dat`;
}

function getDataStream() {
  const filename = getCacheFilename();
  const fileExist = fs.existsSync(filename);

  return new Promise(resolve => {
    if (fileExist) {
      resolve(fs.createReadStream(filename));
    } else {
      request
        .get(dataURI)
        .pipe(fs.createWriteStream(filename))
        .on("finish", () => resolve(fs.createReadStream(filename)));
    }
  });
}

async function streamData(ctx, path, type) {
  const pipeline = [
    StreamJsonParser(), // Parse json byte stream
    StreamJsonPicker({ filter: path }), // Pick the parts we want
    StreamJsonStringer() // Convert back to byte stream
  ];

  if (type === "csv") {
    pipeline.push(
      new CSVTransform({
        fields: [
          "id",
          "date",
          "healthCareDistrict",
          "infectionSourceCountry",
          "infectionSource"
        ]
      })
    );
  }

  const chain = new Chain(pipeline);

  const dataStream = await getDataStream();

  dataStream.pipe(chain.input);

  ctx.type = type;
  ctx.body = chain;
}

router.get("/FI/:path.:type", async ctx => {
  const { path, type } = ctx.params;
  if (
    ["confirmed", "recovered", "deaths"].indexOf(path) > -1 &&
    ["json", "csv"].indexOf(type) > -1
  ) {
    await streamData(ctx, path, type);
    return;
  }
  ctx.status = 404;
});

server
  .use(KoaCors())
  .use(router.routes())
  .use(router.allowedMethods());

if (!module.parent) {
  server.listen(process.env.PORT || 8000);
  console.log(`Server listening on port ${process.env.PORT || 8000}`);
}
