const KoaCors = require("@koa/cors");
const router = require("koa-router")();
const server = new (require("koa"))();

const Chain = require("stream-chain");
const { parser: streamJsonParser } = require("stream-json");
const { pick: streamJsonPicker } = require("stream-json/filters/Pick");
const { stringer: streamJsonStringer } = require("stream-json/Stringer");
const { Transform: CSVTransform } = require("json2csv");

const request = require("request");
const fs = require("fs");

function attachDataStream(stream) {
  // fs.createReadStream("./temp.json").pipe(stream);
  request
    .get(
      "https://w3qa5ydb4l.execute-api.eu-west-1.amazonaws.com/prod/finnishCoronaData"
    )
    .pipe(stream);
}

function streamData(ctx, path, type) {
  const pipeline = [
    streamJsonParser(), // Parse json byte stream
    streamJsonPicker({ filter: path }), // Pick the parts we want
    streamJsonStringer() // Convert back to byte stream
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

  attachDataStream(chain.input);

  // WTF? removing these causes the stream to never end..
  chain.on("data", () => {});
  chain.on("end", () => {});

  ctx.type = type;
  ctx.body = chain.output;
}

router.get("/FI/:path.:type", ctx => {
  const { path, type } = ctx.params;
  if (
    ["confirmed", "recovered", "deaths"].indexOf(path) > -1 &&
    ["json", "csv"].indexOf(type) > -1
  ) {
    streamData(ctx, path, type);
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
