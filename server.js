const KoaCors = require("@koa/cors");
const KoaRouter = require("koa-router")();
const axios = require("axios");
const PassThrough = require("stream").PassThrough;
const server = new (require("koa"))();
// const cache = new (require("streaming-cache"))();
// const { AsyncParser } = require("json2csv");

KoaRouter.get("/json", ctx => {
  const stream = PassThrough();

  ctx.type = "json";
  ctx.body = stream;

  axios({
    method: "get",
    url:
      "https://w3qa5ydb4l.execute-api.eu-west-1.amazonaws.com/prod/finnishCoronaData",
    responseType: "stream"
  }).then(response => response.data.pipe(stream));
});

server
  .use(KoaCors())
  .use(KoaRouter.routes())
  .use(KoaRouter.allowedMethods());

if (!module.parent) {
  server.listen(process.env.PORT || 8000);
  console.log(`Server listening on port ${process.env.PORT || 8000}`);
}
