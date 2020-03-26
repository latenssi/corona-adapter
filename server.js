const server = new (require("koa"))();
const router = require("koa-router")();
const cors = require("@koa/cors")();

const { getParsedDataStream, getBinnedData } = require("./data");

router.get("/FI/:path.:type", async ctx => {
  const { path, type } = ctx.params;
  if (
    ["confirmed", "recovered", "deaths"].indexOf(path) > -1 &&
    ["json", "csv"].indexOf(type) > -1
  ) {
    const dataStream = await getParsedDataStream(path, type);
    ctx.type = type;
    ctx.body = dataStream;
    return;
  }
  ctx.status = 404;
});

router.get("/FI/binned/:path.:type", async ctx => {
  const { path, type } = ctx.params;
  if (
    ["confirmed", "recovered", "deaths"].indexOf(path) > -1 &&
    ["json"].indexOf(type) > -1
  ) {
    const dataStream = await getBinnedData(path);
    ctx.type = type;
    ctx.body = dataStream;
    return;
  }
  ctx.status = 404;
});

server
  .use(cors)
  .use(router.routes())
  .use(router.allowedMethods());

if (!module.parent) {
  server.listen(process.env.PORT || 8000);
  console.log(`Server listening on port ${process.env.PORT || 8000}`);
}
