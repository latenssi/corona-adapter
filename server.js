const server = new (require("koa"))();
const router = require("koa-router")();
const cors = require("@koa/cors")();

const {
  getFilteredDataStream,
  getFormattedDataStream,
  getGroupedData
} = require("./data");

router.get("/FI/grouped.:type", async ctx => {
  const { type } = ctx.params;

  if (!(["json", "csv"].indexOf(type) > -1)) {
    ctx.status = 404;
    return;
  }

  const data = await getGroupedData(type);

  ctx.type = type;
  ctx.body = data;
});

router.get("/FI/all.:type", async ctx => {
  const { type } = ctx.params;
  const { fields = "" } = ctx.query;

  if (!(["json", "csv"].indexOf(type) > -1)) {
    ctx.status = 404;
    return;
  }

  const dataStream = await getFilteredDataStream(null, fields.split(","));
  const formattedStream = getFormattedDataStream(dataStream, type);

  ctx.type = type;
  ctx.body = formattedStream;
});

router.get("/FI/:datum.:type", async ctx => {
  const { datum, type } = ctx.params;
  const { fields = "" } = ctx.query;

  if (
    !(
      ["confirmed", "recovered", "deaths"].indexOf(datum) > -1 &&
      ["json", "csv"].indexOf(type) > -1
    )
  ) {
    ctx.status = 404;
    return;
  }

  const dataStream = await getFilteredDataStream(datum, fields.split(","));
  const formattedStream = getFormattedDataStream(dataStream, type);

  ctx.type = type;
  ctx.body = formattedStream;
});

server
  .use(cors)
  .use(router.routes())
  .use(router.allowedMethods());

if (!module.parent) {
  server.listen(process.env.PORT || 8000);
  console.log(`Server listening on port ${process.env.PORT || 8000}`);
}
