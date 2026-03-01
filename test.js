import SQuery from "./src/index.js";


const db = await SQuery.connect({
  dialect: "mongo",
  config: { connectionString: 'mongodb://localhost:27017/framecraft' },
});
//adminaccounts

console.log(await db.schema().showTables());

const users = await db
  .table("songalbums")
  //.select(["id", "email"])
  //.where("is_active", true)
  //.orderBy("id", "desc")
  .limit(20)
  .get();

  const usersDesc = await db.schema().getDesc("songalbums", { strict: true, includeCreateSql: true });

  console.log(usersDesc);