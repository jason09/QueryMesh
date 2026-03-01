import SQuery from "./src/index.js";


const db = await SQuery.connect({
  dialect: "mongo",
  config: { connectionString: 'mongodb://localhost:27017/framecraft' },
});

console.log(await db.schema().showTables());

const users = await db
  .table("users")
  .select(["id", "email"])
  .where("is_active", true)
  .orderBy("id", "desc")
  .limit(20)
  .get();

  const usersDesc = await db.schema().getDesc("users", { strict: true, includeCreateSql: true });

  console.log(usersDesc);