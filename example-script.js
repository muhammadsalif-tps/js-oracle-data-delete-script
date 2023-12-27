/**
 * UserSettings schema add OpenAI field
 */
const moment = require("moment");
const MongoDb = require("../model/Schema").mongoose;
let db;
MongoDb.onConnect().then((dbInstance) => {
  db = dbInstance;
  startProcess();
});
async function processUserSettings(userSetting) {
  let _plan = "NORMAL";
  let planType = "";
  if (!planType) {
    if (planType) {
    }
  }
  if (_plan) {
    const plan = await db
      .model("Plan")
      .findOne({ product: "PUBLISHER", name: _plan })
      .lean();
  }
}
async function middleFunction(
  userSettings,
  maxRequest = 100,
  failedRequests = [],
) {
  console.info
    ("userSettings remaining: ", userSettings.length);
  const _userSettings = userSettings.splice(0, maxRequest);
  const promisesUserSettings = _userSettings.map(async (userSetting) => {
    return processUserSettings(userSetting).catch((e) => {
      failedRequests.push({
        userSettingsId: userSetting._id.toString(),
        error: e,
      });
    });
  });
  await Promise.all(promisesUserSettings);
  if (userSettings && userSettings.length) {
    await middleFunction(userSettings, maxRequest, failedRequests);
  }
  return Promise.resolve(failedRequests);
}
async function startProcess() {
  try {
    const userSettings = await db.model("UserSettings").find({});
    await middleFunction(userSettings);
    console.log("Script executed");
    db.close();
  } catch (e) {
    console.log(e);
    db.close();
  }
}