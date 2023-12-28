const oracledb = require('oracledb');
require('dotenv').config();
const jsonData = require("./jsonData.json");
const { calculateTimePassed, getRandomDate } = require('./helpers');

let dbInstance = null
const dbConfig = {
  user: process.env.ORACLE_USERNAME,
  password: process.env.ORACLE_PASSWORD,
  connectString: process.env.ORACLE_CONNECTION_STRING,
};

oracledb.getConnection(dbConfig).then((_db) => {
  dbInstance = _db;
  start();
}).catch((error) => {
  console.log("error in connection with db", error)
});

const filteredData = () => {
  const dataObj = {
    TRANSACTION_STATUS: jsonData["transactionStatus"][Math.floor(Math.random() * [jsonData["transactionStatus"].length])],
    TRANSACTION_TYPE: jsonData["transactionType"][Math.floor(Math.random() * [jsonData["transactionType"].length])],
    INSTRUMENT_TYPE: jsonData["instrumentType"][Math.floor(Math.random() * [jsonData["instrumentType"].length])],
    STAGE1_STATUS: jsonData["stages"][Math.floor(Math.random() * [jsonData["stages"].length])] ? "IS NOT NULL" : "IS NULL",
    STAGE2_STATUS: jsonData["stages"][Math.floor(Math.random() * [jsonData["stages"].length])] ? "IS NOT NULL" : "IS NULL",
    STAGE3_STATUS: jsonData["stages"][Math.floor(Math.random() * [jsonData["stages"].length])] ? "IS NOT NULL" : "IS NULL",
    STAGE4_STATUS: jsonData["stages"][Math.floor(Math.random() * [jsonData["stages"].length])] ? "IS NOT NULL" : "IS NULL",
    MERCHANT_ID: jsonData["merchantId"][Math.floor(Math.random() * [jsonData["merchantId"].length])],
  }
  const halfLengthOfObject = Math.floor(Object.keys(dataObj).length / 2);
  for (i = 1; i < halfLengthOfObject; i++) {
    const keys = Object.keys(dataObj);
    const keyToDelete = keys[Math.floor(Math.random() * keys.length)];
    delete dataObj[keyToDelete]
  }
  return dataObj;
}

const filteredData2 = () => {
  const dataObj = {
    // TRANSACTION_STATUS: "<> '0000'",
    STAGE4_STATUS: "IS NOT NULL",
    RESPONSE_CODE: "999"
  }
  return dataObj;
}

const processData = () => {
  let inputData = filteredData2();
  const paymentDateTime = getRandomDate(jsonData["paymentDateTime"].smallest, jsonData["paymentDateTime"].largest);
  inputData = { ...Object.assign({ "PAYMENT_DATETIME": paymentDateTime }, inputData) };

  const inputData2 = {
    TRANSACTION_TYPE: jsonData["transactionType"][Math.floor(Math.random() * [jsonData["transactionType"].length])],
    INSTRUMENT_TYPE: jsonData["instrumentType"][Math.floor(Math.random() * [jsonData["instrumentType"].length])],
  }

  let whereClause = Object.keys(inputData).sort().map((key) => {
    if (key === "RESPONSE_CODE") {
      return `AND (${key} = ${inputData[key]} OR`;
    }
    else if (key === "PAYMENT_DATETIME") {
      return `PAYMENT_DATETIME BETWEEN TO_DATE('${inputData[key]} 00:00:00','YYYY-MM-DD HH24:MI:SS') AND TO_DATE('${inputData[key]} 23:59:59','YYYY-MM-DD HH24:MI:SS')`;
    }
    else if (key === "STAGE4_STATUS") {
      return `${key} ${inputData[key]}`;
    }
  }).join(' ');

  const whereClause2 = Object.keys(inputData2).sort().map((key) => {
    return `${key} = '${inputData2[key]}'`;
  }).join(' AND ');

  whereClause = `${whereClause} AND ${whereClause2})`


  const sql = `DELETE FROM IRIS_CUSTOM.ODS_TRANSACTION_LOG WHERE ${whereClause}`;
  // const sql = `SELECT * FROM IRIS_CUSTOM.ODS_TRANSACTION_LOG WHERE ${whereClause}`;
  console.log("QUERY =====", sql, " ====")

  return dbInstance.execute(sql, {}, {
    outFormat: oracledb.OBJECT,
    autoCommit: true
  })
};

async function middleFunction(
  totalRecords = 0, // total records in table
  maxRequest = 1,
  dataDeleted = 0,
  failedRequests = [],
  startTime = new Date()
) {
  try {
    console.log("records remaining", totalRecords)
    const recordsToDelete = Math.min(maxRequest, totalRecords);
    const _promises = []

    for (let i = 0; i < recordsToDelete; i++) {
      _promises.push(processData().catch((e) => {
        failedRequests.push({
          index: (dataDeleted - 1) * maxRequest + i + 1,
          error: e,
        });
      }));
    }

    const response = await Promise.all(_promises);
    dataDeleted += response[0]?.rowsAffected;
    console.log("rowsAffected", dataDeleted);
    totalRecords = Math.abs(totalRecords - recordsToDelete);
    const timePassed = calculateTimePassed(startTime, new Date());
    console.log(`Time passed: ${timePassed}`);
    console.log(`${process.pid} ===============================>`)
    if (totalRecords) {
      await middleFunction(totalRecords, maxRequest, dataDeleted, failedRequests, startTime);
    }

    return Promise.resolve(failedRequests);
  } catch (error) {
    console.error('Error inserting data:', error);
  }
}
const getRecordsCount = async () => {
  const query = `SELECT COUNT(*) FROM IRIS_CUSTOM.ODS_TRANSACTION_LOG`;
  const response = await dbInstance.execute(query, {})
  return response?.rows[0][0];
}

const start = async () => {
  if (!dbInstance) {
    console.log("Connection to db failed");
    return Promise.resolve();
  }
  console.log("database connected");
  const totalRecords = await getRecordsCount();
  console.log("total records", totalRecords)
  await middleFunction(totalRecords);
  await dbInstance.close();
  console.log("Connection closed");
  return Promise.resolve();
}