import dotenv from "dotenv";
dotenv.config();

import fs from "fs";
import csv from "csv-parser";
import api from "zacks-api";
import pg from "pg";
const { Pool } = pg;

import stripBomStream from "strip-bom-stream";

const pool = new Pool(); // Automatically uses environment variables

async function getStockId(ticker, name) {
  const client = await pool.connect();
  try {
    let res = await client.query("SELECT id FROM stocks WHERE ticker = $1", [
      ticker,
    ]);
    if (res.rows.length > 0) {
      return res.rows[0].id;
    } else {
      res = await client.query(
        "INSERT INTO stocks (ticker, name) VALUES ($1, $2) RETURNING id",
        [ticker, name]
      );
      return res.rows[0].id;
    }
  } finally {
    client.release();
  }
}

async function saveZacksRanking(stockId, data) {
  const client = await pool.connect();
  try {
    console.log(`data: ${JSON.stringify(data)}`);
    await client.query(
      "INSERT INTO zacks_rankings (stock_id, zacksRankText, zacksRank, updatedAt) VALUES ($1, $2, $3, $4)",
      [stockId, data.zacksRankText, data.zacksRank, data.updatedAt]
    );
  } catch (error) {
    if (error.code === "23505") {
      // unique_violation error code in PostgreSQL
      console.log(
        `Duplicate entry for stock ID ${stockId} on ${data.updatedAt} not inserted.`
      );
    } else {
      throw error; // rethrow the error if it's not a unique violation
    }
  } finally {
    client.release();
  }
}

async function getDataWithRetry(ticker, maxRetries = 3, delay = 1000) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await api.getData(ticker);
    } catch (error) {
      console.error(`Attempt ${attempt} failed for ${ticker}:`, error);
      if (attempt < maxRetries) {
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }
  return null; // Indicate failure after all retries
}

fs.createReadStream("stocks.csv", { encoding: "utf8" })
  .pipe(stripBomStream())
  .pipe(csv())
  .on("data", async (row) => {
    const ticker = row["ticker"];
    const name = row["name"];
    try {
      const data = await getDataWithRetry(ticker);
      if (data) {
        // Only proceed if data is not null
        const stockId = await getStockId(ticker, name);
        await saveZacksRanking(stockId, data);
      } else {
        console.error(`Failed to retrieve data for ${ticker} after retries.`);
      }
    } catch (error) {
      console.error(`Error processing ${ticker}:`, error);
    }
  })
  .on("end", () => {
    console.log("CSV file processing completed.");
  });
