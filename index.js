import dotenv from "dotenv";
import fs from "fs";
import csv from "csv-parser";
import api from "zacks-api";
import stripBomStream from "strip-bom-stream";
import { setup, getPool } from "./db-setup.js";

dotenv.config();

async function main() {
  await setup(); // Run the database setup
  const pool = getPool(); // Get a new pool for our application
  const client = await pool.connect(); // Get a single client to use for all operations

  async function getStockId(ticker, name) {
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
  }

  async function saveZacksRanking(stockId, data) {
    try {
      console.log(`data: ${JSON.stringify(data)}`);
      await client.query(
        "INSERT INTO zacks_rankings (stock_id, zacksRankText, zacksRank, updatedAt) VALUES ($1, $2, $3, $4)",
        [stockId, data.zacksRankText, data.zacksRank, data.updatedAt]
      );
    } catch (error) {
      if (error.code === "23505") {
        console.log(
          `Duplicate entry for stock ID ${stockId} on ${data.updatedAt} not inserted.`
        );
      } else {
        throw error;
      }
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
    return null;
  }

  function processCSV() {
    return new Promise((resolve, reject) => {
      const pendingPromises = [];
      const stream = fs
        .createReadStream("stocks.csv", { encoding: "utf8" })
        .pipe(stripBomStream())
        .pipe(csv());

      stream.on("data", (row) => {
        const ticker = row["ticker"];
        const name = row["name"];
        const promise = (async () => {
          try {
            const data = await getDataWithRetry(ticker);
            if (data) {
              const stockId = await getStockId(ticker, name);
              await saveZacksRanking(stockId, data);
            } else {
              console.error(
                `Failed to retrieve data for ${ticker} after retries.`
              );
            }
          } catch (error) {
            console.error(`Error processing ${ticker}:`, error);
          }
        })();
        pendingPromises.push(promise);
      });

      stream.on("end", () => {
        Promise.all(pendingPromises)
          .then(() => {
            console.log("CSV file processing completed.");
            resolve();
          })
          .catch(reject);
      });

      stream.on("error", (error) => {
        reject(error);
      });
    });
  }

  try {
    await processCSV();
  } catch (error) {
    console.error("Error processing CSV:", error);
  } finally {
    await client.release(); // Release the client
    await pool.end(); // Close the pool when all processing is done
    console.log("Database connections closed. Exiting...");
  }
}

main()
  .catch(console.error)
  .finally(() => {
    console.log("Program execution completed. Exiting...");
    process.exit(0); // Ensure the program exits
  });
