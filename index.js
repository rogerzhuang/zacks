// index.js
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

  let totalProcessed = 0;
  let successfulSaves = 0;
  let failedRetrievals = 0;
  let failedSaves = 0;
  let noRankingStocks = 0;
  let totalInserts = 0;
  let totalUpdates = 0;
  let failedProcessing = 0; // Add this counter

  async function getOrCreateStockId(ticker, name) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      let res = await client.query("SELECT id FROM stocks WHERE ticker = $1", [
        ticker,
      ]);
      if (res.rows.length > 0) {
        await client.query("COMMIT");
        return res.rows[0].id;
      } else {
        res = await client.query(
          "INSERT INTO stocks (ticker, name) VALUES ($1, $2) ON CONFLICT (ticker) DO UPDATE SET name = $2 RETURNING id",
          [ticker, name]
        );
        await client.query("COMMIT");
        return res.rows[0].id;
      }
    } catch (error) {
      await client.query("ROLLBACK");
      console.error(`Error in getOrCreateStockId for ticker ${ticker}:`, error);
      throw error;
    } finally {
      client.release();
    }
  }

  async function saveZacksRanking(stockId, data) {
    // Validate data before database operations
    if (
      !data.zacksRankText ||
      data.zacksRank == null ||
      data.updatedAt == null ||
      isNaN(data.zacksRank)
    ) {
      console.warn(`Invalid data for stock ID ${stockId}, skipping save.`);
      noRankingStocks++; // Increment here
      return;
    }

    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query(
        `INSERT INTO zacks_rankings (stock_id, zacksRankText, zacksRank, updatedAt) 
         VALUES ($1, $2, $3, $4) 
         ON CONFLICT (stock_id, updatedAt) DO UPDATE 
         SET zacksRankText = EXCLUDED.zacksRankText, zacksRank = EXCLUDED.zacksRank
         RETURNING (xmax = 0) AS inserted;`,
        [stockId, data.zacksRankText, data.zacksRank, data.updatedAt]
      );
      await client.query("COMMIT");
      const isInserted = result.rows[0].inserted;
      if (isInserted) {
        console.log(
          `Inserted new ranking for stock ID ${stockId} on ${data.updatedAt}`
        );
        totalInserts++;
      } else {
        console.log(
          `Updated existing ranking for stock ID ${stockId} on ${data.updatedAt}`
        );
        totalUpdates++;
      }
      successfulSaves++;
    } catch (error) {
      await client.query("ROLLBACK");
      console.error(`Error saving ranking for stock ID ${stockId}:`, error);
      failedSaves++;
      noRankingStocks++; // Increment here as well
      throw error; // Re-throw to handle in processCSV
    } finally {
      client.release();
    }
  }

  async function getDataWithRetry(ticker, maxRetries = 3, delay = 1000) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        let data;
        try {
          data = await api.getData(ticker);
        } catch (error) {
          console.error(`Error in api.getData for ${ticker}:`, error);
          throw error; // Re-throw to trigger the retry mechanism
        }

        // Check if data is null (no Zacks ranking)
        if (data === null) {
          console.log(`No Zacks ranking available for ${ticker}`);
          noRankingStocks++;
          return null;
        }

        // Validate the data
        if (typeof data !== "object") {
          throw new Error("Invalid data received from API");
        }

        // Additional validation for data.zacksRank and data.zacksRankText
        if (data.zacksRank == null || data.zacksRankText == null) {
          console.warn(`Incomplete data received for ${ticker}, skipping.`);
          noRankingStocks++;
          return null;
        }

        // Check if updatedAt is a valid date
        if (
          typeof data.updatedAt === "string" ||
          data.updatedAt instanceof Date
        ) {
          const parsedDate = new Date(data.updatedAt);
          if (!isNaN(parsedDate.getTime())) {
            data.updatedAt = parsedDate;
          } else {
            console.warn(
              `Invalid date for ${ticker}: ${data.updatedAt}. Using current date.`
            );
            data.updatedAt = new Date();
          }
        } else {
          console.warn(`Missing updatedAt for ${ticker}. Using current date.`);
          data.updatedAt = new Date();
        }

        return data;
      } catch (error) {
        console.error(`Attempt ${attempt} failed for ${ticker}:`, error);
        if (attempt < maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }
    console.log(
      `Failed to retrieve data for ${ticker} after ${maxRetries} attempts.`
    );
    failedRetrievals++;
    noRankingStocks++; // Increment here
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
            totalProcessed++;
            const data = await getDataWithRetry(ticker);
            if (data) {
              const stockId = await getOrCreateStockId(ticker, name);
              await saveZacksRanking(stockId, data);
            } else {
              console.log(
                `Skipping DB insertion for ${ticker} due to null or invalid API response.`
              );
              // No need to increment noRankingStocks here; already incremented in getDataWithRetry or saveZacksRanking
            }
          } catch (error) {
            console.error(`Error processing ${ticker}:`, error);
            failedProcessing++; // Increment failedProcessing
            noRankingStocks++; // Increment noRankingStocks as the stock wasn't processed successfully
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
    await pool.end(); // Close the pool when all processing is done
    console.log("Database connections closed. Exiting...");
    console.log(`Total processed: ${totalProcessed}`);
    console.log(`Successful database operations: ${successfulSaves}`);
    console.log(`Total inserts: ${totalInserts}`);
    console.log(`Total updates: ${totalUpdates}`);
    console.log(`Failed retrievals: ${failedRetrievals}`);
    console.log(`Failed saves: ${failedSaves}`);
    console.log(`Failed processing: ${failedProcessing}`);
    console.log(`Stocks with no Zacks ranking: ${noRankingStocks}`);
  }
}

main()
  .catch(console.error)
  .finally(() => {
    console.log("Program execution completed. Exiting...");
    process.exit(0); // Ensure the program exits
  });
