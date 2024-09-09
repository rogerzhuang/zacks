import pg from "pg";
import dotenv from "dotenv";

dotenv.config();

const { Pool } = pg;

const dbConfig = {
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  port: process.env.PGPORT,
  database: process.env.PGDATABASE,
};

async function setupDatabase() {
  // Connect to 'postgres' database to create our app database if it doesn't exist
  const pgPool = new Pool({
    ...dbConfig,
    database: "postgres",
  });

  try {
    // Check if our database exists
    const res = await pgPool.query(
      "SELECT 1 FROM pg_database WHERE datname=$1",
      [process.env.PGDATABASE]
    );

    if (res.rows.length === 0) {
      // Database doesn't exist, so create it
      await pgPool.query(`CREATE DATABASE ${process.env.PGDATABASE}`);
      console.log(`Database ${process.env.PGDATABASE} created.`);
    }
  } finally {
    await pgPool.end();
  }

  // Now connect to our app database
  const appPool = new Pool(dbConfig);

  try {
    // Create tables if they don't exist
    await appPool.query(`
      CREATE TABLE IF NOT EXISTS stocks (
        id SERIAL PRIMARY KEY,
        ticker VARCHAR(10) UNIQUE NOT NULL,
        name VARCHAR(255) NOT NULL
      );

      CREATE TABLE IF NOT EXISTS zacks_rankings (
        id SERIAL PRIMARY KEY,
        stock_id INTEGER REFERENCES stocks(id) ON DELETE CASCADE,
        zacksRankText VARCHAR(50),
        zacksRank INTEGER,
        updatedAt TIMESTAMP,
        UNIQUE(stock_id, updatedAt)
      );
    `);
    console.log("Tables created if they didn't exist.");
  } finally {
    await appPool.end();
  }
}

// Export the setup function and a function to get a new pool
export const setup = setupDatabase;
export const getPool = () => new Pool(dbConfig);
