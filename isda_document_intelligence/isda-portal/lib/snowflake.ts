import snowflake from "snowflake-sdk";
import fs from "fs";

snowflake.configure({ logLevel: "ERROR" });

let connection: snowflake.Connection | null = null;
let cachedToken: string | null = null;

function getOAuthToken(): string | null {
  const tokenPath = "/snowflake/session/token";
  try {
    if (fs.existsSync(tokenPath)) {
      return fs.readFileSync(tokenPath, "utf8");
    }
  } catch {
    // Not in SPCS environment
  }
  return null;
}

function getConfig(): snowflake.ConnectionOptions {
  const base = {
    account: process.env.SNOWFLAKE_ACCOUNT || "",
    warehouse: process.env.SNOWFLAKE_WAREHOUSE || "COMPUTE_WH",
    database: process.env.SNOWFLAKE_DATABASE || "ISDA_DOCUMENT_POC",
    schema: process.env.SNOWFLAKE_SCHEMA || "DOCUMENT_INTELLIGENCE",
  };

  // Check for SPCS OAuth token first
  const oauthToken = getOAuthToken();
  if (oauthToken) {
    return {
      ...base,
      host: process.env.SNOWFLAKE_HOST,
      token: oauthToken,
      authenticator: "oauth",
    };
  }

  // Use PAT (Programmatic Access Token) if provided
  const pat = process.env.SNOWFLAKE_PAT;
  if (pat) {
    return {
      ...base,
      username: process.env.SNOWFLAKE_USER || "",
      token: pat,
      authenticator: "PROGRAMMATIC_ACCESS_TOKEN",
    };
  }

  // Fallback to external browser (for local dev with manual auth)
  return {
    ...base,
      username: process.env.SNOWFLAKE_USER || "",
      authenticator: "EXTERNALBROWSER",
  };
}

async function getConnection(): Promise<snowflake.Connection> {
  const oauthToken = getOAuthToken();
  const pat = process.env.SNOWFLAKE_PAT;

  if (connection && (!oauthToken || oauthToken === cachedToken)) {
    return connection;
  }

  if (connection) {
    console.log("Token changed, reconnecting");
    connection.destroy(() => {});
  }

  const authType = oauthToken ? "OAuth" : pat ? "PAT" : "external browser";
  console.log(`Connecting with ${authType}`);
  
  const conn = snowflake.createConnection(getConfig());
  
  // PAT and OAuth use connect(), external browser needs connectAsync()
  if (oauthToken || pat) {
    await new Promise<void>((resolve, reject) => {
      conn.connect((err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  } else {
    await new Promise<void>((resolve, reject) => {
      conn.connectAsync((err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }
  
  connection = conn;
  cachedToken = oauthToken;
  return connection;
}

function isRetryableError(err: unknown): boolean {
  const error = err as { message?: string; code?: number };
  return !!(
    error.message?.includes("OAuth access token expired") ||
    error.message?.includes("terminated connection") ||
    error.code === 407002
  );
}

export async function query<T>(sql: string, retries = 1): Promise<T[]> {
  try {
    const conn = await getConnection();
    return await new Promise<T[]>((resolve, reject) => {
      conn.execute({
        sqlText: sql,
        complete: (err, stmt, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve((rows || []) as T[]);
          }
        },
      });
    });
  } catch (err) {
    console.error("Query error:", (err as Error).message);
    if (retries > 0 && isRetryableError(err)) {
      connection = null;
      return query(sql, retries - 1);
    }
    throw err;
  }
}

export async function getToken(): Promise<string> {
  // For PAT auth, return the PAT directly
  const pat = process.env.SNOWFLAKE_PAT;
  if (pat) {
    return pat;
  }
  
  // For OAuth/SPCS, try to get from connection
  const conn = await getConnection();
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (conn as any).rest?.token || "";
}

export function getAuthHeader(): string {
  const pat = process.env.SNOWFLAKE_PAT;
  if (pat) {
    // For REST API, use Bearer token format
    return `Bearer ${pat}`;
  }
  return "";
}

export function getHost(): string {
  const account = process.env.SNOWFLAKE_ACCOUNT || "";
  return process.env.SNOWFLAKE_HOST || `${account}.snowflakecomputing.com`;
}
