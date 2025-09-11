import dotenv from 'dotenv';
dotenv.config();

import { MongoClient } from 'mongodb';

// ── create client WITHOUT serverApi ───────────────────────────
const client = new MongoClient(process.env.COSMOS_URI, {
  // useNewUrlParser / useUnifiedTopology are defaults in driver v5
});

export async function jobs() {
  if (!client.topology) await client.connect();
  return client
    .db(process.env.COSMOS_DB)
    .collection(process.env.COSMOS_COLL);
}
