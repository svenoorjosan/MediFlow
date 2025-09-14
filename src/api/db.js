import { MongoClient } from 'mongodb';

let _client;
let _coll;

/** Get the `jobs` collection (singleton connection). */
export async function jobs() {
  if (_coll) return _coll;

  const uri = process.env.COSMOS_URI;
  if (!uri) throw new Error('COSMOS_URI not set');

  // Works with Cosmos Mongo API; retryWrites must be false
  _client = new MongoClient(uri, { retryWrites: false, appName: 'mediaflow-api' });
  await _client.connect();

  const dbName = process.env.COSMOS_DB || 'mediaflow';
  const collName = process.env.COSMOS_COLL || 'jobs';
  _coll = _client.db(dbName).collection(collName);

  return _coll;
}
