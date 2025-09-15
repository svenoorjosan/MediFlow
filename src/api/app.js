// src/api/app.js
import express from 'express';
import multer from 'multer';
import dotenv from 'dotenv';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  BlobServiceClient,
  StorageSharedKeyCredential,
  BlobSASPermissions,
  generateBlobSASQueryParameters
} from '@azure/storage-blob';
import { jobs } from './db.js';

dotenv.config();

const app = express();
const port = process.env.PORT || 3001;

// ESM-friendly __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/* ── Static UI ──────────────────────────────────────────────── */
app.use(
  express.static(path.join(__dirname, 'public'), {
    maxAge: '1h',
    etag: true,
  })
);
app.get('/', (_req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

/* ── Multer (memory) ───────────────────────────────────────── */
const upload = multer({ storage: multer.memoryStorage() });

/* ── Helpers ───────────────────────────────────────────────── */
function parseStorageConn(conn) {
  const g = (k) => (new RegExp(`${k}=([^;]+)`).exec(conn) || [])[1];
  return { accountName: g('AccountName'), accountKey: g('AccountKey') };
}

function getUploadsContainerClient() {
  const conn = process.env.AZURE_STORAGE_CONNECTION_STRING;
  const name = process.env.AZURE_STORAGE_CONTAINER || 'uploads';
  if (!conn) throw new Error('AZURE_STORAGE_CONNECTION_STRING not set');
  return BlobServiceClient.fromConnectionString(conn).getContainerClient(name);
}

// READ SAS (default 15 min)
function makeReadSasUrl(containerName, blobName, minutes = 15) {
  const conn = process.env.AZURE_STORAGE_CONNECTION_STRING;
  const { accountName, accountKey } = parseStorageConn(conn);
  if (!accountName || !accountKey) {
    throw new Error('AccountName/AccountKey missing in storage connection string');
  }
  const credential = new StorageSharedKeyCredential(accountName, accountKey);
  const startsOn = new Date(Date.now() - 60 * 1000);
  const expiresOn = new Date(Date.now() + minutes * 60 * 1000);
  const sas = generateBlobSASQueryParameters(
    { containerName, blobName, permissions: BlobSASPermissions.parse('r'), startsOn, expiresOn },
    credential
  ).toString();
  return `https://${accountName}.blob.core.windows.net/${containerName}/${encodeURIComponent(blobName)}?${sas}`;
}

function reqProtoHost(req) {
  return `${req.headers['x-forwarded-proto'] || 'https'}://${req.headers.host}`;
}

/* ── Password gate (per-route) ─────────────────────────────── */
function getProvidedPassword(req) {
  const hdr = (n) => (req.get?.(n) || req.headers?.[n] || '').toString();
  const bearer = (req.headers?.authorization || '').replace(/^Bearer\s+/i, '');
  const q = (k) => (req.query?.[k] ?? '').toString();
  const b = (k) => (req.body?.[k] ?? '').toString(); // works after multer on multipart

  // priority: header → bearer → query → form field
  return (
    hdr('x-password') ||
    hdr('x-api-key') ||
    bearer ||
    q('password') || q('pass') || q('token') ||
    b('password') || b('pass') || b('token') ||
    ''
  ).trim();
}

function requirePasswordMiddleware(req, res, next) {
  const requirePassword = /^true$/i.test(process.env.REQUIRE_PASSWORD || '');
  if (!requirePassword) return next();

  const expected = (process.env.UPLOAD_PASSWORD || '').toString();
  const provided = getProvidedPassword(req);

  console.log('[upload-auth]', {
    requirePassword,
    requiredLen: expected.length,
    providedLen: provided.length
  });

  if (!expected) {
    return res.status(503).json({ error: 'upload password not configured on server' });
  }
  if (!provided) return res.status(400).json({ error: 'missing password' });
  if (provided !== expected) return res.status(401).json({ error: 'invalid password' });

  next();
}

/* ── Routes ────────────────────────────────────────────────── */
app.get('/api/health', (_req, res) => res.json({ ok: true }));

app.get('/api/config', (req, res) => {
  const conn = process.env.AZURE_STORAGE_CONNECTION_STRING || '';
  const { accountName } = parseStorageConn(conn);
  const blobBaseUrl = accountName ? `https://${accountName}.blob.core.windows.net` : null;

  res.json({
    apiBase: `${reqProtoHost(req)}`,
    blobBaseUrl,
    uploadsContainer: process.env.AZURE_STORAGE_CONTAINER || 'uploads',
    thumbsContainer: process.env.THUMBS_CONTAINER || 'thumbnails',
    // helpful flags for the UI
    passwordRequired: /^true$/i.test(process.env.REQUIRE_PASSWORD || ''),
    acceptedHeaders: ['x-password', 'x-api-key', 'authorization Bearer'],
    acceptedFields: ['password', 'pass', 'token'],
  });
});

// Diagnostic: what auth did we see?
app.get('/api/_diag/auth', (req, res) => {
  const provided = getProvidedPassword(req);
  res.json({
    providedLen: provided.length,
    hasBearer: /bearer\s+/i.test(req.headers?.authorization || ''),
    hasXPassword: Boolean(req.get?.('x-password') || req.headers?.['x-password']),
    hasQuery: Boolean(req.query?.password || req.query?.pass || req.query?.token)
  });
});

// Upload (multipart): run multer first, then the password gate so we can see form fields.
app.post('/api/upload', upload.single('file'), requirePasswordMiddleware, async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'no file' });

    const containerClient = getUploadsContainerClient();
    const containerName = containerClient.containerName;

    const safeName = req.file.originalname.replace(/[^\w.\-]/g, '_');
    const blobName = `${Date.now()}-${safeName}`;
    const blockBlob = containerClient.getBlockBlobClient(blobName);

    await blockBlob.uploadData(req.file.buffer, {
      blobHTTPHeaders: { blobContentType: req.file.mimetype || 'application/octet-stream' },
    });

    try { await blockBlob.setMetadata({ jobId: blobName }); } catch { }

    const coll = await jobs(); // Cosmos Mongo API
    await coll.insertOne({
      id: blobName, _id: blobName,
      url: blockBlob.url,
      original: req.file.originalname,
      status: 'queued',
      createdAt: new Date(),
    });

    const readUrl = makeReadSasUrl(containerName, blobName, 15);
    res.json({ id: blobName, url: readUrl, original: req.file.originalname });
  } catch (err) {
    console.error(err);
    let msg = err?.message || 'upload failed';
    if (/Mongo.*Password cannot be empty/i.test(String(err))) {
      msg = 'Cosmos URI missing/invalid password (set COSMOS_URI with URL-encoded key)';
    }
    res.status(500).json({ error: msg });
  }
});

// Return status + signed URLs by probing Storage (+ Cosmos best-effort)
const THUMBS = process.env.THUMBS_CONTAINER || 'thumbnails';
app.get('/api/job/:id', async (req, res) => {
  try {
    const id = req.params.id;

    // Try Cosmos (best-effort)
    let doc = null;
    try {
      const coll = await jobs();
      doc = await coll.findOne({ _id: id });
    } catch { /* Cosmos optional here */ }

    // Probe Storage for both sizes
    const conn = process.env.AZURE_STORAGE_CONNECTION_STRING;
    const thumbsCC = BlobServiceClient.fromConnectionString(conn).getContainerClient(THUMBS);
    const name1x = `${id}.thumb.jpg`;
    const name2x = `${id}.thumb@2x.jpg`;
    const blob1x = thumbsCC.getBlockBlobClient(name1x);
    const blob2x = thumbsCC.getBlockBlobClient(name2x);

    let thumbUrl = null, thumb2xUrl = null;
    if (await blob1x.exists()) thumbUrl = makeReadSasUrl(THUMBS, name1x, 15);
    if (await blob2x.exists()) thumb2xUrl = makeReadSasUrl(THUMBS, name2x, 15);

    if (!doc && !thumbUrl && !thumb2xUrl) {
      return res.status(404).json({ error: 'not found' });
    }

    const status = (doc?.status) || ((thumbUrl || thumb2xUrl) ? 'done' : 'queued');
    res.json({ id, status, thumbUrl, thumb2xUrl });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: e?.message || 'lookup failed' });
  }
});

app.listen(port, () => {
  console.log(`API listening on :${port}`);
  console.log('Env sanity:', {
    STORAGE_CONTAINER: process.env.AZURE_STORAGE_CONTAINER || 'uploads',
    COSMOS_DB: process.env.COSMOS_DB || 'mediaflow',
    COSMOS_COLL: process.env.COSMOS_COLL || 'jobs',
  });
});
