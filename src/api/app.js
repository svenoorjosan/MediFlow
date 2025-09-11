import express from 'express';
import multer from 'multer';
import dotenv from 'dotenv';
import { BlobServiceClient } from '@azure/storage-blob';
import { jobs } from './db.js';

dotenv.config();
console.log('COSMOS_URI loaded:', process.env.COSMOS_URI?.slice(0, 40));

const app = express();
const port = process.env.PORT || 3001;

/* ── Multer: keep uploads in memory ─────────────────────────── */
const upload = multer({ storage: multer.memoryStorage() });

/* ── Azure Blob client ──────────────────────────────────────── */
const blobService = BlobServiceClient.fromConnectionString(
  process.env.AZURE_STORAGE_CONNECTION_STRING
);
const containerClient = blobService.getContainerClient(
  process.env.AZURE_STORAGE_CONTAINER   // “uploads”
);

/* ── Routes ─────────────────────────────────────────────────── */
app.get('/api/health', (_, res) => res.json({ ok: true }));

app.post('/api/upload', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'no file' });

    const blobName = `${Date.now()}-${req.file.originalname}`;
    const blockBlob = containerClient.getBlockBlobClient(blobName);

    await blockBlob.uploadData(req.file.buffer, {
      blobHTTPHeaders: { blobContentType: req.file.mimetype }
    });

    // job metadata
    await (await jobs()).insertOne({
      id: blobName,          // shard key
      _id: blobName,          // shard key
      url: blockBlob.url,
      original: req.file.originalname,
      status: 'queued',
      createdAt: new Date()
    });

    res.json({
      id: blobName,
      url: blockBlob.url,
      original: req.file.originalname
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'upload failed' });
  }
});

app.listen(port, () => console.log(`API listening on :${port}`));
