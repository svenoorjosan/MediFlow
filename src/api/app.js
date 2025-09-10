import express from 'express';
import multer from 'multer';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

dotenv.config();                   // reads .env

const app = express();
const port = process.env.PORT || 3001;

// ─── Storage setup (local for now) ────────────────────────────────────────────
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const uploadDir = path.join(__dirname, '../../uploads');
const upload = multer({ dest: uploadDir });   // disk storage

// ─── Routes ───────────────────────────────────────────────────────────────────
app.get('/api/health', (_, res) => res.json({ ok: true }));

app.post('/api/upload', upload.single('file'), (req, res) => {
  // Multer adds `req.file`
  if (!req.file) return res.status(400).json({ error: 'No file' });
  res.json({ id: req.file.filename, original: req.file.originalname });
});

app.listen(port, () => console.log(`API listening on :${port}`));
