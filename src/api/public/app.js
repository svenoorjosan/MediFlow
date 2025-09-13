(function () {
  const byId = (id) => document.getElementById(id);
  const fileInput = byId('file');
  const pickBtn = byId('pickBtn');
  const drop = byId('drop');
  const bar = byId('bar');
  const status = byId('status');
  const out = byId('out');
  const copyId = byId('copyId');
  const apiBaseEl = byId('apiBase');
  const uploadsLink = byId('uploadsLink');
  const thumbsLink = byId('thumbsLink');
  const thumbBox = byId('thumbBox');
  const thumbImg = byId('thumbImg');

  const ORIGIN = window.location.origin; // same-host as API
  apiBaseEl.textContent = ORIGIN.replace(/^https?:\/\//, '');

  // Point convenience links at your Storage account (public containers)
  const blobBase = 'https://stmediaflow1757550168.blob.core.windows.net';
  uploadsLink.href = blobBase + '/uploads';
  thumbsLink.href = blobBase + '/thumbnails';

  pickBtn.addEventListener('click', () => fileInput.click());

  // Drag & drop styling
  ;['dragenter', 'dragover'].forEach(evt => drop.addEventListener(evt, (e) => {
    e.preventDefault(); e.stopPropagation(); drop.classList.add('dragover');
  }));
  ;['dragleave', 'drop'].forEach(evt => drop.addEventListener(evt, (e) => {
    e.preventDefault(); e.stopPropagation(); drop.classList.remove('dragover');
  }));
  drop.addEventListener('drop', (e) => {
    const dt = e.dataTransfer; if (!dt || !dt.files || !dt.files[0]) return;
    handleFile(dt.files[0]);
  });
  fileInput.addEventListener('change', () => {
    if (fileInput.files && fileInput.files[0]) handleFile(fileInput.files[0]);
  });

  function handleFile(file) {
    resetUI();
    if (!file.type.startsWith('image/')) {
      return setStatus('Please choose an image file.', true);
    }
    if (file.size > 25 * 1024 * 1024) { // 25MB soft cap for demo
      return setStatus('That file is a bit large for the demo (25MB limit).', true);
    }
    upload(file);
  }

  function upload(file) {
    const form = new FormData();
    form.append('file', file, file.name);

    const xhr = new XMLHttpRequest();
    xhr.open('POST', ORIGIN + '/api/upload');

    xhr.upload.onprogress = (e) => {
      if (!e.lengthComputable) return;
      const pct = Math.round((e.loaded / e.total) * 100);
      bar.style.width = pct + '%';
      setStatus('Uploading… ' + pct + '%');
    };

    xhr.onload = () => {
      try {
        const ok = xhr.status >= 200 && xhr.status < 300;
        const json = ok ? JSON.parse(xhr.responseText || '{}') : null;
        if (!ok) throw new Error('Upload failed: ' + xhr.status + ' ' + xhr.statusText);

        out.textContent = JSON.stringify(json, null, 2);
        setStatus('Uploaded. Job queued.');
        bar.style.width = '100%';

        if (json && json.id) {
          copyId.disabled = false;
          copyId.onclick = () => navigator.clipboard.writeText(json.id).then(() => {
            copyId.textContent = 'Copied';
            setTimeout(() => (copyId.textContent = 'Copy job id'), 1200);
          });
        }

        // If the API already returns a thumbUrl (fast worker), show it.
        if (json && json.thumbUrl) {
          thumbImg.src = json.thumbUrl;
          thumbBox.style.display = 'block';
        }
      } catch (err) {
        console.error(err);
        setStatus(err.message || String(err), true);
      }
    };

    xhr.onerror = () => setStatus('Network error while uploading.', true);
    xhr.send(form);
  }

  function resetUI() {
    bar.style.width = '0%';
    out.textContent = '(uploading…)';
    thumbBox.style.display = 'none';
    thumbImg.removeAttribute('src');
    setStatus('Starting upload…');
  }

  function setStatus(msg, isError) {
    status.textContent = msg;
    status.style.color = isError ? '#fca5a5' : '#94a3b8';
  }
})();