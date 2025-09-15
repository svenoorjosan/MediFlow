(() => {
  const apiBaseEl = document.getElementById('apiBase');
  const uploadsLink = document.getElementById('uploadsLink');
  const thumbsLink = document.getElementById('thumbsLink');
  const copyBtn = document.getElementById('copyId');
  const outEl = document.getElementById('out');
  const statusEl = document.getElementById('status');
  const barEl = document.getElementById('bar');
  const drop = document.getElementById('drop');
  const fileInput = document.getElementById('file');
  const pickBtn = document.getElementById('pickBtn');
  const thumbBox = document.getElementById('thumbBox');
  const thumbImg = document.getElementById('thumbImg');
  const thumbLink = document.getElementById('thumbLink');
  const thumbLinks = document.getElementById('thumbLinks');

  let lastId = null;
  let passwordRequired = false;

  // Helpful links + base label + whether password is required
  fetch('/api/config').then(r => r.json()).then(cfg => {
    const apiBase = cfg.apiBase || location.origin;
    apiBaseEl.textContent = apiBase.replace(/^https?:\/\//, '');
    if (cfg.blobBaseUrl) {
      uploadsLink.href = `${cfg.blobBaseUrl}/${cfg.uploadsContainer || 'uploads'}/`;
      thumbsLink.href = `${cfg.blobBaseUrl}/${cfg.thumbsContainer || 'thumbnails'}/`;
    } else {
      uploadsLink.removeAttribute('href'); thumbsLink.removeAttribute('href');
    }
    passwordRequired = !!cfg.passwordRequired;
  }).catch(() => { });

  pickBtn.addEventListener('click', () => fileInput.click());

  // Drag & drop
  ['dragenter', 'dragover'].forEach(evt => drop.addEventListener(evt, e => {
    e.preventDefault(); e.stopPropagation(); drop.classList.add('dragover');
  }));
  ['dragleave', 'drop'].forEach(evt => drop.addEventListener(evt, e => {
    e.preventDefault(); e.stopPropagation(); drop.classList.remove('dragover');
  }));
  drop.addEventListener('drop', e => {
    const f = e.dataTransfer.files?.[0]; if (f) uploadFile(f);
  });
  fileInput.addEventListener('change', () => {
    const f = fileInput.files?.[0]; if (f) uploadFile(f);
  });

  copyBtn.addEventListener('click', async () => {
    if (!lastId) return;
    await navigator.clipboard.writeText(lastId);
    copyBtn.textContent = 'Copied!'; setTimeout(() => copyBtn.textContent = 'Copy job id', 1200);
  });

  function setProgress(pct) { barEl.style.width = `${pct}%`; }
  function showResult(obj) { outEl.textContent = JSON.stringify(obj, null, 2); }

  // Render a clamped preview; default click opens 1×, link list offers 1×/2× explicitly
  function showThumb(j) {
    const url1x = j.thumbUrl || j.thumb2xUrl || '';
    const url2x = j.thumb2xUrl || null;

    // clamp via CSS (container .thumb max-width: 640px)
    if (url2x && url1x) {
      thumbImg.srcset = `${url1x} 1x, ${url2x} 2x`;
      thumbImg.sizes = '(max-width: 760px) 100vw, 640px';
    } else {
      thumbImg.removeAttribute('srcset');
      thumbImg.removeAttribute('sizes');
    }
    thumbImg.src = url1x;

    // make the image clickable → open 1× by default
    thumbLink.href = url1x;

    // helper links
    if (url2x) {
      thumbLinks.style.display = 'block';
      thumbLinks.innerHTML = `<a href="${url1x}" target="_blank" rel="noopener">Open 1×</a> · <a href="${url2x}" target="_blank" rel="noopener">Open 2×</a>`;
    } else {
      thumbLinks.style.display = 'none';
      thumbLinks.textContent = '';
    }

    thumbBox.style.display = 'block';
  }

  function pollJob(id) {
    const iv = setInterval(async () => {
      try {
        const r = await fetch(`/api/job/${encodeURIComponent(id)}`);
        if (!r.ok) return;
        const j = await r.json();
        if (j.thumbUrl || j.thumb2xUrl) {
          clearInterval(iv);
          statusEl.textContent = 'Done.';
          showThumb(j);
        } else {
          statusEl.textContent = `Processing… (status: ${j.status || 'queued'})`;
        }
      } catch { }
    }, 1500);
  }

  function uploadFile(file) {
    thumbBox.style.display = 'none';
    thumbImg.removeAttribute('src'); thumbImg.removeAttribute('srcset'); thumbImg.removeAttribute('sizes');
    statusEl.textContent = 'Uploading…'; setProgress(0);
    outEl.textContent = '(uploading…)'; copyBtn.disabled = true; lastId = null;

    const fd = new FormData(); fd.append('file', file);
    const xhr = new XMLHttpRequest(); xhr.open('POST', '/api/upload', true);

    // Send password if present
    const pwd = (localStorage.getItem('mf_pwd') || '').trim();
    if (pwd) xhr.setRequestHeader('x-password', pwd);
    else if (passwordRequired) {
      // Soft reminder in UI; upload will still attempt
      statusEl.textContent = 'Uploading… (tip: set password via localStorage.mf_pwd)';
    }

    xhr.upload.onprogress = e => { if (e.lengthComputable) setProgress(Math.round((e.loaded / e.total) * 100)); };
    xhr.onload = () => {
      try {
        const data = JSON.parse(xhr.responseText); showResult(data);
        if (data.id) { lastId = data.id; copyBtn.disabled = false; statusEl.textContent = 'Queued. Waiting for thumbnail…'; pollJob(data.id); }
        else if (data.error) statusEl.textContent = `Error: ${data.error}`;
        else statusEl.textContent = 'Uploaded.';
      } catch { statusEl.textContent = 'Upload finished, response not JSON.'; }
      setProgress(100);
    };
    xhr.onerror = () => { statusEl.textContent = 'Upload failed.'; };
    xhr.send(fd);
  }
})();
