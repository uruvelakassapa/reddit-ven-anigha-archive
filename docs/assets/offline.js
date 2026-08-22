(function () {
  const CACHE = "va-archive-v3";
  const script = document.currentScript;
  const rootAttr = (script && script.getAttribute("data-root")) || "./";
  const siteRoot = new URL(rootAttr, location.href);
  const bar = document.querySelector(".offline-bar");
  const btn = document.getElementById("offline-save");
  const status = document.getElementById("offline-status");
  if (!bar || !btn || !status) return;
  if (!("serviceWorker" in navigator) || !("caches" in window)) return;

  bar.hidden = false;

  function setStatus(text) {
    status.textContent = text || "";
  }

  function markSaved() {
    btn.hidden = true;
    setStatus("Available offline");
  }

  navigator.serviceWorker
    .register(new URL("sw.js", siteRoot), { updateViaCache: "none" })
    .catch(function () {});

  async function fileList() {
    const resp = await fetch(new URL("offline-files.json", siteRoot));
    if (!resp.ok) throw new Error("Could not load file list");
    return resp.json();
  }

  async function alreadySaved(files) {
    try {
      const n = localStorage.getItem("va-offline-n");
      if (!(n && Number(n) >= files.length)) return false;
      const cache = await caches.open(CACHE);
      const keys = await cache.keys();
      return keys.length >= files.length;
    } catch (e) {
      return false;
    }
  }

  fileList()
    .then(async function (files) {
      if (await alreadySaved(files)) markSaved();
    })
    .catch(function () {});

  btn.addEventListener("click", async function () {
    btn.disabled = true;
    setStatus("Saving\u2026");
    try {
      const files = await fileList();
      const cache = await caches.open(CACHE);
      let done = 0;
      let failed = 0;
      const total = files.length;
      const chunk = 8;
      for (let i = 0; i < files.length; i += chunk) {
        const batch = files.slice(i, i + chunk);
        await Promise.all(batch.map(async function (file) {
          const url = new URL(file, siteRoot).href;
          try {
            const hit = await cache.match(url);
            if (!hit) {
              const resp = await fetch(url, { credentials: "same-origin" });
              if (!resp.ok) throw new Error(String(resp.status));
              await cache.put(url, resp);
            }
          } catch (e) {
            failed += 1;
          }
          done += 1;
          setStatus("Saving " + done + "/" + total + "\u2026");
        }));
      }
      try {
        localStorage.setItem("va-offline-n", String(total - failed));
      } catch (e) {}
      if (failed) {
        btn.disabled = false;
        btn.hidden = false;
        setStatus("Saved " + (total - failed) + "/" + total + " \u2014 retry to finish");
      } else {
        markSaved();
      }
    } catch (e) {
      btn.disabled = false;
      setStatus("Could not save offline.");
    }
  });
})();
