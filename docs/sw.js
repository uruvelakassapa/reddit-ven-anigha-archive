const CACHE = "va-archive-v3";

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (event) => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)));
    await self.clients.claim();
  })());
});

async function fromCache(request) {
  const cache = await caches.open(CACHE);
  const match = await cache.match(request);
  if (match) return match;
  const ignored = await cache.match(request, { ignoreSearch: true });
  if (ignored) return ignored;
  const url = new URL(request.url);
  if (url.pathname.endsWith("/")) {
    return cache.match(new URL("index.html", url).href);
  }
  return undefined;
}

self.addEventListener("fetch", (event) => {
  const req = event.request;
  if (req.method !== "GET") return;
  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;

  event.respondWith((async () => {
    const cached = await fromCache(req);
    const network = fetch(req).then((resp) => {
      if (resp && resp.ok) {
        caches.open(CACHE).then((cache) => cache.put(req, resp.clone()));
      }
      return resp;
    }).catch(() => undefined);
    if (cached) {
      void network;
      return cached;
    }
    const resp = await network;
    if (resp) return resp;
    return new Response("Offline", {
      status: 503,
      headers: { "Content-Type": "text/plain" },
    });
  })());
});
