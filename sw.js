/**
 * AGSIST Service Worker — v1
 * ─────────────────────────────────────────────────────────────────
 * CACHE STRATEGY:
 *   HTML pages      → Network first, cache fallback (always fresh)
 *   JS/CSS/images   → Cache first IF versioned (?v=N), else network first
 *   Data (JSON)     → Network only, no caching (prices must be live)
 *
 * TO BUST CACHE FOR ALL USERS ON DEPLOY:
 *   Increment CACHE_VERSION below by 1, commit, push.
 *   Every visitor's browser will detect the new worker, activate it,
 *   delete old caches, and fetch fresh files automatically.
 *
 * ─────────────────────────────────────────────────────────────────
 * BUMP THIS ON EVERY DEPLOY:
 */
var CACHE_VERSION = 1;
/* ───────────────────────────────────────────────────────────────── */

var CACHE_NAME = 'agsist-v' + CACHE_VERSION;

// These paths are always fetched from network — never cached
var NEVER_CACHE = [
  '/data/',          // prices.json, daily.json, markets.json etc — must be live
  '/api/',
  'open-meteo.com',
  'nominatim.openstreetmap.org',
  'ondemand.websol.barchart.com',
  'farmers1st.com/api',
];

// ── Install: open new cache (don't pre-cache anything) ────────────
self.addEventListener('install', function(e) {
  self.skipWaiting(); // activate immediately, don't wait for old tabs to close
});

// ── Activate: delete all old caches ──────────────────────────────
self.addEventListener('activate', function(e) {
  e.waitUntil(
    caches.keys().then(function(keys) {
      return Promise.all(
        keys.map(function(key) {
          if (key !== CACHE_NAME) {
            return caches.delete(key);
          }
        })
      );
    }).then(function() {
      return self.clients.claim(); // take control of all open tabs immediately
    })
  );
});

// ── Fetch: decide strategy per request ───────────────────────────
self.addEventListener('fetch', function(e) {
  var url = e.request.url;

  // Skip non-GET requests
  if (e.request.method !== 'GET') return;

  // Skip chrome-extension and non-http requests
  if (!url.startsWith('http')) return;

  // Never cache data endpoints or external APIs
  for (var i = 0; i < NEVER_CACHE.length; i++) {
    if (url.indexOf(NEVER_CACHE[i]) >= 0) {
      e.respondWith(fetch(e.request));
      return;
    }
  }

  // HTML pages → network first, cache fallback
  var isHTML = e.request.headers.get('accept') &&
               e.request.headers.get('accept').indexOf('text/html') >= 0;
  if (isHTML) {
    e.respondWith(networkFirst(e.request));
    return;
  }

  // Versioned assets (?v=N in URL) → cache first (they won't change)
  var isVersioned = url.indexOf('?v=') >= 0;
  if (isVersioned) {
    e.respondWith(cacheFirst(e.request));
    return;
  }

  // Everything else (unversioned JS, CSS, images) → network first
  e.respondWith(networkFirst(e.request));
});

// ── Network first: try network, fall back to cache ────────────────
function networkFirst(request) {
  return fetch(request).then(function(response) {
    if (response && response.ok) {
      var copy = response.clone();
      caches.open(CACHE_NAME).then(function(cache) {
        cache.put(request, copy);
      });
    }
    return response;
  }).catch(function() {
    return caches.match(request);
  });
}

// ── Cache first: serve from cache, update cache in background ─────
function cacheFirst(request) {
  return caches.match(request).then(function(cached) {
    var networkFetch = fetch(request).then(function(response) {
      if (response && response.ok) {
        caches.open(CACHE_NAME).then(function(cache) {
          cache.put(request, response.clone());
        });
      }
      return response;
    });
    return cached || networkFetch;
  });
}
