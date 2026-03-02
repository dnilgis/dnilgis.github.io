/**
 * AGSIST geo.js — Shared JS: weather, prices, ticker, geo, forecast
 * ─────────────────────────────────────────────────────────────────
 * Price sources (all free, no API keys, no trials):
 *   1. data/prices.json  — pre-fetched every 30min by GitHub Actions (yfinance)
 *   2. CoinGecko          — crypto prices (proxied via corsproxy.io)
 *   3. Farmers First API  — FFAI Index
 *   4. Open-Meteo         — weather
 *   5. Nominatim OSM      — reverse geocoding
 *
 * AUDIT v5 — 2026-03-01
 *   FIX 1: calcUrea() — added temperature gate (frozen ground was showing "Moderate")
 *   FIX 2: uT() — returns 0 for <32°F, 3 for 32-39°F (was 5 for all <40)
 *   FIX 3: Spray badge — distinct "Frozen" message below 32°F
 *   FIX 4: calcUrea returns 'frozen' level for homepage display
 *   NEW 5: Prediction markets v2 — categories, relevance tiers, "why it matters"
 */

// ─────────────────────────────────────────────────────────────────
// WEATHER CONSTANTS
// ─────────────────────────────────────────────────────────────────
var WX_CODES = {
  0:'Clear Sky',1:'Mainly Clear',2:'Partly Cloudy',3:'Overcast',
  45:'Foggy',48:'Icy Fog',51:'Light Drizzle',53:'Drizzle',55:'Heavy Drizzle',
  61:'Light Rain',63:'Rain',65:'Heavy Rain',71:'Light Snow',73:'Snow',
  75:'Heavy Snow',80:'Rain Showers',81:'Showers',82:'Heavy Showers',
  95:'Thunderstorm',96:'T-Storm w/Hail',99:'Severe T-Storm'
};
var WX_ICONS = {
  0:'☀️',1:'🌤️',2:'⛅',3:'☁️',45:'🌫️',48:'🌫️',51:'🌦️',53:'🌧️',55:'🌧️',
  61:'🌦️',63:'🌧️',65:'⛈️',71:'🌨️',73:'❄️',75:'❄️',80:'🌦️',81:'🌧️',
  82:'⛈️',95:'⛈️',96:'⛈️',99:'⛈️'
};

// ─────────────────────────────────────────────────────────────────
// GEOLOCATION + WEATHER
// ─────────────────────────────────────────────────────────────────
function requestGeo() {
  if (!navigator.geolocation) { showZipEntry(); return; }
  var wl = document.getElementById('wx-loading');
  if (wl) wl.innerHTML = '<div style="font-size:1.5rem;margin-bottom:.5rem">📍</div>'
    + '<div style="font-size:.88rem;color:var(--text-dim)">Detecting location…</div>';
  navigator.geolocation.getCurrentPosition(
    function(pos) { fetchWeather(pos.coords.latitude, pos.coords.longitude, null); },
    function() { showZipEntry(); },
    { timeout: 8000 }
  );
}

function showZipEntry() {
  var wl = document.getElementById('wx-loading');
  var ze = document.getElementById('wx-zip-entry');
  if (wl) wl.style.display = 'none';
  if (ze) ze.style.display = 'block';
}

function loadWeatherZip() {
  var zip = (document.getElementById('wx-zip') || {}).value;
  if (!zip || zip.length !== 5 || isNaN(zip)) return;
  fetch('https://geocoding-api.open-meteo.com/v1/search?name=' + zip + '&count=1&language=en&format=json&countryCode=US')
    .then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.results && d.results.length) {
        var r = d.results[0];
        fetchWeather(r.latitude, r.longitude, r.name + (r.admin1 ? ', ' + r.admin1.substring(0,2) : ''));
      }
    }).catch(function() {});
}

function degToCompass(d) {
  var dirs = ['N','NNE','NE','ENE','E','ESE','SE','SSE','S','SSW','SW','WSW','W','WNW','NW','NNW'];
  return dirs[Math.round(d / 22.5) % 16];
}

// ─────────────────────────────────────────────────────────────────
// UREA VOLATILIZATION RISK — TEMPERATURE-GATED SCORING
// ─────────────────────────────────────────────────────────────────
function calcUrea(tempF, humid, wind, popPct) {
  function uT(f) {
    if (f < 32)  return 0;
    if (f < 40)  return 3;
    if (f < 50)  return 15;
    if (f < 60)  return 30;
    if (f < 70)  return 50;
    if (f < 80)  return 72;
    if (f < 90)  return 88;
    return 98;
  }
  function uH(h){return h<30?25:h<50?45:h<70?75:h<85?60:35;}
  function uW(w){return w<2?15:w<5?35:w<10?60:w<15?78:90;}
  function uR(p){return p>=70?10:p>=50?25:p>=30?55:85;}

  var raw = Math.round(uT(tempF)*0.35 + uH(humid)*0.25 + uW(wind)*0.20 + uR(popPct)*0.20);
  var score;
  if (tempF < 32)      score = 0;
  else if (tempF < 40) score = Math.min(raw, 15);
  else if (tempF < 50) score = Math.min(raw, 38);
  else                 score = raw;

  var level;
  if (tempF < 32)       level = 'frozen';
  else if (score < 25)  level = 'low';
  else if (score < 50)  level = 'moderate';
  else if (score < 72)  level = 'high';
  else                  level = 'extreme';

  return { score: score, level: level, gated: tempF < 50 };
}

// ─────────────────────────────────────────────────────────────────
// SPRAY CONDITIONS RATING
// ─────────────────────────────────────────────────────────────────
function calcSprayRating(tempF, humid, wind) {
  if (wind > 15 || tempF > 90 || tempF < 32 || humid < 30) return 'poor';
  if (tempF < 40) return 'poor';
  if (wind > 10 || tempF < 50 || tempF > 85 || humid < 40 || humid > 90 || wind < 3) return 'caution';
  return 'good';
}

function fetchWeather(lat, lon, label) {
  try { localStorage.setItem('agsist-wx-loc', JSON.stringify({lat:lat, lon:lon, label:label})); } catch(e) {}

  var wl = document.getElementById('wx-loading');
  var ze = document.getElementById('wx-zip-entry');
  var wd = document.getElementById('wx-data');
  if (wl) wl.style.display = 'none';
  if (ze) ze.style.display = 'none';
  if (wd) wd.style.display = 'block';

  var wxLoc = document.getElementById('wx-loc');
  if (wxLoc) wxLoc.textContent = '📍 ' + (label || 'Your Location');

  var frame = document.getElementById('windy-frame');
  if (frame) {
    var la = lat.toFixed(4), lo = lon.toFixed(4);
    frame.src = 'https://embed.windy.com/embed.html?type=map&location=coordinates'
      + '&metricRain=in&metricTemp=%C2%B0F&metricWind=mph'
      + '&zoom=7&overlay=radar&product=radar&level=surface'
      + '&lat='+la+'&lon='+lo+'&detailLat='+la+'&detailLon='+lo
      + '&detail=false&pressure=false&menu=false&message=false&marker=false'
      + '&calendar=now&thunder=false';
  }

  (function() {
    var h = new Date().getHours();
    var g = h<12?'Good Morning':h<17?'Good Afternoon':'Good Evening';
    var el = document.getElementById('site-greeting');
    if (el) el.textContent = g;
  })();

  var url = 'https://api.open-meteo.com/v1/forecast?latitude='+lat+'&longitude='+lon
    + '&current=temperature_2m,relative_humidity_2m,apparent_temperature,precipitation_probability,weather_code,wind_speed_10m,wind_direction_10m,dew_point_2m'
    + '&temperature_unit=fahrenheit&wind_speed_unit=mph&precipitation_unit=inch&timezone=auto&forecast_days=1';

  fetch(url)
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var c = d.current;
      var code   = c.weather_code;
      var tempF  = Math.round(c.temperature_2m);
      var feelsF = Math.round(c.apparent_temperature);
      var wind   = Math.round(c.wind_speed_10m);
      var humid  = c.relative_humidity_2m;
      var precip = c.precipitation_probability;
      var dew    = Math.round(c.dew_point_2m);

      var el;
      el = document.getElementById('wx-temp');  if(el) el.textContent = tempF + '°F';
      el = document.getElementById('wx-icon');  if(el) el.textContent = WX_ICONS[code] || '🌡️';
      el = document.getElementById('wx-desc');  if(el) el.textContent = (WX_CODES[code]||'Current Conditions') + ' · Feels ' + feelsF + '°';
      el = document.getElementById('wx-wind');  if(el) el.textContent = degToCompass(c.wind_direction_10m) + ' ' + wind + ' mph';
      el = document.getElementById('wx-humid'); if(el) el.textContent = humid + '%';
      el = document.getElementById('wx-precip');if(el) el.textContent = precip + '%';
      el = document.getElementById('wx-dew');   if(el) el.textContent = dew + '°F';

      var spray = document.getElementById('wx-spray');
      if (spray) {
        var sprayR = calcSprayRating(tempF, humid, wind);
        var sprayMsg;
        if (sprayR === 'poor') {
          if (tempF < 32)       sprayMsg = '🚫 Do Not Spray — Frozen (' + tempF + '°F) →';
          else if (tempF < 40)  sprayMsg = '🚫 Do Not Spray — Too cold (' + tempF + '°F) →';
          else if (wind > 15)   sprayMsg = '🚫 Poor Spray Conditions — Wind too high (' + wind + ' mph) →';
          else if (tempF > 90)  sprayMsg = '🚫 Poor Spray Conditions — Too hot (' + tempF + '°F) →';
          else                  sprayMsg = '🚫 Poor Spray Conditions — Humidity too low (' + humid + '%) →';
        } else if (sprayR === 'caution') {
          sprayMsg = '⚠️ Marginal Spray Conditions — Review before applying →';
        } else {
          sprayMsg = '✅ Good Spray Conditions →';
        }
        spray.className = 'spray-badge ' + sprayR;
        spray.textContent = sprayMsg;
      }

      var ureaWrap = document.getElementById('wx-urea');
      if (ureaWrap) {
        var u = calcUrea(tempF, humid, wind, precip);
        var uPalette = {frozen:'91,163,224', low:'62,207,110', moderate:'230,176,66', high:'240,145,58', extreme:'240,96,96'};
        var uLabels  = {frozen:'Frozen — N/A', low:'Low', moderate:'Moderate', high:'High', extreme:'Extreme'};
        var uColors  = {frozen:'var(--blue)', low:'var(--green)', moderate:'var(--gold)', high:'#f0913a', extreme:'var(--red)'};
        var sEl = document.getElementById('wx-urea-score');
        var bEl = document.getElementById('wx-urea-badge');
        if (sEl) { sEl.textContent = u.score; sEl.style.color = uColors[u.level]; }
        if (bEl) {
          bEl.textContent = uLabels[u.level];
          bEl.style.color = uColors[u.level];
          bEl.style.background = 'rgba('+uPalette[u.level]+',.12)';
          bEl.style.border = '1px solid rgba('+uPalette[u.level]+',.25)';
        }
        ureaWrap.style.display = 'block';
      }

      updateWidgetPreviews(tempF, humid, wind, precip);
      propagateLocation(lat, lon, label);
    })
    .catch(function() {
      if (wd) wd.style.display = 'none';
      var wl2 = document.getElementById('wx-loading');
      if (wl2) {
        wl2.innerHTML = '';
        var msg = document.createElement('div');
        msg.style.cssText = 'font-size:.88rem;color:var(--text-dim)';
        msg.textContent = 'Weather unavailable. ';
        var btn = document.createElement('button');
        btn.textContent = 'Try ZIP →';
        btn.setAttribute('style','background:none;border:none;color:var(--gold);cursor:pointer;font-size:.88rem;font-family:inherit');
        btn.onclick = showZipEntry;
        msg.appendChild(btn);
        wl2.appendChild(msg);
        wl2.style.display = 'block';
      }
    });

  renderForecast(lat, lon);
}

function propagateLocation(lat, lon, label) {
  fetch('https://nominatim.openstreetmap.org/reverse?lat='+lat+'&lon='+lon+'&format=json')
    .then(function(r) { return r.json(); })
    .then(function(geo) {
      var city = geo.address.city || geo.address.town || geo.address.village || geo.address.county || '';
      var st   = geo.address.state_code || '';
      var zip  = geo.address.postcode || '';
      var name = city + (st ? ', '+st : '');

      var wxLoc = document.getElementById('wx-loc');
      if (wxLoc && name) wxLoc.textContent = '📍 ' + name;

      var radarLbl = document.getElementById('wx-loc-label');
      if (radarLbl && name) radarLbl.textContent = name;

      if (zip) {
        var bidsZip = document.getElementById('bids-zip');
        if (bidsZip && !bidsZip.value) bidsZip.value = zip;
        if (typeof loadCashBids === 'function') loadCashBids(zip);
      } else if (name) {
        var geoTxt = document.getElementById('bids-geo-txt');
        if (geoTxt) geoTxt.textContent = 'Near ' + name + ' — enter ZIP for live bids';
      }

      try {
        var saved = JSON.parse(localStorage.getItem('agsist-wx-loc') || '{}');
        if (name) saved.label = name;
        if (zip)  saved.zip   = zip;
        localStorage.setItem('agsist-wx-loc', JSON.stringify(saved));
      } catch(e) {}
    }).catch(function() {});
}

function updateWidgetPreviews(tempF, humid, wind, pop) {
  var sprayRating = calcSprayRating(tempF, humid, wind);
  var sprayDisplay = sprayRating === 'caution' ? 'marginal' : sprayRating;
  var sprayColors  = {good:'rgba(62,207,110,.08)',marginal:'rgba(230,176,66,.08)',poor:'rgba(240,96,96,.08)'};
  var sprayBorders = {good:'rgba(62,207,110,.2)',marginal:'rgba(230,176,66,.2)',poor:'rgba(240,96,96,.2)'};
  var sprayIcons   = {good:'✅',marginal:'⚠️',poor:'🚫'};
  var sprayLabels  = {good:'Good — Apply Now',marginal:'Use Caution',poor:'Do Not Spray'};
  var sprayEl  = document.getElementById('wsp-spray-icon');
  var statusEl = document.getElementById('wsp-spray-status');
  var detailEl = document.getElementById('wsp-spray-detail');
  var wrapEl   = document.getElementById('wsp-spray');
  if (sprayEl)  sprayEl.textContent = sprayIcons[sprayDisplay];
  if (statusEl) {
    statusEl.textContent = sprayLabels[sprayDisplay];
    statusEl.style.color = {good:'var(--green)',marginal:'var(--gold)',poor:'var(--red)'}[sprayDisplay];
  }
  if (detailEl) detailEl.textContent = 'Wind '+wind+' mph · '+tempF+'°F · Humidity '+humid+'%';
  if (wrapEl) {
    var inner = wrapEl.querySelector('div');
    if (inner) {
      inner.style.background = sprayColors[sprayDisplay];
      inner.style.borderColor = sprayBorders[sprayDisplay];
    }
  }

  var u = calcUrea(tempF, humid, wind, pop);
  var uPalette = {frozen:'91,163,224', low:'62,207,110', moderate:'230,176,66', high:'240,145,58', extreme:'240,96,96'};
  var uLbls    = {frozen:'Frozen — No Risk', low:'Low Risk', moderate:'Moderate Risk', high:'High Risk', extreme:'Extreme Risk'};
  var uRecs    = {frozen:'Ground frozen — urease inactive', low:'Favorable for application', moderate:'Consider NBPT stabilizer', high:'Use stabilizer or wait', extreme:'Do not apply without stabilizer'};
  var uColors  = {frozen:'var(--blue)', low:'var(--green)', moderate:'var(--gold)', high:'#f0913a', extreme:'var(--red)'};
  var uSc = document.getElementById('wsp-urea-score');
  var uBd = document.getElementById('wsp-urea-badge');
  var uRc = document.getElementById('wsp-urea-rec');
  if (uSc) { uSc.textContent = u.score; uSc.style.color = uColors[u.level]; }
  if (uBd) {
    uBd.textContent = uLbls[u.level];
    uBd.style.color = uColors[u.level];
    uBd.style.background = 'rgba('+uPalette[u.level]+',.12)';
    uBd.style.border = '1px solid rgba('+uPalette[u.level]+',.25)';
  }
  if (uRc) uRc.textContent = uRecs[u.level];
}

function renderForecast(lat, lon) {
  var url = 'https://api.open-meteo.com/v1/forecast?latitude='+lat+'&longitude='+lon
    + '&daily=weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max'
    + '&temperature_unit=fahrenheit&timezone=auto&forecast_days=4';
  fetch(url)
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var days = d.daily;
      var dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
      var fc = document.getElementById('wx-forecast');
      var fcFull = document.getElementById('wx-forecast-full');
      var locLabel = document.getElementById('wx-loc-label');
      var wxLocEl  = document.getElementById('wx-loc');
      if (locLabel && wxLocEl) locLabel.textContent = wxLocEl.textContent.replace('📍 ','');

      if (fc) {
        fc.innerHTML = '';
        for (var i = 1; i < 4; i++) {
          var day = new Date(days.time[i] + 'T12:00:00');
          var dname = i===1 ? 'Tomorrow' : dayNames[day.getDay()];
          var el = document.createElement('div');
          el.style.cssText = 'flex:1;background:var(--surface2);border:1px solid var(--border);border-radius:6px;padding:.5rem .4rem;text-align:center';
          var pop = days.precipitation_probability_max[i];
          el.innerHTML = '<div style="font-size:.64rem;font-weight:700;letter-spacing:.07em;text-transform:uppercase;color:var(--text-muted);margin-bottom:.2rem">'+dname+'</div>'
            + '<div style="font-size:1.3rem;line-height:1;margin-bottom:.2rem">'+(WX_ICONS[days.weather_code[i]]||'🌡️')+'</div>'
            + '<div style="font-size:.82rem;font-weight:700;color:var(--text)">'+Math.round(days.temperature_2m_max[i])+'°</div>'
            + '<div style="font-size:.74rem;color:var(--text-muted)">'+Math.round(days.temperature_2m_min[i])+'°</div>'
            + (pop>20?'<div style="font-size:.68rem;color:var(--blue);margin-top:.15rem">💧'+pop+'%</div>':'');
          fc.appendChild(el);
        }
      }

      if (fcFull) {
        fcFull.innerHTML = '';
        for (var j = 0; j < 4; j++) {
          var dj = new Date(days.time[j] + 'T12:00:00');
          var dnj = j===0 ? 'Today' : dayNames[dj.getDay()];
          var elj = document.createElement('div');
          elj.className = 'wx-day';
          var popj = days.precipitation_probability_max[j];
          elj.innerHTML = '<div class="wx-day-name">'+dnj+'</div>'
            + '<div class="wx-day-icon">'+(WX_ICONS[days.weather_code[j]]||'🌡️')+'</div>'
            + '<div class="wx-day-hi">'+Math.round(days.temperature_2m_max[j])+'°</div>'
            + '<div class="wx-day-lo">'+Math.round(days.temperature_2m_min[j])+'°</div>'
            + (popj>15?'<div class="wx-day-pop">💧'+popj+'%</div>':'');
          fcFull.appendChild(elj);
        }
      }
    }).catch(function() {});
}

// ─────────────────────────────────────────────────────────────────
// CASH BIDS PLACEHOLDER
// ─────────────────────────────────────────────────────────────────
function lookupBids() {
  var zip = (document.getElementById('bids-zip') || {}).value;
  if (!zip || zip.length !== 5 || isNaN(zip)) return;
  loadCashBids(zip);
}

function loadCashBids(zip) {
  var geoBar   = document.getElementById('bids-geo-bar');
  var zipRow   = document.getElementById('bids-zip-row');
  var listArea = document.getElementById('bids-list-area');
  if (geoBar) geoBar.style.display = 'none';
  if (zipRow) zipRow.style.display = 'none';
  var bz = document.getElementById('bids-zip');
  if (bz && !bz.value) bz.value = zip;
  if (!listArea) return;
  listArea.innerHTML = renderBidsPlaceholder(zip);
}

function renderBidsPlaceholder(zip) {
  return '<div style="text-align:center;padding:1.25rem .5rem">'
    + '<div style="font-size:1.5rem;margin-bottom:.4rem">💵</div>'
    + '<div style="font-size:.88rem;font-weight:600;color:var(--text);margin-bottom:.25rem">Cash bids coming soon</div>'
    + '<div style="font-size:.78rem;color:var(--text-muted);line-height:1.5">Live local elevator bids near '
    + (zip || 'your area') + ' will appear here.</div>'
    + '</div>';
}

// ─────────────────────────────────────────────────────────────────
// PRICE CONFIGURATION
// ─────────────────────────────────────────────────────────────────
var PRICE_MAP = {
  'corn':       { label:'Corn',          priceEl:'pcp-corn-near', chgEl:'pcc-corn-near', dec:2, grain:true  },
  'corn-dec':   { label:"Corn Dec'26",   priceEl:'pcp-corn-dec',  chgEl:'pcc-corn-dec',  dec:2, grain:true  },
  'beans':      { label:'Soybeans',      priceEl:'pcp-bean-near', chgEl:'pcc-bean-near', dec:2, grain:true  },
  'beans-nov':  { label:"Beans Nov'26",  priceEl:'pcp-bean-nov',  chgEl:'pcc-bean-nov',  dec:2, grain:true  },
  'wheat':      { label:'Wheat',         priceEl:'pcp-wheat',     chgEl:'pcc-wheat',     dec:2, grain:true  },
  'oats':       { label:'Oats',          priceEl:'pcp-oats',      chgEl:'pcc-oats',      dec:2, grain:true  },
  'cattle':     { label:'Live Cattle',   priceEl:'pcp-cattle',    chgEl:'pcc-cattle',    dec:2, grain:false },
  'feeders':    { label:'Feeder Cattle', priceEl:'pcp-feeders',   chgEl:'pcc-feeders',   dec:2, grain:false },
  'hogs':       { label:'Lean Hogs',     priceEl:'pcp-hogs',      chgEl:'pcc-hogs',      dec:2, grain:false },
  'milk':       { label:'Class III Milk',priceEl:'pcp-milk',      chgEl:'pcc-milk',      dec:2, grain:false },
  'meal':       { label:'Soy Meal',      priceEl:'pcp-meal',      chgEl:'pcc-meal',      dec:2, grain:false },
  'soyoil':     { label:'Soy Oil',       priceEl:'pcp-soyoil',    chgEl:'pcc-soyoil',    dec:2, grain:false },
  'crude':      { label:'Crude Oil',     priceEl:'pcp-crude',     chgEl:'pcc-crude',     dec:2, grain:false },
  'natgas':     { label:'Natural Gas',   priceEl:'pcp-natgas',    chgEl:'pcc-natgas',    dec:2, grain:false },
  'gold':       { label:'Gold',          priceEl:'pcp-gold',      chgEl:'pcc-gold',      dec:2, grain:false },
  'silver':     { label:'Silver',        priceEl:'pcp-silver',    chgEl:'pcc-silver',    dec:2, grain:false },
  'dollar':     { label:'Dollar Index',  priceEl:'pcp-dollar',    chgEl:'pcc-dollar',    dec:2, grain:false },
  'treasury10': { label:'10-Yr Treasury',priceEl:'pcp-treasury',  chgEl:'pcc-treasury',  dec:2, grain:false, suffix:'%' },
  'sp500':      { label:'S&P 500',       priceEl:'pcp-sp500',     chgEl:'pcc-sp500',     dec:2, grain:false },
};

// ─────────────────────────────────────────────────────────────────
// PRICE FORMATTING
// ─────────────────────────────────────────────────────────────────
function fmtPrice(val, dec, grain, suffix) {
  var p = parseFloat(val);
  if (isNaN(p)) return '--';
  if (grain) { return '$' + (p / 100).toFixed(2); }
  var str = p.toFixed(dec);
  if (suffix) return str + suffix;
  return str;
}

function fmtChange(close, open, grain, netChg, pctChg) {
  var c = parseFloat(close), o = parseFloat(open);
  if (isNaN(c) || isNaN(o)) return {text:'--', cls:'nc'};
  var diff  = netChg !== undefined && netChg !== null ? parseFloat(netChg) : (c - o);
  var pct   = pctChg !== undefined && pctChg !== null ? parseFloat(pctChg) : (o !== 0 ? (diff/o)*100 : 0);
  var dir   = diff > 0 ? 'up' : diff < 0 ? 'dn' : 'nc';
  var arrow = diff > 0 ? '▲' : diff < 0 ? '▼' : '—';
  var sign  = diff > 0 ? '+' : '';
  return {text: arrow + ' ' + sign + pct.toFixed(1) + '%', cls: dir};
}

function fmtTickerChange(close, open, grain, netChg, pctChg) {
  var c = parseFloat(close), o = parseFloat(open);
  if (isNaN(c) || isNaN(o)) return {text:'--', cls:'nc'};
  var diff  = netChg !== undefined && netChg !== null ? parseFloat(netChg) : (c - o);
  var pct   = pctChg !== undefined && pctChg !== null ? parseFloat(pctChg) : (o !== 0 ? (diff/o)*100 : 0);
  var dir   = diff > 0 ? 'up' : diff < 0 ? 'dn' : 'nc';
  var arrow = diff > 0 ? '▲' : diff < 0 ? '▼' : '';
  return {text: arrow + ' ' + Math.abs(pct).toFixed(2) + '%', cls: dir};
}

function fmtTickerPrice(val, grain) {
  var p = parseFloat(val);
  if (isNaN(p)) return '--';
  if (grain) return '$' + (p / 100).toFixed(2);
  return p.toFixed(2);
}

function updatePriceEl(id, txt, cls) {
  var el = document.getElementById(id);
  if (!el) return;
  el.textContent = txt;
  if (cls) el.className = el.className.replace(/\b(up|dn|nc)\b/g,'').trim() + ' ' + cls;
}

function update52WeekRange(priceElId, price, wk52Lo, wk52Hi, isGrain) {
  if (!priceElId) return;
  var priceEl = document.getElementById(priceElId);
  if (!priceEl) return;
  var card = priceEl.closest ? priceEl.closest('.pc') : null;
  if (!card) return;
  var fill   = card.querySelector('.pc-range-fill');
  var dot    = card.querySelector('.pc-range-dot');
  var labels = card.querySelectorAll('.pc-range-labels span');
  if (!fill || labels.length < 3) return;
  var lo = parseFloat(wk52Lo), hi = parseFloat(wk52Hi);
  if (isNaN(lo) || isNaN(hi) || hi <= lo) return;
  if (isGrain) {
    labels[0].textContent = '$' + (lo / 100).toFixed(2);
    labels[2].textContent = '$' + (hi / 100).toFixed(2);
  } else if (hi >= 10000) {
    labels[0].textContent = Math.round(lo).toLocaleString('en-US');
    labels[2].textContent = Math.round(hi).toLocaleString('en-US');
  } else if (hi >= 100) {
    labels[0].textContent = lo.toFixed(2);
    labels[2].textContent = hi.toFixed(2);
  } else {
    labels[0].textContent = lo < 1 ? lo.toFixed(4) : lo.toFixed(2);
    labels[2].textContent = hi < 1 ? hi.toFixed(4) : hi.toFixed(2);
  }
  var pct = Math.min(100, Math.max(0, ((price - lo) / (hi - lo)) * 100));
  fill.style.width = pct + '%';
  if (dot) dot.style.left = pct + '%';
}

function applyPriceResult(key, q, close, open, netChg, pctChg) {
  var meta = PRICE_MAP[key];
  if (!meta) return;
  var priceTxt = fmtPrice(close, meta.dec, meta.grain, meta.suffix);
  var chgObj   = fmtChange(close, open, meta.grain, netChg, pctChg);
  if (meta.priceEl) updatePriceEl(meta.priceEl, priceTxt);
  if (meta.chgEl)   updatePriceEl(meta.chgEl, chgObj.text, chgObj.cls);
  var prevEl = document.getElementById(meta.priceEl ? meta.priceEl.replace('pcp-','pcprev-') : '');
  if (prevEl && open != null) {
    var prevTxt = meta.grain ? '$' + (parseFloat(open)/100).toFixed(2) : parseFloat(open).toFixed(meta.dec) + (meta.suffix||'');
    prevEl.textContent = 'prev: ' + prevTxt + ' ' + (q && q.ticker ? q.ticker : '');
  }
  if (meta.priceEl && q) {
    var wk52Hi = q.wk52_hi, wk52Lo = q.wk52_lo;
    if (wk52Hi != null && wk52Lo != null) {
      update52WeekRange(meta.priceEl, parseFloat(close), wk52Lo, wk52Hi, meta.grain);
    }
  }
  var tickerPriceTxt = fmtTickerPrice(close, meta.grain);
  var tickerChgObj = fmtTickerChange(close, open, meta.grain, netChg, pctChg);
  document.querySelectorAll('[data-sym="' + key + '"]').forEach(function(el) {
    var pe = el.querySelector('.t-price');
    var ce = el.querySelector('.t-chg');
    if (pe) pe.textContent = tickerPriceTxt;
    if (ce) { ce.textContent = tickerChgObj.text; ce.className = 't-chg ' + tickerChgObj.cls; }
  });
  rebuildTickerLoop();
}

// ─────────────────────────────────────────────────────────────────
// PRIMARY PRICE SOURCE — data/prices.json
// ─────────────────────────────────────────────────────────────────
function fetchAllPrices() {
  fetch('/data/prices.json', { cache: 'no-store' })
    .then(function(r) {
      if (!r.ok) throw new Error('prices.json ' + r.status);
      return r.json();
    })
    .then(function(data) {
      var quotes = data.quotes || {};
      Object.keys(quotes).forEach(function(key) {
        var q = quotes[key];
        if (!q || q.close === null || q.close === undefined) return;
        applyPriceResult(key, q, q.close, q.open, q.netChange, q.pctChange);
      });
    })
    .catch(function(e) { console.warn('prices.json fetch failed:', e); });
}

// ─────────────────────────────────────────────────────────────────
// CRYPTO — CoinGecko via CORS proxy
// ─────────────────────────────────────────────────────────────────
function fetchCryptoLive() {
  var cgUrl = 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ripple,kaspa&vs_currencies=usd&include_24hr_change=true&precision=4';
  var proxied = 'https://corsproxy.io/?' + encodeURIComponent(cgUrl);
  fetch(proxied)
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var cmap = {
        bitcoin: { priceEl:'pc-btc',  chgEl:'pcc-btc',  tickerSym:'bitcoin', dec:2 },
        ripple:  { priceEl:'pc-xrp',  chgEl:'pcc-xrp',  tickerSym:'ripple',  dec:2 },
        kaspa:   { priceEl:'pc-kas',  chgEl:'pcc-kas',  tickerSym:'kaspa',   dec:2 },
      };
      Object.keys(cmap).forEach(function(id) {
        var info  = cmap[id];
        var price = d[id] && d[id].usd;
        var chgP  = d[id] && d[id].usd_24h_change;
        if (!price) return;
        var priceTxt = price < 1 ? price.toFixed(4) : price.toLocaleString('en-US',{maximumFractionDigits:2});
        updatePriceEl(info.priceEl, priceTxt);
        if (chgP !== undefined) {
          var cv = parseFloat(chgP);
          updatePriceEl(info.chgEl, (cv>0?'▲':'▼')+' '+Math.abs(cv).toFixed(2)+'%', cv>0?'up':'dn');
        }
        document.querySelectorAll('[data-sym="'+info.tickerSym+'"]').forEach(function(el) {
          var pe = el.querySelector('.t-price');
          var ce = el.querySelector('.t-chg');
          if (pe) pe.textContent = priceTxt;
          if (ce && chgP !== undefined) {
            var cv2 = parseFloat(chgP);
            ce.textContent = (cv2>0?'▲':'▼')+' '+Math.abs(cv2).toFixed(2)+'%';
            ce.className = 't-chg '+(cv2>0?'up':'dn');
          }
        });
      });
      rebuildTickerLoop();
    }).catch(function(e) { console.warn('CoinGecko fetch failed:', e); });
}

// ─────────────────────────────────────────────────────────────────
// FFAI INDEX — Farmers First Agri Service
// ─────────────────────────────────────────────────────────────────
function fetchFFAILive() {
  fetch('https://farmers1st.com/api/v3/current.json')
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var score = d.composite;
      var prev  = d.previous ? d.previous.composite : null;
      var diff  = prev !== null ? parseFloat((score - prev).toFixed(1)) : null;
      var dir   = diff && diff > 0 ? 'up' : 'dn';
      var sign  = diff && diff > 0 ? '▲' : '▼';
      var priceTxt = score.toFixed(1);
      var chgTxt   = diff ? sign + ' ' + Math.abs(diff) + ' pts' : '--';

      document.querySelectorAll('[data-sym="ffai"]').forEach(function(el) {
        var pe = el.querySelector('.t-price');
        var ce = el.querySelector('.t-chg');
        if (pe) { pe.textContent = priceTxt; pe.style.color = 'var(--blue)'; }
        if (ce && diff) { ce.className = 't-chg ' + dir; ce.textContent = chgTxt; }
      });

      var compactEl = document.getElementById('ffai-score-compact');
      if (compactEl) compactEl.textContent = priceTxt;
    }).catch(function() {});
}

// ─────────────────────────────────────────────────────────────────
// TICKER
// ─────────────────────────────────────────────────────────────────
var _tickerRebuildTimer = null;
function rebuildTickerLoop() {
  if (_tickerRebuildTimer) clearTimeout(_tickerRebuildTimer);
  _tickerRebuildTimer = setTimeout(function() {
    var single = document.getElementById('ticker-items-single');
    var track  = document.getElementById('ticker-track');
    if (!single || !track) return;
    var old = document.getElementById('ticker-items-clone');
    if (old) old.remove();
    var c = single.cloneNode(true);
    c.id = 'ticker-items-clone';
    c.setAttribute('aria-hidden', 'true');
    track.appendChild(c);
    track.style.animation = 'none';
    track.offsetHeight;
    track.style.animation = '';
    var w = single.scrollWidth || single.offsetWidth;
    if (w > 200) {
      track.style.animationDuration = Math.max(20, Math.round(w / 20)) + 's';
    }
  }, 120);
}

// ─────────────────────────────────────────────────────────────────
// PREDICTION MARKETS v2 — Categories, Relevance Tiers, Why It Matters
// ─────────────────────────────────────────────────────────────────

// Category display: icon + sort order for grouped rendering
var MARKET_CATEGORIES = {
  'Commodities':       { icon: '🌽', order: 1 },
  'Trade & Policy':    { icon: '🏛️', order: 2 },
  'Energy & Inputs':   { icon: '⛽', order: 3 },
  'Weather & Climate': { icon: '🌦️', order: 4 },
  'Economy & Markets': { icon: '📊', order: 5 },
  'Infrastructure':    { icon: '🚂', order: 6 },
  'Other':             { icon: '🎯', order: 7 }
};

// Relevance tier badges: color-coded by how directly agricultural
var RELEVANCE_TIERS = {
  100: { label: 'Direct Ag',      color: 'var(--green)', bg: 'rgba(62,207,110,.10)',  border: 'rgba(62,207,110,.25)' },
  70:  { label: 'Trade & Energy', color: 'var(--gold)',  bg: 'rgba(230,176,66,.10)',  border: 'rgba(230,176,66,.25)' },
  40:  { label: 'Macro Impact',   color: 'var(--blue)',  bg: 'rgba(91,163,224,.10)',  border: 'rgba(91,163,224,.25)' }
};

function getRelevanceTier(score) {
  if (score >= 100) return RELEVANCE_TIERS[100];
  if (score >= 70)  return RELEVANCE_TIERS[70];
  return RELEVANCE_TIERS[40];
}

function fetchKalshiMarkets() {
  var container = document.getElementById('kalshi-grid');
  var loading   = document.getElementById('kalshi-loading');
  if (!container) return;

  fetch('/data/markets.json', { cache: 'no-store' })
    .then(function(r) {
      if (!r.ok) throw new Error('markets.json ' + r.status);
      return r.json();
    })
    .then(function(data) {
      if (loading) loading.style.display = 'none';
      container.innerHTML = '';

      var markets = data.markets || [];
      var isV2    = data.version >= 2;

      // ── Empty state ──────────────────────────────────────────
      if (!markets.length) {
        container.innerHTML = '<div style="grid-column:1/-1;text-align:center;padding:2rem 1rem">'
          + '<div style="font-size:2rem;margin-bottom:.5rem;opacity:.6">🎯</div>'
          + '<div style="font-size:.88rem;font-weight:600;color:var(--text);margin-bottom:.35rem">No active prediction markets right now</div>'
          + '<div style="font-size:.78rem;color:var(--text-muted);line-height:1.5;max-width:32rem;margin:0 auto">'
          + 'We scan Kalshi and Polymarket every 2 hours for events that affect agriculture — tariffs, weather, trade, energy, USDA reports, and more.</div>'
          + '<div style="margin-top:.75rem;font-size:.78rem">'
          + '<a href="https://kalshi.com/markets" target="_blank" rel="noopener" style="color:var(--gold)">Browse Kalshi →</a>'
          + '<span style="color:var(--text-muted);margin:0 .5rem">·</span>'
          + '<a href="https://polymarket.com" target="_blank" rel="noopener" style="color:var(--gold)">Browse Polymarket →</a></div></div>';
        return;
      }

      // ── v2: Group by category, render with headers ───────────
      if (isV2 && data.categories) {
        var catKeys = Object.keys(data.categories).sort(function(a, b) {
          return ((MARKET_CATEGORIES[a] || {}).order || 99) - ((MARKET_CATEGORIES[b] || {}).order || 99);
        });

        catKeys.forEach(function(catName) {
          var catMarkets = data.categories[catName];
          if (!catMarkets || !catMarkets.length) return;
          var catMeta = MARKET_CATEGORIES[catName] || { icon: '🎯', order: 99 };

          // Category header row
          var header = document.createElement('div');
          header.style.cssText = 'grid-column:1/-1;display:flex;align-items:center;gap:.45rem;'
            + 'padding:.65rem 0 .2rem;border-bottom:1px solid var(--border);margin-bottom:.2rem';
          header.innerHTML = '<span style="font-size:.9rem">' + catMeta.icon + '</span>'
            + '<span style="font-size:.72rem;font-weight:700;letter-spacing:.08em;'
            + 'text-transform:uppercase;color:var(--text-muted)">' + catName + '</span>'
            + '<span style="font-size:.6rem;color:var(--text-muted);margin-left:auto">'
            + catMarkets.length + ' market' + (catMarkets.length !== 1 ? 's' : '') + '</span>';
          container.appendChild(header);

          // Cards in this category
          catMarkets.forEach(function(m) {
            container.appendChild(buildMarketCard(m, true));
          });
        });
      } else {
        // ── v1 fallback: flat list (backward compatible) ──────
        markets.forEach(function(m) {
          container.appendChild(buildMarketCard(m, false));
        });
      }

      // ── Stats + timestamp footer ─────────────────────────────
      var footerParts = [];
      if (data.total_found) footerParts.push(data.total_found + ' markets scanned');
      if (data.tier_breakdown) {
        var tb = data.tier_breakdown;
        var bits = [];
        if (tb.direct_ag)     bits.push(tb.direct_ag + ' direct ag');
        if (tb.trade_energy)  bits.push(tb.trade_energy + ' trade/energy');
        if (tb.macro_weather) bits.push(tb.macro_weather + ' macro/weather');
        if (bits.length) footerParts.push(bits.join(', '));
      }
      if (data.fetched) {
        var mins = Math.round((Date.now() - new Date(data.fetched).getTime()) / 60000);
        var ageStr = mins < 2 ? 'Just updated' : mins < 60 ? mins + 'min ago' : Math.round(mins / 60) + 'h ago';
        footerParts.push(ageStr);
      }

      var footer = document.createElement('div');
      footer.style.cssText = 'grid-column:1/-1;font-size:.62rem;color:var(--text-muted);'
        + 'text-align:center;padding:.5rem 0 .15rem;border-top:1px solid var(--border);'
        + 'margin-top:.25rem;line-height:1.6';
      footer.innerHTML = 'Odds from Kalshi &amp; Polymarket · ' + footerParts.join(' · ')
        + '<br>Markets shown based on agricultural relevance scoring · Not investment advice';
      container.appendChild(footer);
    })
    .catch(function() {
      if (loading) {
        loading.innerHTML = '<div style="font-size:1.3rem;margin-bottom:.4rem;opacity:.6">🎯</div>'
          + 'Market data updating shortly. '
          + '<a href="https://kalshi.com/markets" target="_blank" rel="noopener" style="color:var(--gold)">Kalshi →</a> · '
          + '<a href="https://polymarket.com" target="_blank" rel="noopener" style="color:var(--gold)">Polymarket →</a>';
      }
    });
}

function buildMarketCard(m, showExtras) {
  var yes   = m.yes || 50;
  var title = (m.title || '').length > 100 ? m.title.slice(0, 97) + '…' : (m.title || 'Market');

  // Probability-based color theming
  var color, bgAlpha, borderC;
  if (yes >= 65)      { color = 'var(--green)'; bgAlpha = 'rgba(62,207,110,.05)';  borderC = 'rgba(62,207,110,.18)'; }
  else if (yes <= 35) { color = 'var(--red)';   bgAlpha = 'rgba(240,96,96,.05)';   borderC = 'rgba(240,96,96,.18)';  }
  else                { color = 'var(--gold)';  bgAlpha = 'rgba(230,176,66,.05)';  borderC = 'rgba(230,176,66,.18)'; }

  var platColor = m.platform === 'Kalshi' ? '#00b2ff' : '#9b59b6';
  var platLabel = m.platform || 'Market';

  var vol = m.volume_24h || 0;
  var volStr = vol >= 1e6  ? '$' + (vol / 1e6).toFixed(1) + 'M vol'
             : vol >= 1e3  ? '$' + (vol / 1e3).toFixed(0) + 'k vol'
             : vol > 0     ? '$' + Math.round(vol) + ' vol' : '';

  // Relevance tier badge (v2 only)
  var tierHTML = '';
  if (showExtras && m.relevance) {
    var tier = getRelevanceTier(m.relevance);
    tierHTML = '<span style="font-size:.55rem;font-weight:700;letter-spacing:.05em;'
      + 'text-transform:uppercase;color:' + tier.color + ';background:' + tier.bg
      + ';border:1px solid ' + tier.border + ';border-radius:3px;padding:.08rem .3rem;'
      + 'white-space:nowrap">' + tier.label + '</span>';
  }

  // "Why it matters" blurb (v2 only)
  var whyHTML = '';
  if (showExtras && m.why_it_matters) {
    var whyText = m.why_it_matters.length > 150 ? m.why_it_matters.slice(0, 147) + '…' : m.why_it_matters;
    whyHTML = '<div style="font-size:.7rem;line-height:1.55;color:var(--text-dim);'
      + 'padding:.45rem .55rem;background:var(--surface2);border-radius:6px;'
      + 'border:1px solid var(--border);margin-top:.1rem">'
      + '<span style="font-weight:700;color:var(--text-muted);font-size:.58rem;'
      + 'letter-spacing:.06em;text-transform:uppercase;display:block;margin-bottom:.2rem">'
      + 'Why it matters to farmers</span>' + whyText + '</div>';
  }

  var div = document.createElement('div');
  div.className = 'market-card';
  div.style.cssText = 'background:' + bgAlpha + ';border:1px solid ' + borderC
    + ';border-radius:10px;padding:.8rem;display:flex;flex-direction:column;'
    + 'gap:.4rem;cursor:pointer;transition:border-color .15s,transform .1s';

  div.onmouseenter = function() { div.style.borderColor = color; div.style.transform = 'translateY(-1px)'; };
  div.onmouseleave = function() { div.style.borderColor = borderC; div.style.transform = 'none'; };
  div.onclick = function() { window.open(m.url, '_blank', 'noopener'); };

  div.innerHTML =
    // Row 1: Platform badge + relevance tier + time remaining
    '<div style="display:flex;align-items:center;gap:.35rem;flex-wrap:wrap">'
      + '<span style="font-size:.57rem;font-weight:700;letter-spacing:.08em;color:' + platColor
        + ';text-transform:uppercase;background:' + platColor + '12;border:1px solid '
        + platColor + '30;border-radius:4px;padding:.08rem .35rem">' + platLabel + '</span>'
      + tierHTML
      + '<span style="font-size:.6rem;color:var(--text-muted);margin-left:auto;white-space:nowrap">'
        + (m.time_left || '') + '</span>'
    + '</div>'

    // Row 2: Title
    + '<div style="font-size:.78rem;font-weight:600;color:var(--text);line-height:1.4">' + title + '</div>'

    // Row 3: Probability + bar
    + '<div>'
      + '<div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:.25rem">'
        + '<span style="font-size:1.5rem;font-weight:700;color:' + color
          + ';font-family:\'Oswald\',sans-serif;line-height:1">' + yes + '%</span>'
        + '<span style="font-size:.68rem;color:var(--text-muted)">YES probability</span>'
      + '</div>'
      + '<div style="height:5px;background:var(--border);border-radius:3px;overflow:hidden">'
        + '<div style="height:100%;width:' + yes + '%;background:' + color
          + ';border-radius:3px;transition:width .4s ease"></div>'
      + '</div>'
    + '</div>'

    // Row 4: Why it matters (v2 only — the key new feature)
    + whyHTML

    // Row 5: Footer — NO/volume/link
    + '<div style="display:flex;justify-content:space-between;align-items:center;'
      + 'padding-top:.35rem;border-top:1px solid var(--border);margin-top:.1rem">'
      + '<span style="font-size:.66rem;color:var(--text-muted)">NO: '
        + (m.no || (100 - yes)) + '%' + (volStr ? ' · ' + volStr : '') + '</span>'
      + '<a href="' + m.url + '" target="_blank" rel="noopener" onclick="event.stopPropagation()" '
        + 'style="font-size:.64rem;color:' + color + ';text-decoration:none;font-weight:600;'
        + 'border:1px solid currentColor;border-radius:4px;padding:.12rem .4rem;'
        + 'white-space:nowrap;transition:opacity .15s"'
        + ' onmouseenter="this.style.opacity=\'.8\'" onmouseleave="this.style.opacity=\'1\'">'
        + 'View Market →</a>'
    + '</div>';

  return div;
}

// ─────────────────────────────────────────────────────────────────
// DAILY BRIEFING — v2 schema
// ─────────────────────────────────────────────────────────────────
function loadDailyBriefing() {
  fetch('/data/daily.json', { cache: 'no-store' })
    .then(function(r) {
      if (!r.ok) throw new Error('daily.json ' + r.status);
      return r.json();
    })
    .then(function(d) {
      // v3 layout: delegate to inline hydrateDaily() if present
      if (typeof window.hydrateDaily === 'function') {
        window.hydrateDaily(d);
        return;
      }
      var el;
      el = document.getElementById('daily-headline');    if (el && d.headline)    el.textContent = d.headline;
      el = document.getElementById('daily-subheadline'); if (el && d.subheadline) el.textContent = d.subheadline;
      el = document.getElementById('daily-lead');        if (el && d.lead)        el.textContent = d.lead;
      el = document.getElementById('daily-date');        if (el && d.date)        el.textContent = d.date;
      el = document.getElementById('daily-teaser-text'); if (el && d.teaser)      el.textContent = d.teaser;
      el = document.getElementById('daily-teaser-date'); if (el && d.date)        el.textContent = '\u{1F4F0} AGSIST Daily \u00b7 ' + d.date;

      if (d.one_number) {
        el = document.getElementById('daily-number-value');   if (el) el.textContent = d.one_number.value;
        el = document.getElementById('daily-number-unit');    if (el) el.textContent = d.one_number.unit;
        el = document.getElementById('daily-number-context'); if (el) el.textContent = d.one_number.context;
      }

      if (d.sections && Array.isArray(d.sections)) {
        d.sections.forEach(function(sec, i) {
          el = document.getElementById('daily-section-' + (i+1) + '-title'); if (el && sec.title) el.textContent = sec.title;
          el = document.getElementById('daily-section-' + (i+1) + '-body');  if (el && sec.body)  el.textContent = sec.body;
        });
      }

      if (d.the_more_you_know) {
        el = document.getElementById('daily-tmyk-title'); if (el && d.the_more_you_know.title) el.textContent = d.the_more_you_know.title;
        el = document.getElementById('daily-tmyk-body');  if (el && d.the_more_you_know.body)  el.textContent = d.the_more_you_know.body;
      }

      if (d.daily_quote) {
        el = document.getElementById('daily-quote-text');
        if (el && d.daily_quote.text) {
          var qt = d.daily_quote.text.replace(/^[\u201c""]|[\u201d""]$/g, '');
          el.textContent = '\u201c' + qt + '\u201d';
        }
        el = document.getElementById('daily-quote-attr');
        if (el && d.daily_quote.attribution) {
          var attr = d.daily_quote.attribution.replace(/^[\u2014\u2013-]\s*/, '');
          el.textContent = '\u2014 ' + attr;
        }
      }

      var wl = document.getElementById('daily-watch-list');
      if (wl && d.watch_list && d.watch_list.length) {
        wl.innerHTML = '';
        d.watch_list.forEach(function(item) {
          var li = document.createElement('li');
          li.className = 'watch-item';
          li.innerHTML = '<span class="watch-time">' + (item.time||'') + '</span><span class="watch-desc">' + (item.desc||'') + '</span>';
          wl.appendChild(li);
        });
      }

      el = document.getElementById('daily-source');  if (el) el.textContent = d.source_summary || d.source || 'USDA \u00b7 Yahoo Finance \u00b7 Open-Meteo';
      el = document.getElementById('daily-loading'); if (el) el.style.display = 'none';
      el = document.getElementById('daily-content'); if (el) el.style.display = 'block';
    })
    .catch(function() {
      var loading = document.getElementById('daily-loading');
      var content = document.getElementById('daily-content');
      if (loading) loading.style.display = 'none';
      if (content) content.style.display = 'block';
    });
}

// ─────────────────────────────────────────────────────────────────
// BOOT
// ─────────────────────────────────────────────────────────────────
(function boot() {
  function init() {
    rebuildTickerLoop();
    fetchAllPrices();
    fetchCryptoLive();
    fetchFFAILive();
    if (document.getElementById('dv3-headline') || document.getElementById('daily-headline')) loadDailyBriefing();
    if (document.getElementById('kalshi-grid'))    fetchKalshiMarkets();
    setInterval(function() {
      fetchAllPrices();
      fetchCryptoLive();
      fetchFFAILive();
    }, 5 * 60 * 1000);
    setTimeout(function() {
      try {
        var saved = localStorage.getItem('agsist-wx-loc');
        if (saved) {
          var p = JSON.parse(saved);
          if (p.lat && p.lon) { fetchWeather(p.lat, p.lon, p.label); return; }
        }
      } catch(e) {}
      requestGeo();
    }, 400);
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
