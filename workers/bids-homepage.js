// ═══════════════════════════════════════════════════════════════════
// bids-homepage.js — Homepage Cash Bids card wiring
// Reads /data/bids.json (pre-fetched national grid) and shows
// the top 5 nearest bids in the homepage Cash Bids card.
//
// DEPLOY: Save as /components/bids-homepage.js
//         Add <script src="/components/bids-homepage.js"></script>
//         after geo.js in index.html
//
// OR: Call window.loadHomepageBids(lat, lng, label) from geo.js
// ═══════════════════════════════════════════════════════════════════

(function(){
  'use strict';

  function haversine(lat1,lng1,lat2,lng2){
    var R=3958.8,d2r=Math.PI/180;
    var dLat=(lat2-lat1)*d2r,dLng=(lng2-lng1)*d2r;
    var a=Math.sin(dLat/2)*Math.sin(dLat/2)+
          Math.cos(lat1*d2r)*Math.cos(lat2*d2r)*
          Math.sin(dLng/2)*Math.sin(dLng/2);
    return R*2*Math.asin(Math.sqrt(a));
  }

  function loadHomepageBids(lat, lng, label){
    var area = document.getElementById('bids-list-area');
    var geoTxt = document.getElementById('bids-geo-txt');
    if(!area) return;

    fetch('/data/bids.json?v=' + Date.now())
      .then(function(r){ return r.ok ? r.json() : Promise.reject(r.status); })
      .then(function(data){
        var bids = data.bids || [];
        var grid = data.zip_grid || [];

        var nearZips = grid
          .map(function(z){ return {zip:z.zip, dist:haversine(lat,lng,z.lat,z.lng)}; })
          .sort(function(a,b){ return a.dist-b.dist; })
          .slice(0, 4);
        var zipSet = {};
        nearZips.forEach(function(z){ zipSet[z.zip]=true; });

        var nearby = bids
          .filter(function(b){ return zipSet[b.sourceZip]; })
          .map(function(b){
            b._d = (b.lat!=null && b.lng!=null) ? haversine(lat,lng,b.lat,b.lng) : (b.distance||999);
            return b;
          })
          .filter(function(b){ return b._d <= 75; })
          .sort(function(a,b){ return a._d - b._d; });

        var seen = {};
        var unique = [];
        nearby.forEach(function(b){
          var key = b.facility + '|' + b.commodity;
          if(!seen[key]){ seen[key]=true; unique.push(b); }
        });

        var top = unique.slice(0, 5);

        if(top.length === 0){
          area.innerHTML = '<div style="text-align:center;padding:1rem;font-size:.82rem;color:var(--text-muted)">'
            + '<div style="font-size:1.2rem;margin-bottom:.3rem">📍</div>'
            + 'No elevator bids found nearby.<br><a href="/cash-bids" style="color:var(--gold)">Search any ZIP →</a>'
            + '</div>';
          return;
        }

        if(geoTxt && label) geoTxt.textContent = '📍 Bids near ' + label;

        var html = '';
        top.forEach(function(b){
          var ci = (b.category==='corn')?'🌽':(b.category==='soybeans')?'🫘':(b.category==='wheat')?'🌾':'🌱';
          var cashStr = b.cashPrice != null ? '$' + b.cashPrice.toFixed(2) : '—';
          var bN = b.basis, bStr = '—', bColor = 'var(--text-muted)';
          if(bN != null){
            var cents = Math.abs(bN) > 5 ? bN : bN * 100;
            bStr = (cents >= 0 ? '+' : '') + cents.toFixed(0) + '¢';
            bColor = cents > 0 ? 'var(--green)' : cents < 0 ? 'var(--red,#ef4444)' : 'var(--text-muted)';
          }
          var distStr = b._d != null ? b._d.toFixed(0) + ' mi' : '';

          html += '<div style="display:flex;align-items:center;gap:.65rem;padding:.55rem 0;border-bottom:1px solid var(--border)">'
            + '<span style="font-size:1rem;flex-shrink:0">' + ci + '</span>'
            + '<div style="flex:1;min-width:0">'
              + '<div style="font-size:.8rem;font-weight:600;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis">' + (b.facility||'Unknown') + '</div>'
              + '<div style="font-size:.68rem;color:var(--text-muted)">' + (b.commodity||'') + (distStr ? ' · ' + distStr : '') + '</div>'
            + '</div>'
            + '<div style="text-align:right;flex-shrink:0">'
              + '<div style="font-family:\'JetBrains Mono\',monospace;font-size:.88rem;font-weight:700;color:var(--text)">' + cashStr + '</div>'
              + '<div style="font-family:\'JetBrains Mono\',monospace;font-size:.72rem;font-weight:600;color:' + bColor + '">' + bStr + '</div>'
            + '</div>'
          + '</div>';
        });

        html += '<a href="/cash-bids" style="display:block;text-align:center;padding:.65rem 0;font-size:.78rem;color:var(--gold);font-weight:600;text-decoration:none;margin-top:.3rem">View All Cash Bids →</a>';
        area.innerHTML = html;
        console.log('[AGSIST] Homepage bids: ' + top.length + ' shown');
      })
      .catch(function(err){
        console.warn('[AGSIST] Homepage bids failed:', err);
        area.innerHTML = '<div style="text-align:center;padding:1rem;font-size:.82rem;color:var(--text-muted)">'
          + 'Cash bids unavailable.<br><a href="/cash-bids" style="color:var(--gold)">Search cash bids →</a>'
          + '</div>';
      });
  }

  window.loadHomepageBids = loadHomepageBids;

  if(window.AGSIST_GEO && window.AGSIST_GEO.lat){
    loadHomepageBids(
      window.AGSIST_GEO.lat,
      window.AGSIST_GEO.lng,
      (window.AGSIST_GEO.city || '') + ', ' + (window.AGSIST_GEO.state || '')
    );
  }
})();
