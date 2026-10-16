/* Torob Travel — itinerary map.
 *
 * Extracted from templates/itinerary/planner.html's inline script (architecture
 * refactor §5.1). Behavior identical, with exactly two mechanical deltas,
 * both invisible to the running page:
 *   1. the vendored asset URLs (Leaflet CSS/JS, Supercluster) moved from
 *      Jinja {{ asset_url(...) }} expressions to data-* attributes on
 *      #itinerary-map, read at boot — a static file cannot render Jinja, and
 *      this keeps the content-hash cache-busting;
 *   2. Persian digits come from the shared TorobFa helpers (fa_digits.js,
 *      loaded before this file) instead of a local copy;
 *   3. the map-host local was renamed to `mapHost` so the bare Tailwind scan
 *      token that used to generate a dead core utility here is gone.
 *
 * The address-geocoder Leaflet pattern (frontend/src/utils/leaflet.ts +
 * CommerceMap.tsx) translated to vanilla JS for Jinja2/HTMX:
 *
 *   1. lazy, dependency-free loader for the VENDORED Leaflet
 *      (/static/vendor/leaflet — self-hosted, zero third-party origins, the
 *      base.html policy) and vendored Supercluster;
 *   2. OSM raster tiles + the full attribution string;
 *   3. plain imperative L.* calls, booted on DOMContentLoaded AND
 *      htmx:afterSwap (fragment loads);
 *   4. Supercluster clustering with L.divIcon pills;
 *   5. preferCanvas: true, zoomSnap: 0.2 (CommerceMap's exact options);
 *   6. one dashed polyline per day, straight-line, drawn in stop order.
 */
(function () {
  "use strict";

  var LEAFLET_JS = "";        // armed at boot() from #itinerary-map data-* attrs
  var LEAFLET_CSS = "";
  var SUPERCLUSTER_JS = "";

  /* ── vendor loaders (leaflet.ts pattern: idempotent by DOM id, promise
        memoised; the memo is dropped on settle so a failed load can retry
        while a successful one short-circuits on window.L) ── */
  function loadCss(href, id) {
    return new Promise(function (resolve, reject) {
      if (document.getElementById(id)) return resolve();
      var link = document.createElement("link");
      link.id = id; link.rel = "stylesheet"; link.href = href;
      link.onload = function () { resolve(); };
      link.onerror = function () { reject(new Error("css_load_failed")); };
      document.head.appendChild(link);
    });
  }
  function loadScript(src, id) {
    return new Promise(function (resolve, reject) {
      var existing = document.getElementById(id);
      if (existing) {
        if (existing.dataset.loaded) return resolve();
        existing.addEventListener("load", function () { resolve(); });
        return;
      }
      var script = document.createElement("script");
      script.id = id; script.src = src; script.async = true;
      script.onload = function () { script.dataset.loaded = "1"; resolve(); };
      script.onerror = function () { reject(new Error("script_load_failed " + src)); };
      document.head.appendChild(script);
    });
  }
  var leafletPromise = null;
  function ensureLeaflet() {
    if (window.L && window.L.map) return Promise.resolve(window.L);
    if (!leafletPromise) {
      leafletPromise = Promise.all([
        loadCss(LEAFLET_CSS, "leaflet-css"),
        loadScript(LEAFLET_JS, "leaflet-js"),
      ]).then(function () { return window.L; })
        .finally(function () { leafletPromise = null; });
    }
    return leafletPromise;
  }
  function ensureSupercluster() {
    if (window.Supercluster) return Promise.resolve(window.Supercluster);
    return loadScript(SUPERCLUSTER_JS, "supercluster-js")
      .then(function () { return window.Supercluster; });
  }

  /* ── Persian digits via the shared helpers (fa_digits.js — same output as
        the server's fa_num/toman filters, formatting.py); Toman grouping is
        map-specific and stays local ── */
  var toFa = window.TorobFa.toFa;
  function faToman(value) {
    var grouped = String(value).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
    return toFa(grouped) + " تومان";
  }
  function escapeHtml(value) {  /* CommerceMap tooltip discipline — names are data, not HTML */
    return String(value).replace(/[&<>"']/g, function (ch) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch];
    });
  }

  /* ── Day switcher (#itinerary-daybar) — ONE source of truth shared by the
        sticky bar, the timeline cards and the map. Lives OUTSIDE the Leaflet
        promise on purpose: cards keep filtering even when the map never
        boots. applyDayToMap is installed by initMap once Leaflet is up, so a
        day chosen while it was still loading is applied the moment the map
        is ready. Document-level delegation = no re-init after HTMX swaps
        (search.js §3 discipline); the timeline→map handler below ignores
        a/button clicks, so the two delegations never collide. ── */
  var activeDay = "all";        // "all" | 1..n
  var applyDayToMap = null;     // installed by initMap

  function setDay(value) {
    activeDay = value === "all" ? "all" : Number(value);
    var bar = document.getElementById("itinerary-daybar");
    if (bar) {
      var current = activeDay === "all" ? "all" : String(activeDay);
      bar.querySelectorAll("button[data-day]").forEach(function (el) {
        el.setAttribute("aria-pressed", String(el.dataset.day === current));
      });
    }
    document.querySelectorAll("[data-day-card]").forEach(function (card) {
      card.classList.toggle("hidden",
        activeDay !== "all" && Number(card.dataset.dayCard) !== activeDay);
    });
    if (applyDayToMap) applyDayToMap();
  }

  document.addEventListener("click", function (event) {
    var button = event.target.closest("#itinerary-daybar button[data-day]");
    if (button) setDay(button.dataset.day);
  });

  function initMap(mapHost, data) {
    Promise.all([ensureLeaflet(), ensureSupercluster()]).then(function (libs) {
      var L = libs[0];
      var Supercluster = libs[1];

      var map = L.map(mapHost, {
        zoomControl: false,
        attributionControl: true,
        preferCanvas: true,   // canvas renderer — polylines stay cheap on low-end phones
        zoomSnap: 0.2,        // smooth re-fits when the day filter changes
        minZoom: 10,
        maxZoom: 18,
      });
      L.control.zoom({ position: "bottomright" }).addTo(map);
      L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
        maxZoom: 18,
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener noreferrer">OpenStreetMap</a> contributors',
      }).addTo(map);

      var routeLayer = L.layerGroup().addTo(map);
      var markerLayer = L.layerGroup().addTo(map);

      /* one dashed polyline per day, in TSP stop order */
      var routesByDay = {};
      var placesBySlug = {};
      data.days.forEach(function (day) {
        routesByDay[day.no] = L.polyline(day.path, {
          color: day.color, weight: 3, opacity: 0.75,
          dashArray: "1 7", lineCap: "round",
        }).addTo(routeLayer);
        day.places.forEach(function (place) { placesBySlug[place.slug] = place; });
      });

      /* supercluster over every stop — radius 70 / maxZoom 14 dissolves
         clusters well before building zoom (CommerceMap §clusterIndex) */
      var index = new Supercluster({
        radius: 70, maxZoom: 14, minPoints: 2,
        map: function () { return {}; },
        reduce: function () {},
      });
      var features = [];
      data.days.forEach(function (day) {
        day.places.forEach(function (place) {
          place.stop_total = day.places.length;  // popup: «ایستگاه ۲ از ۶»
          features.push({
            type: "Feature",
            properties: { place: place, day: day.no, color: day.color },
            geometry: { type: "Point", coordinates: [place.lng, place.lat] },
          });
        });
      });
      index.load(features);

      function pinIcon(color, label) {
        return L.divIcon({
          className: "ds-itin-pin-wrap",
          html: '<span class="ds-itin-pin" style="--c:' + color + '"><small>ایستگاه</small>' + label + "</span>",
          iconSize: [62, 26], iconAnchor: [31, 13],
        });
      }
      function clusterIcon(count) {
        return L.divIcon({
          className: "ds-itin-pin-wrap",
          html: '<span class="ds-itin-cluster">' + toFa(count) + "<small>مکان</small></span>",
          iconSize: [66, 26], iconAnchor: [33, 13],
        });
      }
      function popupHtml(place) {
        var price = place.lowest_price
          ? '<span class="ds-price-highlight ds-price-highlight--popup">کف قیمت <strong class="ds-price-highlight__amount text-[13px]">' + faToman(place.lowest_price) + "</strong></span>"
          : "";
        var cta = place.has_ticket && place.redirect_url
          ? '<a class="ds-button--red inline-flex items-center gap-1 mt-2 h-8 px-2.5 rounded-lg text-[11px] font-bold no-underline" href="' + escapeHtml(place.redirect_url) +
            '" target="_blank" rel="noopener noreferrer">تأمین‌کنندگان' +
            '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg></a>'
          : "";
        return '<p class="ds-itin-pop__stop">ایستگاه ' + toFa(place.stop) + " از " + toFa(place.stop_total || "?") + "</p>" +
               '<p class="ds-itin-pop__name">' + escapeHtml(place.name) + "</p>" +
               '<p class="ds-itin-pop__cat">' + escapeHtml(place.category) + "</p>" + price + cta;
      }

      function renderClusters() {
        markerLayer.clearLayers();
        var bounds = map.getBounds();
        var zoom = Math.round(map.getZoom());
        index.getClusters(
          [bounds.getWest(), bounds.getSouth(), bounds.getEast(), bounds.getNorth()], zoom
        ).forEach(function (feature) {
          var lng = feature.geometry.coordinates[0];
          var lat = feature.geometry.coordinates[1];
          var props = feature.properties || {};
          if (props.cluster) {
            var cluster = L.marker([lat, lng], { icon: clusterIcon(props.point_count) });
            cluster.on("click", function () {
              var target = Math.min(
                index.getClusterExpansionZoom(props.cluster_id), map.getMaxZoom());
              map.flyTo([lat, lng], target);
            });
            cluster.addTo(markerLayer);
            return;
          }
          if (activeDay !== "all" && props.day !== activeDay) return;  // legend filter
          var place = props.place;
          var marker = L.marker([lat, lng], {
            icon: pinIcon(props.color, toFa(place.stop)),
          });
          marker.bindPopup(popupHtml(place), { autoPanPadding: [12, 12] });
          marker.addTo(markerLayer);
        });
      }
      map.on("moveend", renderClusters);
      map.on("zoomend", renderClusters);

      /* initial view: whole plan, pad like CommerceMap's fit */
      var allPoints = [];
      data.days.forEach(function (day) { allPoints = allPoints.concat(day.path); });
      if (allPoints.length > 1) map.fitBounds(L.latLngBounds(allPoints).pad(0.18));
      else if (allPoints.length === 1) map.setView(allPoints[0], data.zoom);
      else map.setView(data.center, data.zoom);
      renderClusters();
      setTimeout(function () { map.invalidateSize({ pan: false }); }, 220);

      /* map side of the day switcher — toggles that day's polyline + pins
         and re-fits, driven by setDay() above (the sticky bar owns the
         state); installed late so the pre-load selection syncs on boot */
      applyDayToMap = function () {
        var visible = [];
        Object.keys(routesByDay).forEach(function (no) {
          var show = activeDay === "all" || Number(no) === activeDay;
          if (show) {
            routesByDay[no].addTo(routeLayer);
            var day = data.days.filter(function (d) { return d.no === Number(no); })[0];
            if (day) visible = visible.concat(day.path);
          } else {
            routeLayer.removeLayer(routesByDay[no]);
          }
        });
        renderClusters();
        if (visible.length > 1) map.fitBounds(L.latLngBounds(visible).pad(0.18));
        /* single-stop day (روز ۱ = کشتی یونانی): fitBounds needs 2+ points,
           so fly in instead — zoom 15 > cluster maxZoom 14, the lone pin
           dissolves out of its cluster (same rule as the stop-click) */
        else if (visible.length === 1) {
          map.flyTo(visible[0], Math.max(map.getZoom(), 15), { duration: 0.6 });
        }
      };
      applyDayToMap();

      /* timeline stop → focus map. Document-level delegation so HTMX swaps
         need no re-init (search.js §3 discipline); links/buttons pass through
         so the «تأمین‌کنندگان» handoff is never hijacked. */
      document.addEventListener("click", function (event) {
        if (event.target.closest("a,button")) return;
        var stop = event.target.closest("[data-place-slug]");
        if (!stop) return;
        var place = placesBySlug[stop.dataset.placeSlug];
        if (!place) return;
        /* zoom 15 > cluster maxZoom 14, so the target stop always dissolves
           out of its cluster and gets its own pin */
        map.flyTo([place.lat, place.lng], Math.max(map.getZoom(), 15), { duration: 0.6 });
        /* the map moves WITH the reader (user direction): when the frame is
           out of view — mobile, where the map sits below the whole list —
           scroll it to the middle so the flyTo is actually watched. On
           desktop the sticky aside is always in view, so this is a no-op. */
        var frame = mapHost.closest(".ds-itin-map-frame");
        if (frame) {
          var rect = frame.getBoundingClientRect();
          if (rect.top > window.innerHeight - 120 || rect.bottom < 120) {
            frame.scrollIntoView({ behavior: "smooth", block: "center" });
          }
        }
      });
    }).catch(function (error) {
      mapHost.innerHTML =
        '<div class="ds-itin-map-error">نقشه در دسترس نیست؛ مسیرها را از تایم‌لاین دنبال کن.</div>';
      if (window.console) console.warn("itinerary map:", error);
    });
  }

  /* boot on full-page load AND on HTMX fragment swaps; dataset.booted
     guards the double-fire when both fire for the same markup */
  function boot() {
    var mapHost = document.getElementById("itinerary-map");
    var dataEl = document.getElementById("itinerary-data");
    if (!mapHost || !dataEl || mapHost.dataset.booted) return;
    mapHost.dataset.booted = "1";
    LEAFLET_JS = mapHost.dataset.leafletJs;
    LEAFLET_CSS = mapHost.dataset.leafletCss;
    SUPERCLUSTER_JS = mapHost.dataset.superclusterJs;
    var data;
    try { data = JSON.parse(dataEl.textContent || "{}"); }
    catch (error) { if (window.console) console.warn("itinerary data:", error); return; }
    initMap(mapHost, data);
  }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
  document.body.addEventListener("htmx:afterSwap", boot);
})();
