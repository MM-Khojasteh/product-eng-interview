/* Torob Travel — results filter-rail behaviour.
 *
 * Extracted verbatim from templates/flights/partials/filters.html's inline script
 * (architecture refactor §5.1). The partial still carries a <script src> for
 * this file, so htmx re-executes it on every shell swap exactly as the inline
 * original did — bytes now come from the immutable content-hashed asset
 * instead of riding inside every /search/flights/results fragment.
 *
 * Rail behaviour: the mobile drawer (captured mobile-filter-drawer), the
 * FilterTitleRow accordions, «جستجو در نتایج» (client-side card filter), the
 * airline SearchBox and the price actions.
 *
 * NB: htmx SETTLES every swap by restoring the server-rendered class/style
 * attributes, which erases JS-added classes made during the swap window
 * (accordion collapse, filtered rows/cards). All rail state therefore lives in
 * window.* and is re-applied by syncRail() — now, and after every
 * htmx:afterSettle.
 */
(function () {
  "use strict";
  var drawer = document.getElementById("filter-drawer");
  var scrim = document.getElementById("filter-scrim");
  if (drawer && scrim) {
    var close = function () {
      drawer.classList.add("translate-x-full");
      scrim.hidden = true;
      document.body.style.overflow = "";
    };
    document.getElementById("filter-open")?.addEventListener("click", function () {
      drawer.classList.remove("translate-x-full");
      scrim.hidden = false;
      document.body.style.overflow = "hidden";
    });
    scrim.addEventListener("click", close);
    document.getElementById("filter-close")?.addEventListener("click", close);
  }

  var collapsed = window.__railCollapsed || (window.__railCollapsed = {});

  // Re-apply ALL rail state onto whatever nodes are in the document.
  function syncRail() {
    document.querySelectorAll("[data-filter-group]").forEach(function (group) {
      var toggle = group.querySelector("[data-filter-toggle]");
      var content = group.querySelector("[data-filter-content]");
      if (!toggle || !content) return;
      var closed = !!collapsed[group.dataset.filterGroup];
      toggle.setAttribute("aria-expanded", String(!closed));
      content.classList.toggle("hidden", closed);
      var icon = toggle.querySelector("svg");
      if (icon) icon.classList.toggle("rotate-180", closed);
    });

    var box = document.querySelector("[data-airline-search]");
    if (box) {
      var q = (window.__airlineQuery || "").trim();
      var any = false;
      document.querySelectorAll("[data-airline-row]").forEach(function (row) {
        var hit = !q || row.dataset.name.indexOf(q) !== -1;
        row.classList.toggle("hidden", !hit);
        any = any || hit;
      });
      var empty = document.querySelector("[data-airline-empty]");
      if (empty) empty.classList.toggle("hidden", any);
    }

    var rq = (window.__resultsQuery || "").trim();
    var rany = false;
    document.querySelectorAll("article[id^='flight-']").forEach(function (card) {
      var hit = !rq || card.textContent.indexOf(rq) !== -1;
      card.style.display = hit ? "" : "none";
      rany = rany || hit;
    });
    var note = document.querySelector("[data-results-empty-note]");
    if (note) note.classList.toggle("hidden", rany || !rq);
  }

  // bind onto the fresh nodes (this script re-runs on every shell swap)
  document.querySelectorAll("[data-filter-group]").forEach(function (group) {
    var toggle = group.querySelector("[data-filter-toggle]");
    if (!toggle) return;
    toggle.addEventListener("click", function () {
      var key = group.dataset.filterGroup;
      collapsed[key] = !collapsed[key];
      syncRail();
    });
  });
  var box = document.querySelector("[data-airline-search]");
  if (box) {
    box.value = window.__airlineQuery || "";
    box.addEventListener("input", function () {
      window.__airlineQuery = box.value;
      syncRail();
    });
  }
  var rbox = document.querySelector("[data-results-search]");
  if (rbox) {
    rbox.value = window.__resultsQuery || "";
    rbox.addEventListener("input", function () {
      window.__resultsQuery = rbox.value;
      syncRail();
    });
  }

  // one global settle hook — afterSettle fires once htmx has finished
  // restoring attributes, so classes added here stick. Covers shell swaps
  // AND the OOB liveness pulse inserting streamed-in cards.
  if (!window.__railSyncHook) {
    window.__railSyncHook = true;
    document.body.addEventListener("htmx:afterSettle", syncRail);
  }
  syncRail();

  // PriceSuggestions — click fills از/تا with the observed result range
  var suggest = document.querySelector("[data-price-suggest]");
  if (suggest) suggest.addEventListener("click", function () {
    var lo = document.querySelector('#flight-filters input[name="min_price"]');
    var hi = document.querySelector('#flight-filters input[name="max_price"]');
    if (lo) lo.value = suggest.dataset.min;
    if (hi) hi.value = suggest.dataset.max;
  });

  // حذف — clears the price bounds and re-requests the shell (captured
  // priceFilter removeBtn). requestSubmit() is intercepted by htmx.
  var clear = document.querySelector("[data-price-clear]");
  if (clear) clear.addEventListener("click", function () {
    var form = document.getElementById("flight-filters");
    var lo = form.querySelector('input[name="min_price"]');
    var hi = form.querySelector('input[name="max_price"]');
    if (!((lo && lo.value) || (hi && hi.value))) return;
    if (lo) lo.value = "";
    if (hi) hi.value = "";
    form.requestSubmit();
  });
})();
