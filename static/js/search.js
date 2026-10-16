/* Torob Travel — search-widget interactions (self-hosted, zero dependencies).
 *
 * 1) Jalali calendar popover on #date — the ticket platforms' dropdown month
 *    grid (guide capture «تقویم.png»), restyled in Torob ds tokens: weekday
 *    header شنبه…جمعه, past days disabled, today dotted, selected day filled.
 * 2) Travelers popover on #passengers-trigger — بزرگسال/کودک/نوزاد steppers
 *    with age hints and a تأیید button (guide capture «مسافران.png»); the
 *    hidden #passengers input keeps the server's `passengers=1..9` contract.
 * 3) Offer chips — the sellers-list «خدمات فروشنده» filter (guide capture
 *    «Screenshot 2026-10-02 233258.png»): client-side AND-filtering of agency
 *    offer rows in the Compare modal and the PDP seller card; document-level
 *    delegation so HTMX swaps need no re-init.
 *
 * Jalali ⇄ Gregorian conversion uses the published 33-year-cycle calendar
 * algorithm popularized by jalaali-js (MIT, Behrang Norouzinia) — compact
 * enough to keep the picker offline instead of pulling a date library.
 */
(function () {
  "use strict";

  /* ── Persian digit helpers — the shared TorobFa implementations
        (static/js/fa_digits.js, loaded before this file on every page) ── */
  var toFa = window.TorobFa.toFa;
  var toEn = window.TorobFa.toEn;
  var pad2 = window.TorobFa.pad2;

  /* ── Jalali ⇄ Gregorian (jalaali-js algorithm, MIT) ── */
  var BREAKS = [-61, 9, 38, 199, 426, 686, 756, 818, 1111, 1181, 1210,
    1635, 2060, 2097, 2192, 2262, 2324, 2394, 2456, 3178];
  function div(a, b) { return Math.trunc(a / b); }
  function mod(a, b) { return a - Math.trunc(a / b) * b; }

  function jalCal(jy) {
    var gy = jy + 621, leapJ = -14, jp = BREAKS[0], jump = 0, leap, n, i, jm;
    for (i = 1; i < BREAKS.length; i += 1) {
      jm = BREAKS[i];
      jump = jm - jp;
      if (jy < jm) break;
      leapJ += div(jump, 33) * 8 + div(mod(jump, 33), 4);
      jp = jm;
    }
    n = jy - jp;
    leapJ += div(n, 33) * 8 + div(mod(n, 33) + 3, 4);
    if (mod(jump, 33) === 4 && jump - n === 4) leapJ += 1;
    var leapG = div(gy, 4) - div((div(gy, 100) + 1) * 3, 4) - 150;
    var march = 20 + leapJ - leapG;
    if (jump - n < 6) n = n - jump + div(jump + 4, 33) * 33;
    leap = mod(mod(n + 1, 33) - 1, 4);
    if (leap === -1) leap = 4;
    return { leap: leap, gy: gy, march: march };
  }

  function g2d(gy, gm, gd) {
    var d = div((gy + div(gm - 8, 6) + 100100) * 1461, 4)
      + div(153 * mod(gm + 9, 12) + 2, 5) + gd - 34840408;
    return d - div(div(gy + 100100 + div(gm - 8, 6), 100) * 3, 4) + 752;
  }

  function d2g(jdn) {
    var j = 4 * jdn + 139361631;
    j += div(div(4 * jdn + 183187720, 146097) * 3, 4) * 4 - 3908;
    var i = div(mod(j, 1461), 4) * 5 + 308;
    var gd = div(mod(i, 153), 5) + 1;
    var gm = mod(div(i, 153), 12) + 1;
    return { gy: div(j, 1461) - 100100 + div(8 - gm, 6), gm: gm, gd: gd };
  }

  function j2d(jy, jm, jd) {
    var r = jalCal(jy);
    return g2d(r.gy, 3, r.march) + (jm - 1) * 31 - div(jm, 7) * (jm - 7) + jd - 1;
  }

  function d2j(jdn) {
    var gy = d2g(jdn).gy, jy = gy - 621, r = jalCal(jy);
    var k = jdn - g2d(gy, 3, r.march), jm, jd;
    if (k >= 0) {
      if (k <= 185) return { jy: jy, jm: 1 + div(k, 31), jd: mod(k, 31) + 1 };
      k -= 186;
    } else {
      jy -= 1;
      k += 179;
      if (r.leap === 1) k += 1;
    }
    return { jy: jy, jm: 7 + div(k, 30), jd: mod(k, 30) + 1 };
  }

  function toJalaali(gy, gm, gd) { return d2j(g2d(gy, gm, gd)); }
  function isLeapJalaali(jy) { return jalCal(jy).leap === 0; }

  function monthLength(jy, jm) {
    if (jm <= 6) return 31;
    if (jm <= 11) return 30;
    return isLeapJalaali(jy) ? 30 : 29;
  }

  var MONTHS = ["فروردین", "اردیبهشت", "خرداد", "تیر", "مرداد", "شهریور",
    "مهر", "آبان", "آذر", "دی", "بهمن", "اسفند"];

  function todayJ() {
    var g = new Date();
    return toJalaali(g.getFullYear(), g.getMonth() + 1, g.getDate());
  }

  function keyOf(d) { return d.jy + "/" + pad2(d.jm) + "/" + pad2(d.jd); }

  /* '۱۴۰۵/۰۷/۱۳' → {jy,jm,jd} (null when malformed/out of range) */
  function parseFaDate(text) {
    var m = /^(\d{4})\/(\d{2})\/(\d{2})$/.exec(toEn(text || "").trim());
    if (!m) return null;
    var jy = +m[1], jm = +m[2], jd = +m[3];
    if (jm < 1 || jm > 12 || jd < 1 || jd > monthLength(jy, jm)) return null;
    return { jy: jy, jm: jm, jd: jd };
  }

  /* ── Home flow: the hero+search cluster NEVER moves when a search fills
     #results — results flow in below the form (user direction, 2026-10-03).
     Keeping the safe-center class alone can't promise that: the search is
     multi-swap (skeleton → tall shell), and safe center (a) re-centers the
     cluster on every below-form height change and (b) falls back to top-align
     the moment the shell outgrows the viewport — a visible hop. So the FIRST
     swap freezes the cluster exactly where it sits: htmx:beforeSwap runs
     before any layout change, so the measured hero offset is the pre-search
     position; the centering is traded for that exact padding-top, and every
     later swap only ever changes content BELOW the form. The home-only extras
     (popular routes + the ترب‌پی strip) hide once results own the page. */
  document.body.addEventListener("htmx:beforeSwap", function (event) {
    var target = event.detail && event.detail.target;
    if (!target || target.id !== "results") return;
    var flow = document.getElementById("home-flow");
    if (!flow || flow.dataset.pinned) return;
    var first = flow.firstElementChild;
    if (!first) return;
    var pad = first.getBoundingClientRect().top - flow.getBoundingClientRect().top;
    flow.classList.remove("[justify-content:safe_center]", "pt-10");
    flow.style.paddingTop = pad + "px";
    flow.dataset.pinned = "1";
  });
  document.body.addEventListener("htmx:afterSwap", function (event) {
    var target = event.detail && event.detail.target;
    if (target && target.id === "results") {
      var extras = document.getElementById("home-extras");
      if (extras) extras.classList.add("hidden");
    }
  });

  /* ── Popover plumbing (shared by both widgets) ── */
  var openRoot = null;

  function closePopover() {
    if (!openRoot) return;
    var pop = openRoot.querySelector("[role=dialog]");
    var trigger = openRoot.querySelector("[aria-haspopup=dialog]");
    if (pop) pop.classList.add("hidden");
    if (trigger) trigger.setAttribute("aria-expanded", "false");
    openRoot = null;
  }

  function openPopover(root) {
    closePopover();
    openRoot = root;
    var pop = root.querySelector("[role=dialog]");
    var trigger = root.querySelector("[aria-haspopup=dialog]");
    if (pop) pop.classList.remove("hidden");
    if (trigger) trigger.setAttribute("aria-expanded", "true");
  }

  document.addEventListener("click", function (event) {
    if (openRoot && !openRoot.contains(event.target)) closePopover();
  });
  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape") closePopover();
  });

  /* ── 1) Calendar ── */
  var dateInput = document.getElementById("date");
  var calGrid = document.getElementById("cal-grid");
  var calTitle = document.getElementById("cal-title");
  var dateRoot = dateInput && dateInput.closest("[data-popover-root]");

  if (dateInput && dateRoot && calGrid) {
    var today = todayJ();
    var selected = parseFaDate(dateInput.value);
    var view = selected ? { jy: selected.jy, jm: selected.jm }
      : { jy: today.jy, jm: today.jm };

    // The picker is the primary input path now — typing stays available
    // only without JS (progressive enhancement, pattern-validated).
    dateInput.readOnly = true;

    dateInput.addEventListener("focus", function () { openPopover(dateRoot); render(); });
    dateInput.addEventListener("click", function () { openPopover(dateRoot); render(); });

    dateRoot.addEventListener("click", function (event) {
      var nav = event.target.closest("[data-cal]");
      if (nav) {
        var step = nav.dataset.cal === "next" ? 1 : -1;
        var jm = view.jm + step;
        if (jm < 1) { view = { jy: view.jy - 1, jm: 12 }; }
        else if (jm > 12) { view = { jy: view.jy + 1, jm: 1 }; }
        else { view.jm = jm; }
        render();
        return;
      }
      if (event.target.closest('[data-cal="today"]')) {
        view = { jy: today.jy, jm: today.jm };
        render();
        return;
      }
      var day = event.target.closest("[data-day]");
      if (day && !day.disabled) {
        selected = parseFaDate(day.dataset.day);
        dateInput.value = toFa(keyOf(selected));
        closePopover();
        render();
      }
    });

    function render() {
      calTitle.textContent = MONTHS[view.jm - 1] + " " + toFa(view.jy);

      // start-of-month weekday: Jalali 1st → Gregorian → getDay (0=Sun),
      // Persian week starts شنبه (Saturday) = column 0.
      var g1 = d2g(j2d(view.jy, view.jm, 1));
      var lead = (new Date(g1.gy, g1.gm - 1, g1.gd).getDay() + 1) % 7;

      var prevDisabled = view.jy < today.jy || (view.jy === today.jy && view.jm <= today.jm);
      var prevBtn = dateRoot.querySelector('[data-cal="prev"]');
      if (prevBtn) prevBtn.disabled = prevDisabled;

      var selectedKey = selected ? keyOf(selected) : "";
      var todayKey = keyOf(today);
      var html = "";
      for (var i = 0; i < lead; i++) html += '<span></span>';
      for (var d = 1; d <= monthLength(view.jy, view.jm); d++) {
        var day = { jy: view.jy, jm: view.jm, jd: d };
        var key = keyOf(day);
        var past = key < todayKey;
        var isToday = key === todayKey;
        var isSelected = key === selectedKey;
        var friday = lead + d - 1; // (lead + d - 1) % 7 === 6 ⇒ جمعه
        html += '<button type="button" data-day="' + key + '"' + (past ? " disabled" : "") +
          ' class="relative flex h-9 items-center justify-center rounded-lg text-xs font-medium transition';
        if (isSelected) {
          html += " bg-focus text-fog";
        } else if (past) {
          html += " cursor-default text-muted opacity-40";
        } else {
          html += " text-ink hover:bg-soft";
          if (friday % 7 === 6) html += " text-redink";
        }
        if (isToday && !isSelected) html += " font-bold";
        html += '">' + toFa(d);
        if (isToday && !isSelected) {
          html += '<span class="absolute bottom-1 h-1 w-1 rounded-full bg-focus" aria-hidden="true"></span>';
        }
        html += "</button>";
      }
      calGrid.innerHTML = html;
    }

    render();
  }

  /* ── 2) Travelers ── */
  var paxRoot = document.querySelector("#pax-popover")
    && document.getElementById("pax-popover").closest("[data-popover-root]");
  var paxTrigger = document.getElementById("passengers-trigger");
  var paxHidden = document.getElementById("passengers");
  var MAX_TOTAL = 9;

  if (paxRoot && paxTrigger && paxHidden) {
    var initial = Math.max(1, Math.min(parseInt(paxHidden.value, 10) || 1, MAX_TOTAL));
    var pax = { adult: initial, child: 0, infant: 0 };
    var labels = { adult: "بزرگسال", child: "کودک", infant: "نوزاد" };

    paxTrigger.addEventListener("click", function () {
      if (openRoot === paxRoot) { closePopover(); return; }
      openPopover(paxRoot);
      renderPax();
    });

    paxRoot.addEventListener("click", function (event) {
      if (event.target.closest("[data-pax-apply]")) {
        closePopover();
        return;
      }
      var btn = event.target.closest("[data-pax]");
      if (!btn) return;
      var kind = btn.dataset.pax;
      var delta = parseInt(btn.dataset.delta, 10);
      var total = pax.adult + pax.child + pax.infant;
      if (delta > 0 && total >= MAX_TOTAL) return;
      var next = pax[kind] + delta;
      if (kind === "adult") {
        if (next < 1 || next > MAX_TOTAL) return;
        pax.adult = next;
        if (pax.infant > pax.adult) pax.infant = pax.adult; // a نوزاد rides with a بزرگسال
      } else {
        if (next < 0 || next > 6) return;
        if (kind === "infant" && next > pax.adult) return;
        pax[kind] = next;
      }
      renderPax();
    });

    function renderPax() {
      for (var kind in pax) {
        var count = paxRoot.querySelector('[data-pax-count="' + kind + '"]');
        if (count) count.textContent = toFa(pax[kind]);
      }
      var total = pax.adult + pax.child + pax.infant;
      paxRoot.querySelectorAll("[data-pax]").forEach(function (btn) {
        var k = btn.dataset.pax;
        var delta = parseInt(btn.dataset.delta, 10);
        var atLimit = (delta < 0 && pax[k] === 0)
          || (delta < 0 && k === "adult" && pax.adult === 1)
          || (delta > 0 && total >= MAX_TOTAL)
          || (delta > 0 && k === "infant" && pax.infant >= pax.adult);
        btn.disabled = atLimit;
      });
      paxHidden.value = total;
      var summary = document.getElementById("pax-summary");
      if (summary) {
        // adults-only reads «N مسافر» (the platforms' compact form); the
        // بزرگسال/کودک/نوزاد breakdown appears once the mix is non-trivial.
        if (!pax.child && !pax.infant) {
          summary.textContent = toFa(total) + " مسافر";
        } else {
          var parts = [];
          ["adult", "child", "infant"].forEach(function (k) {
            if (pax[k]) parts.push(toFa(pax[k]) + " " + labels[k]);
          });
          summary.textContent = parts.join("، ");
        }
      }
    }

    renderPax();
  }

  /* ── 3) Offer chips — client-side ds-seller-list filtering (Compare modal +
     the PDP «خرید از کدام آژانس؟» card), the torob.com sellers-list
     «خدمات فروشنده» pattern. Chips toggle aria-pressed (the compiled
     aria-pressed:* states style them); a row stays visible iff it carries
     EVERY active chip's attribute (AND); tombstoned rows carry no flags, so
     any active filter hides them. Delegated on document — survives the HTMX
     swaps into #compare-modal-slot; each swap replaces the DOM, so filters
     reset naturally with the fresh data. No-JS: chips render but are inert,
     all rows stay visible. ── */
  document.addEventListener("click", function (event) {
    var chip = event.target.closest("[data-offer-chip]");
    if (!chip) return;
    var group = chip.closest("[data-seller-group]");
    if (!group) return;
    chip.setAttribute("aria-pressed",
      chip.getAttribute("aria-pressed") === "true" ? "false" : "true");
    var active = [].slice.call(
      group.querySelectorAll('[data-offer-chip][aria-pressed="true"]')
    ).map(function (c) { return "data-" + c.dataset.offerChip; });
    group.querySelectorAll("[data-offer-row]").forEach(function (row) {
      row.classList.toggle("hidden",
        active.length > 0 && active.some(function (attr) {
          return !row.hasAttribute(attr);
        }));
    });
    var note = group.querySelector("[data-offer-empty]");
    if (note) {
      note.classList.toggle("hidden",
        group.querySelectorAll("[data-offer-row]:not(.hidden)").length > 0);
    }
  });
})();
