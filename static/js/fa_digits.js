/* Torob Travel — shared Persian digit helpers (self-hosted, zero dependencies).
 *
 * ONE implementation for every first-party script (search.js, itinerary.js);
 * the server's twin lives in src/formatting.py (fa_num — cross-language, it
 * must stay duplicated). Exposed as the TorobFa namespace: classic scripts,
 * no module loader — include this file BEFORE its consumers (all defer,
 * execution order preserved). Not stateful, so it lives on window without the
 * __-prefix reserved for HTMX-swap-surviving state (see filter_rail.js).
 */
(function () {
  "use strict";

  var FA_DIGITS = "۰۱۲۳۴۵۶۷۸۹";
  function toFa(value) {
    return String(value).replace(/\d/g, function (d) { return FA_DIGITS[+d]; });
  }
  function toEn(value) {
    return String(value).replace(/[۰-۹]/g, function (d) {
      return String(FA_DIGITS.indexOf(d));
    });
  }
  function pad2(n) { return n < 10 ? "0" + n : String(n); }

  window.TorobFa = { toFa: toFa, toEn: toEn, pad2: pad2 };
})();
