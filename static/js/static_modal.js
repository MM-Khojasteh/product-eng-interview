/* Torob Travel — static ds-modal behaviour (open / X / backdrop / Escape).
 *
 * The theme-modal contract (theme.js) generalized for modals whose markup
 * ships with the page — no fetch, no HTMX: the itinerary «تأمین‌کنندگان»
 * breakdown modal renders from the same page context as the budget card,
 * because itinerary floors are build-time facts (unlike the flight Compare
 * modal, which stays HTMX-driven for its live Redis read).
 *
 *   [data-modal-open="#id"]   opener button; aria-expanded is maintained
 *                             and focus returns to it on close
 *   [data-modal-root]         the .ds-modal-backdrop root — .hidden toggles
 *                             visibility (never a BEM state class)
 *   [data-modal-close]        close buttons inside the dialog
 *
 * Escape closes every open static modal, so it also gives the theme modal
 * a backdrop tap it did not have (its own X / Esc paths stay untouched).
 */
(function () {
  "use strict";

  function openerFor(modal) {
    return document.querySelector('[data-modal-open="#' + modal.id + '"]');
  }

  function close(modal) {
    modal.classList.add("hidden");
    var opener = openerFor(modal);
    if (opener) {
      opener.setAttribute("aria-expanded", "false");
      opener.focus();
    }
  }

  document.addEventListener("click", function (event) {
    var opener = event.target.closest("[data-modal-open]");
    if (opener) {
      var modal = document.querySelector(opener.getAttribute("data-modal-open") || "");
      if (modal) {
        modal.classList.remove("hidden");
        opener.setAttribute("aria-expanded", "true");
      }
      return;
    }
    var root = event.target.closest("[data-modal-root]");
    if (root && (event.target.closest("[data-modal-close]") || event.target === root)) {
      close(root);
    }
  });

  document.addEventListener("keydown", function (event) {
    if (event.key !== "Escape") return;
    document.querySelectorAll("[data-modal-root]:not(.hidden)").forEach(close);
  });
})();
