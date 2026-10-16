/* Torob Travel — ds-info-pop behaviour (the «؟» explainer popover).
 *
 * One instance = one .ds-info-pop root containing a .ds-info-pop__trigger
 * button and a .ds-info-pop__panel. State contract (torob.src.css component
 * layer): open/closed rides .hidden on the panel + aria-expanded on the
 * trigger — never a BEM state class.
 *
 *   • hover (mouse pointers only — the pointerType guard keeps touch taps
 *     from racing the click toggle): opens after a 140ms intent delay and
 *     closes 260ms after the pointer leaves the root, a grace long enough
 *     to travel from the trigger into the panel itself;
 *   • click / Enter / Space toggles. A click on an already-open-but-unpinned
 *     (hover-opened) popover PINS it instead of closing — the user asked to
 *     keep it; a pinned popover ignores mouseleave, so touch and keyboard
 *     users keep it until they dismiss it;
 *   • Esc, a click outside every popover, focus leaving the root, and the
 *     panel's own close button (data-info-pop-close) all close it.
 *
 * Document-level listeners are installed once; per-root pointer/focus
 * listeners are wired at boot and re-wired after HTMX swaps (the
 * itinerary.js boot discipline — dataset.wired guards double-fires).
 */
(function () {
  "use strict";

  var OPEN_MS = 140;   // hover intent
  var CLOSE_MS = 260;  // leave grace: trigger → panel travel time

  function roots() { return document.querySelectorAll(".ds-info-pop"); }
  function panelOf(root) { return root.querySelector(".ds-info-pop__panel"); }
  function triggerOf(root) { return root.querySelector(".ds-info-pop__trigger"); }
  function isOpen(root) { return !panelOf(root).classList.contains("hidden"); }

  function openPop(root, pinned) {
    if (pinned) root.dataset.pinned = "1";
    panelOf(root).classList.remove("hidden");
    triggerOf(root).setAttribute("aria-expanded", "true");
  }
  function closePop(root, refocus) {
    delete root.dataset.pinned;
    panelOf(root).classList.add("hidden");
    triggerOf(root).setAttribute("aria-expanded", "false");
    if (refocus) triggerOf(root).focus();
  }

  var openTimer = null;
  var closeTimer = null;
  function clearTimers() {
    clearTimeout(openTimer);
    clearTimeout(closeTimer);
  }

  function wire(root) {
    if (root.dataset.wired) return;
    root.dataset.wired = "1";

    root.addEventListener("pointerenter", function (event) {
      if (event.pointerType !== "mouse") return;
      clearTimers();
      openTimer = setTimeout(function () { openPop(root, false); }, OPEN_MS);
    });
    root.addEventListener("pointerleave", function (event) {
      if (event.pointerType !== "mouse") return;
      clearTimers();
      if (!root.dataset.pinned) {
        closeTimer = setTimeout(function () { closePop(root, false); }, CLOSE_MS);
      }
    });
    /* keyboard: focus tabbing past the popover dismisses it (relatedTarget
       is null when focus jumps to the URL bar or another window) */
    root.addEventListener("focusout", function (event) {
      if (!root.contains(event.relatedTarget)) closePop(root, false);
    });
  }

  /* click toggle (+pin), the panel's close button, and the outside tap —
     all delegated at document level */
  document.addEventListener("click", function (event) {
    var trigger = event.target.closest(".ds-info-pop__trigger");
    if (trigger) {
      var root = trigger.closest(".ds-info-pop");
      clearTimers();
      if (!isOpen(root)) openPop(root, true);
      else if (root.dataset.pinned) closePop(root, false);
      else root.dataset.pinned = "1";  // hover-opened: the click asks to keep it
      return;
    }
    var closeButton = event.target.closest("[data-info-pop-close]");
    if (closeButton) {
      closePop(closeButton.closest(".ds-info-pop"), true);
      return;
    }
    /* clicks inside any popover cluster (text selection etc.) leave it be */
    if (event.target.closest(".ds-info-pop")) return;
    roots().forEach(function (root) {
      if (isOpen(root)) closePop(root, false);
    });
  });

  document.addEventListener("keydown", function (event) {
    if (event.key !== "Escape") return;
    roots().forEach(function (root) {
      if (isOpen(root)) closePop(root, true);
    });
  });

  function boot() { roots().forEach(wire); }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
  document.body.addEventListener("htmx:afterSwap", boot);
})();
