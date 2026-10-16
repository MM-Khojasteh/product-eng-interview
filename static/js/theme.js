/* Torob Travel — theme picker («ظاهر برنامه» modal).
 *
 * Extracted verbatim from templates/base.html's end-of-body inline script
 * (architecture refactor §5.1 — behavior identical: the external file is
 * loaded deferred at the same position, i.e. after the modal markup exists).
 *
 * torob.com's captured ThemeSettingsModal flow: the header glyph opens the
 * dialog, options apply system/light/dark (persisted in localStorage,
 * applied pre-paint by the inline resolver that must STAY in <head>), X /
 * backdrop / Escape close it. The pre-paint resolver is inline in base.html
 * on purpose — externalizing it would reintroduce a flash of wrong theme.
 */
(function () {
  "use strict";
  const button = document.getElementById("theme-toggle");
  const modal = document.getElementById("theme-modal");
  const labels = { system: "Theme: system", light: "Theme: light", dark: "Theme: dark" };

  function current() {
    return localStorage.getItem("torob-theme") || "system";
  }
  function apply(theme) {
    const dark = theme === "dark"
      || (theme === "system" && matchMedia("(prefers-color-scheme: dark)").matches);
    document.documentElement.dataset.theme = dark ? "dark" : "light";
    button.setAttribute("aria-label", labels[theme]);
    for (const icon of button.querySelectorAll("[data-icon]")) {
      icon.classList.toggle("hidden", icon.dataset.icon !== theme);
    }
    for (const option of modal.querySelectorAll("[data-theme-opt]")) {
      option.setAttribute("aria-pressed", String(option.dataset.themeOpt === theme));
    }
  }
  function open() {
    modal.classList.remove("hidden");
    button.setAttribute("aria-expanded", "true");
  }
  function close() {
    modal.classList.add("hidden");
    button.setAttribute("aria-expanded", "false");
  }
  button.addEventListener("click", open);
  modal.querySelector("[data-theme-close]").addEventListener("click", close);
  modal.addEventListener("click", (event) => {
    if (event.target === modal) close();  // backdrop click
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") close();
  });
  for (const option of modal.querySelectorAll("[data-theme-opt]")) {
    option.addEventListener("click", () => {
      localStorage.setItem("torob-theme", option.dataset.themeOpt);
      apply(option.dataset.themeOpt);
      close();
    });
  }
  matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => apply(current()));
  apply(current());
})();
