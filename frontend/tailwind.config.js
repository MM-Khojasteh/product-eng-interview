/** Torob Travel — Tailwind build config (Tailwind v3 CLI).
 *  Mirrors the design tokens in torob-design-capture/design-system.json — the
 *  same config the Tailwind Play CDN used to receive inline in base.html.
 *  Colors resolve at runtime to the CSS variables declared in
 *  frontend/torob.src.css (light :root + html[data-theme="dark"] sets).
 *
 *  Build: bash scripts/build_css.sh   (output committed — prod needs no Node)
 */
module.exports = {
  darkMode: ["selector", '[data-theme="dark"]'],
  // relative: globs resolve from this file, not from the shell's cwd.
  content: {
    relative: true,
    files: ["../templates/**/*.html", "../main.py", "../static/js/**/*.js"],
  },
  // "container" appears only as scan noise (.leaflet-container override in
  // itinerary/planner.html) — nothing uses the core container utility; block it so a
  // future hand-written .container can never fight Tailwind's (refactor §4.5).
  blocklist: ["container"],
  // poi_tile.html sizes its square via Jinja (h-[{{ px }}px]) — Tailwind
  // can't scan computed class names, so the concrete tile utilities are
  // safelisted here (100px/txt20 = timeline stop rows, 44px/txt14 =
  // suppliers-modal rows). Without this the no-photo letter tile collapses
  // (imgs survive on their width/height attributes) and the stop rows
  // misalign — seen 2026-10-16.
  safelist: [
    "h-[100px]", "w-[100px]", "text-[20px]",
    "h-[44px]", "w-[44px]", "text-[14px]",
  ],
  theme: {
    extend: {
      colors: {
        bright: "var(--bg-bright)",   // page canvas  · #f1f5f9 ⇄ #15202b
        fog: "var(--bg-fog)",         // card surface · #ffffff ⇄ #212b36
        field: "var(--sky-50)",       // subtle fill  · #f8fafc ⇄ #0f172b
        soft: "var(--sky-100)",       // chips track  · #f1f5f9 ⇄ #1e293b
        ink: "var(--sky-800)",        // text         · #1e293b ⇄ #f1f5f9
        muted: "var(--sky-500)",      // secondary text
        line: "var(--sky-300)",       // 1px surface borders
        brand: { DEFAULT: "var(--brand)", hover: "#bd2d3c" },  // ds-button--red:hover
        logo: "var(--logo-color-1)",  // wordmark red · #e91e33 ⇄ dark #f1f5f9 (captured monochrome flip)
        focus: "var(--blue-600)",     // --focus
        bluesoft: "var(--blue-50)",   // ds-chip--blue / AI-strip tint · #eff6ff ⇄ #172554
        blueink: "var(--blue-800)",   // blue-tint text · #1e40af ⇄ #b6ccfb
        redsoft: "var(--red-50)",     // ds-price-highlight fill
        redink: "var(--red-800)",     // ds-price-highlight text
        greensoft: "var(--green-50)",
        greenink: "var(--green-800)",
      },
      fontFamily: {
        sans: ["IRANYekanX", "Vazirmatn", "system-ui", "Tahoma", "sans-serif"],
      },
      boxShadow: {
        card: "var(--shadow-sm)",     // 0 1px 3px rgba(30,41,59,.08)
        pop: "var(--shadow-md)",      // 0 8px 24px rgba(30,41,59,.12)
      },
    },
  },
};
