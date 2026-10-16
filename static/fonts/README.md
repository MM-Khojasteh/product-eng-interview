# Fonts — NOT committed (licensing)

`IRANYekanXVF.woff2` (the IRANYekanX variable typeface this UI self-hosts)
is a **licensed third-party asset** and is deliberately excluded from the
repository — a public repo must not redistribute the font binary.

## Restoring it locally

1. Obtain IRANYekanX from your licensed source (e.g. your Fontiran account
   or the copy captured with the design system).
2. Drop the variable font here as:

   ```
   static/fonts/IRANYekanXVF.woff2
   ```

Nothing else changes: `frontend/torob.src.css` declares
`@font-face { src: url("../fonts/IRANYekanXVF.woff2") }` — resolved relative
to the compiled `static/css/torob.css` — with
`font-display: swap` — while the file is absent the page simply renders in
the fallback system font stack.
