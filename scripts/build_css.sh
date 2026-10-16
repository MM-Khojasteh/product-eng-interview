#!/usr/bin/env bash
# Build static/css/torob.css from the templates (Tailwind v3 CLI via npx —
# no package.json needed). Run after adding/changing utility classes in
# templates/**/*.html; the minified output is COMMITTED so deployments never
# need Node. Build inputs live in frontend/ (never served). Replaces the
# Tailwind Play CDN (console warning + third-party requests + blocked-in-Iran
# CDN).
set -euo pipefail
cd "$(dirname "$0")/.."
npx -y tailwindcss@3.4.17 \
  -c frontend/tailwind.config.js \
  -i frontend/torob.src.css \
  -o static/css/torob.css --minify
echo ">> built static/css/torob.css ($(wc -c < static/css/torob.css) bytes)"
