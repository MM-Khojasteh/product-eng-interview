# Brand logos — provenance

Bundled locally on purpose: the app is offline-first and Iran-network reality
makes runtime CDN/logo-service dependencies unreliable (several brand sites are
unreachable; Google-based favicon services are filtered). Refresh by re-running
the curl commands below and committing the files.

All logos are **trademarks of their owners**, used nominatively to identify the
airline/agency on a price-comparison surface — the same fair-use basis every
metasearch (incl. torob.com itself) displays brand marks under. Not affiliation,
not endorsement.

## Airlines (`airlines/<IATA>.png` — 128px PNG thumbnails via Special:FilePath)

| File | Source (en.wikipedia local file) | Fetched |
|---|---|---|
| IR.png | `Special:FilePath/Iran_Air_logo.svg?width=128` | 2026-10-03 |
| W5.png | `Special:FilePath/Mahan_Air_Logo.svg?width=128` | 2026-10-03 |
| EP.png | `Special:FilePath/Iran_Aseman_Airlines_logo.png?width=128` | 2026-10-03 |
| B9.png | `Special:FilePath/Iran_Airtour_Logo_Main.png?width=128` | 2026-10-03 |
| YK.png | `Special:FilePath/Kish_Airlines_logo.png?width=128` | 2026-10-03 |
| NV.png | `Special:FilePath/Zagros_Airlines_logo.png?width=128` | 2026-10-03 |
| IS.png | `Special:FilePath/Sepehran_Airlines_Logo.svg?width=128` | 2026-10-03 |
| ZV.png | byte-copy of NV.png — FlyToday's live inventory reports Zagros as IATA **ZV** (verified in `additionalData.airlines`), so the mark ships under both codes | 2026-10-03 |

Live-inventory carriers seen on THR→KIH with **no fetchable logo** (color chip
fallback, same as QB): TKN «فلای‌کیش» — no Wikipedia file; MEH «مهر» — no
Wikipedia file.

QB (قشم‌ایر): no logo file exists on en/fa Wikipedia and qeshmair.com is
unreachable from the dev network — renders the colored-initial chip fallback
(`templates/flights/partials/logo_tile.html`). Add `QB.png` + an AIRLINES entry whenever a source
is available; nothing else changes.

## Agencies (`agencies/<key>` — logo assets from each brand's own site)

| File | Source | Fetched |
|---|---|---|
| alibaba.png | https://www.alibaba.ir/logo.png (og:image, «لوگوی علی‌بابا») | 2026-10-03 |
| flytoday.svg | https://www.flytoday.ir → cdn-a.cdnfl2.ir .../white-labels/flytoday/images/logo.svg | 2026-10-03 |
| mrbilit.png | https://mrbilit.com/img/mrbilit-logo-with-name.png | 2026-10-03 |
| snapptrip.png | https://snapptrip.com → store.snapptrip.com .../logo.12xjc4xr19yoa.png | 2026-10-03 |

iranbilit: site unreachable/flaky from the dev network — colored-initial
fallback, same as QB.
