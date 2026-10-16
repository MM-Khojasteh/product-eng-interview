# POI photos — provenance

Bundled locally on purpose (same doctrine as `static/logos/SOURCES.md`):
the app is offline-first and Iran-network reality makes runtime Wikimedia
hotlinks unreliable. All photos come from Wikimedia Commons under free
licenses (CC BY-SA / CC0 / PD), redistributed verbatim with the per-file
attribution below — which the UI also surfaces per photo (tile tooltip +
the «اعتبار تصاویر» details on /itinerary/{city}). Non-free files are
skipped by the script and render the fallback tile instead.

Fetched once by `scripts/enrich_poi_media.py` (2026-10-04); re-run to refresh
(then commit the JSON, the images and this file together).

| File | POI | Commons file page | Artist | License | Fetched |
|---|---|---|---|---|---|
| greek-ship.jpg | کشتی یونانی | https://commons.wikimedia.org/wiki/File:Greek_Ship.jpg | Amin Noubahar | CC BY-SA 4.0 | 2026-10-04 |
| harireh.jpg | شهر باستانی حریره | https://commons.wikimedia.org/wiki/File:Harireh02.jpg | Kamix | CC BY-SA 3.0 | 2026-10-04 |
| water-sports-pier.jpg | اسکله تفریحی کیش | https://commons.wikimedia.org/wiki/File:021127-TheGrandPier-Kish-IMG_9330-2.jpg | Safa.daneshvar | CC0 | 2026-10-04 |
| ocean-water-park.jpg | پارک آبی اوشن | https://commons.wikimedia.org/wiki/File:Anaconda_2.jpg | Stephan.dedalus | CC BY-SA 4.0 | 2026-10-04 |
| coral-beach.jpg | ساحل مرجانی | https://commons.wikimedia.org/wiki/File:Persian_gulf,kish_island,Coral_Coast%D8%B3%D8%A7%D8%AD%D9%84_%D9%85%D8%B1%D8%AC%D8%A7%D9%86_%D8%AC%D8%B2%DB%8C%D8%B1%D9%87_%DA%A9%DB%8C%D8%B4_-_panoramio.jpg | mohsen barazandeh ah… | CC BY-SA 3.0 | 2026-10-04 |
| dolphin-park.jpg | دلفیناریوم کیش | https://commons.wikimedia.org/wiki/File:Kish_Island_Dolphin_park.jpg | Ivan Mlinaric from Slovenia | CC BY 2.0 | 2026-10-04 |
| bird-garden.jpg | باغ پرندگان کیش | https://commons.wikimedia.org/wiki/File:Kish_Island_Bird_Garden,_Persian_Gulf,_2014.jpg | PersianDutchNetwork | CC BY-SA 3.0 | 2026-10-04 |
