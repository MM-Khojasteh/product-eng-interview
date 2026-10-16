"""Torob Travel BFF — FastAPI + Jinja2 + HTMX flight meta-search frontend.

Package layout mirrors the crm-monitoring reference project's src/ conventions:
a thin root shim (main.py at the project root) → src.main app assembly, one
module per concern, and routers/ with one file per surface. Importing this
package has NO side effects — app assembly lives in src.main.
"""
