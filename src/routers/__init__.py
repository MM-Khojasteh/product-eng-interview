"""HTTP surface — one module per page family (crm-monitoring routers/ pattern):
pages (home + content), search (results surface), flight (PDP + compare modal).

Routers never import each other; they depend on the domain modules only.
"""
