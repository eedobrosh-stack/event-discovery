"""Route 3 — recipe-driven extraction engine.

    schema.py     validate a recipe document, domain helpers
    fetch.py      polite HTTP with budget / delay / robots
    parse.py      response → list[dict] for the 4 parse kinds (+ pagination)
    normalize.py  dict → RawEvent (dates, ids, urls, defaults)
    runner.py     glue: run one recipe (dry or persist), health bookkeeping
"""
from app.services.recipes.runner import (  # noqa: F401
    run_recipe, RunResult, execute_recipe_row, persist_result,
    result_to_payload, result_from_payload,
)
from app.services.recipes.schema import validate_recipe, registered_domain  # noqa: F401
