"""LLM-powered extraction layer for Route 1 (long-tail venue scanning).

Public surface — see app/extractors/llm_extractor.py for the implementation.
Stays out of the import path of the runtime web app: nothing here is loaded
at request time. The extractor is invoked from collectors / CLI tools that
run the LLM-driven scans on an explicit schedule.
"""
from app.extractors.llm_extractor import (  # noqa: F401
    extract,
    extract_auto,
    resolve_template_urls,
    ExtractionResult,
    ExtractorUnconfigured,
)
from app.extractors.discovery import (  # noqa: F401
    discover_via_gemini,
    DiscoveryError,
    looks_like_event_listing,
)
from app.extractors.discovery_cse import (  # noqa: F401
    cse_search,
    discover_via_cse,
    filter_candidates_via_llm,
    discover_via_cse_pipeline,
    CseHit,
)
