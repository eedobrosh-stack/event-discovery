"""LLM-powered extraction layer for Route 1 (long-tail venue scanning).

Public surface — see app/extractors/llm_extractor.py for the implementation.
Stays out of the import path of the runtime web app: nothing here is loaded
at request time. The extractor is invoked from collectors / CLI tools that
run the LLM-driven scans on an explicit schedule.
"""
from app.extractors.llm_extractor import (  # noqa: F401
    extract,
    ExtractionResult,
    ExtractorUnconfigured,
)
