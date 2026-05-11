// Selected type/performer filters: [{kind, value, badge}]
let selectedTypeFilters = [];

function formatPrice(amount, currency) {
    if (!amount) return "-";
    const code = (currency || "USD").toUpperCase();
    try {
        return new Intl.NumberFormat("en-US", {
            style: "currency",
            currency: code,
            minimumFractionDigits: 0,
            maximumFractionDigits: 0,
        }).format(Math.round(amount));
    } catch {
        // Fallback for unrecognised currency codes
        return `${code} ${Math.round(amount)}`;
    }
}
let renderTypeChips = null; // set by setupTypeAutocomplete, used by artist click handler
let offset = 0;
const LIMIT = 50;

// ── Analytics ──────────────────────────────────────────────────────────
// Sends a custom event to GA4. Uses gtag.js when available (the direct
// GA4 path loaded in the head); falls back to a dataLayer push so the
// helper still works if the page is ever switched back to GTM. No-op
// gracefully if neither is loaded (e.g. blocked by an ad blocker).
function pushAnalytics(eventName, props = {}) {
    // Drop null / undefined / "" so GA reports don't fill with empties.
    const clean = Object.fromEntries(
        Object.entries(props).filter(([_, v]) => v !== null && v !== undefined && v !== "")
    );
    if (typeof window.gtag === "function") {
        window.gtag("event", eventName, clean);
    } else if (window.dataLayer) {
        window.dataLayer.push({ event: eventName, ...clean });
    }
}

// Sortable signature of which chip kinds are present in the current filter
// set. Lets GA bucket searches as "artist", "artist+genre", "genre+format"
// etc. without exploding cardinality on individual chip values.
function _chipKindsSignature() {
    const kinds = new Set(selectedTypeFilters.map(f => f.kind));
    return [...kinds].sort().join("+") || "none";
}

document.addEventListener("DOMContentLoaded", async () => {
    // Set default date range: today → today + 30 days
    const today = new Date();
    const future = new Date(today);
    future.setDate(future.getDate() + 30);
    const fmt = d => d.toISOString().split("T")[0];
    document.getElementById("start-date").value = fmt(today);
    document.getElementById("end-date").value = fmt(future);

    // Filter source priority:
    //   1. URL query string  — pasted/shared link, always wins
    //   2. sessionStorage    — homepage-→-results handoff (legacy path,
    //                          preserved for back-compat with the existing
    //                          home.js navigation flow)
    //   3. defaults          — global view, no chips
    //
    // Either source suppresses geo-detection (the user made an explicit
    // choice — including "Global"/blank).
    const urlParams = new URLSearchParams(window.location.search);
    const fromURL = Array.from(urlParams.keys()).length > 0;

    const homeSearch = !fromURL
        ? JSON.parse(sessionStorage.getItem("supercaly_search") || "null")
        : null;
    if (homeSearch) sessionStorage.removeItem("supercaly_search");

    if (fromURL || homeSearch?._fromHome) window._citySetFromParams = true;

    setupTypeAutocomplete();
    await loadCities();
    bindEvents();

    // Apply filters
    if (fromURL) {
        applyFiltersFromURL(urlParams);
    } else if (homeSearch?.typeValue) {
        selectedTypeFilters.push({
            kind:  homeSearch.typeKind  || "freetext",
            value: homeSearch.typeValue,
            badge: homeSearch.typeBadge || "Search",
        });
        if (renderTypeChips) renderTypeChips();
    }
    if (!fromURL && homeSearch?.cityId && homeSearch?.cityLabel) {
        document.getElementById("city-input").value = homeSearch.cityLabel;
        document.getElementById("city-id").value    = homeSearch.cityId;
        updateCityClearBtn();
    }

    await searchEvents();

    // ── Compact / search-mode toggle ──────────────────────────────────────────
    // After a search: compact bar is shown, filter panel is hidden.
    // Clicking the compact bar: filter panel slides back in, compact bar hides.

    const compactBar = document.getElementById("compact-search");
    const compactPill = document.getElementById("compact-pill");

    // Only the pill opens the filter panel — buttons handle their own clicks
    compactPill.addEventListener("click", showSearchMode);
    compactPill.addEventListener("keydown", e => {
        if (e.key === "Enter" || e.key === " ") { e.preventDefault(); showSearchMode(); }
    });

    // Wire compact export buttons to the same handlers as the full-panel ones
    document.getElementById("compact-ics-btn").addEventListener("click", e => { e.stopPropagation(); exportICS(); });
    document.getElementById("compact-csv-btn").addEventListener("click", e => { e.stopPropagation(); exportCSV(); });
    document.getElementById("compact-subscribe-btn").addEventListener("click", e => { e.stopPropagation(); openSubscribeModal(); });
    document.getElementById("compact-share-btn").addEventListener("click", e => { e.stopPropagation(); copyShareableLink(e.currentTarget); });

    // Escape while filter panel is open → back to compact mode (if results exist)
    document.addEventListener("keydown", e => {
        if (e.key === "Escape") {
            const filters = document.querySelector(".filters");
            if (!filters.classList.contains("search-hidden") &&
                document.getElementById("events-body").children.length > 0) {
                showCompactMode();
            }
        }
    });

    // Handle return from Google OAuth
    const params = new URLSearchParams(window.location.search);
    if (params.get("sheets_auth") === "success") {
        alert("Google Sheets authorized! Click 'Export to Google Sheet' to export.");
        window.history.replaceState({}, "", "/");
    } else if (params.get("auth_error")) {
        alert("Google authorization failed: " + params.get("auth_error"));
        window.history.replaceState({}, "", "/");
    }
});

function setupTypeAutocomplete() {
    const input   = document.getElementById("type-search-input");
    const list    = document.getElementById("type-suggestions");
    const chipsEl = document.getElementById("type-chips");
    let activeIdx = -1;
    let debounceTimer = null;
    let suggestController = null;  // AbortController for the in-flight /api/suggestions fetch

    // Client-side response cache. Re-typed queries (e.g. user types
    // "ja" → "jaz" → backspaces to "ja" again) return instantly with
    // no network. TTL matches the server cache (5 min) so we don't
    // serve stale results across long-lived sessions.
    const suggestionsCache = new Map();   // q → {data, ts}
    const SUGGESTIONS_CACHE_TTL_MS = 5 * 60 * 1000;
    const SUGGESTIONS_CACHE_MAX = 100;
    function _cacheGet(q) {
        const e = suggestionsCache.get(q);
        if (!e) return null;
        if (Date.now() - e.ts > SUGGESTIONS_CACHE_TTL_MS) {
            suggestionsCache.delete(q);
            return null;
        }
        return e.data;
    }
    function _cacheSet(q, data) {
        suggestionsCache.set(q, { data, ts: Date.now() });
        if (suggestionsCache.size > SUGGESTIONS_CACHE_MAX) {
            // Drop oldest insertion (Map preserves insertion order).
            const oldest = suggestionsCache.keys().next().value;
            suggestionsCache.delete(oldest);
        }
    }

    /**
     * Derive results for `q` from a longer-prefix cached result, when
     * possible. When a user types "jazz" → "jazzy" → "jazzye" the
     * "jazzye" result is always a SUBSET of "jazzy" — same rule path
     * (word-start, length-aware) — so we can filter the cached parent
     * locally instead of fetching.
     *
     * Subset invariant only holds when parent and child use the SAME
     * matching rule. The rule changes at length 4 (single-word):
     *   • len < 4  → whole-word match (strict)
     *   • len >= 4 → word-start match (looser)
     * So "cla" (whole-word) → 0 hits; "clas" (word-start) → 12 hits.
     * Filtering "cla"'s empty set for "clas" would return empty —
     * a false negative.
     *
     * Guard: only derive when parent length >= 4. That's the safe
     * floor — the matching rule doesn't change for any longer suffix.
     * We also bail when the cached parent hit the server's 12-item
     * cap, since the child's true result could include items the
     * parent cut off.
     */
    const SERVER_LIMIT = 12;
    const SAFE_DERIVE_FLOOR = 4;
    function _deriveFromParent(q) {
        const qLower = q.toLowerCase();
        for (let n = q.length - 1; n >= SAFE_DERIVE_FLOOR; n--) {
            const parent = q.slice(0, n);
            const cachedParent = _cacheGet(parent);
            if (!cachedParent) continue;
            if (cachedParent.length >= SERVER_LIMIT) {
                // Parent was capped by the server limit — child set
                // could include items the parent cut off. Bail.
                return null;
            }
            // Filter cached parent by substring containment — safe
            // over-approximation: the server already filtered to
            // legitimate matches at the parent length, so any child
            // match must contain the (longer) child query somewhere
            // in the value/label.
            return cachedParent.filter(item => {
                const hay = ((item.value || "") + " " + (item.label || "")).toLowerCase();
                return hay.includes(qLower);
            });
        }
        return null;
    }

    renderTypeChips = function() { renderChips(); };

    function renderChips() {
        chipsEl.innerHTML = selectedTypeFilters.map((f, i) => `
            <span class="type-chip type-chip--${f.kind}">
                <span class="type-chip__badge">${f.badge}</span>
                ${esc(f.value)}
                <button class="type-chip__remove" data-idx="${i}" aria-label="Remove">×</button>
            </span>
        `).join("");
        chipsEl.querySelectorAll(".type-chip__remove").forEach(btn => {
            btn.addEventListener("click", e => {
                e.stopPropagation();
                const idx = parseInt(btn.dataset.idx);
                selectedTypeFilters.splice(idx, 1);
                renderChips();
            });
        });
    }

    function showSuggestions(items) {
        if (!items.length) { list.hidden = true; return; }
        activeIdx = -1;
        list.innerHTML = items.map((item, i) =>
            `<li data-idx="${i}" data-kind="${item.kind}" data-value="${esc(item.value)}" data-badge="${item.badge}">
                <span class="sugg-badge sugg-badge--${item.kind}">${item.badge}</span>
                ${esc(item.label)}
            </li>`
        ).join("");
        list.hidden = false;
    }

    function selectItem(li) {
        const kind  = li.dataset.kind;
        const value = li.dataset.value;
        const badge = li.dataset.badge;
        const label = li.textContent.trim().replace(/^City\s*/, "");

        // City clicks aren't type filters — they navigate to that city's
        // calendar. Push the value (a numeric cityId) into the city filter
        // so the existing city_ids query path picks it up.
        if (kind === "city") {
            const cityInput  = document.getElementById("city-input");
            const cityHidden = document.getElementById("city-id");
            if (cityInput && cityHidden) {
                cityInput.value  = label;
                cityHidden.value = value;
                if (typeof updateCityClearBtn === "function") updateCityClearBtn();
            }
            input.value = "";
            list.hidden = true;
            list.innerHTML = "";
            activeIdx = -1;
            offset = 0;
            document.getElementById("events-body").innerHTML = "";
            searchEvents();
            return;
        }

        // Avoid duplicates
        if (!selectedTypeFilters.find(f => f.kind === kind && f.value === value)) {
            selectedTypeFilters.push({ kind, value, badge });
            renderChips();
        }
        input.value = "";
        list.hidden = true;
        list.innerHTML = "";
        activeIdx = -1;
    }

    input.addEventListener("input", () => {
        const q = input.value.trim();
        clearTimeout(debounceTimer);
        // Cancel any in-flight /api/suggestions request from a previous keystroke;
        // otherwise a slow earlier query can resolve after a faster later one
        // and overwrite suggestions, plus it wastes a worker slot.
        if (suggestController) {
            suggestController.abort();
            suggestController = null;
        }
        if (q.length < 2) { list.hidden = true; return; }

        // Cache hit — render instantly, skip the debounce + network round-trip.
        const cached = _cacheGet(q);
        if (cached) {
            showSuggestions(cached);
            return;
        }

        // Forward-typing optimisation: if a shorter prefix is cached
        // and wasn't truncated by the server limit, derive this query's
        // result locally. Saves the fetch entirely. We still issue the
        // fetch in the background to refresh the cache for the next
        // visit (in case the derivation missed an edge case).
        const derived = _deriveFromParent(q);
        if (derived !== null) {
            _cacheSet(q, derived);  // serve from cache on re-visit too
            showSuggestions(derived);
            return;
        }

        debounceTimer = setTimeout(async () => {
            suggestController = new AbortController();
            const signal = suggestController.signal;
            try {
                const resp = await fetch(
                    `/api/suggestions?q=${encodeURIComponent(q)}`,
                    { signal },
                );
                const items = await resp.json();
                _cacheSet(q, items);
                showSuggestions(items);
            } catch (err) {
                // AbortError = a newer keystroke superseded this one; ignore.
                if (err && err.name !== "AbortError") {
                    console.warn("suggestions fetch failed", err);
                }
            }
        }, 200);
    });

    // mousedown prevents input blur before click fires
    list.addEventListener("mousedown", e => e.preventDefault());
    list.addEventListener("click", e => {
        const li = e.target.closest("li");
        if (li) selectItem(li);
    });

    input.addEventListener("keydown", e => {
        const items = list.querySelectorAll("li");
        if (!items.length) return;
        if (e.key === "ArrowDown") {
            e.preventDefault();
            activeIdx = Math.min(activeIdx + 1, items.length - 1);
        } else if (e.key === "ArrowUp") {
            e.preventDefault();
            activeIdx = Math.max(activeIdx - 1, 0);
        } else if (e.key === "Enter") {
            e.preventDefault();
            if (activeIdx >= 0) {
                selectItem(items[activeIdx]);
            } else {
                // Commit raw typed text as a free-text chip
                const raw = input.value.trim();
                if (raw.length >= 1) {
                    if (!selectedTypeFilters.find(f => f.value === raw)) {
                        selectedTypeFilters.push({ kind: "freetext", value: raw, badge: "Search" });
                        renderChips();
                    }
                    input.value = "";
                    list.hidden = true;
                }
            }
            return;
        } else if (e.key === "Escape") {
            list.hidden = true; return;
        } else if (e.key === "Backspace" && input.value === "" && selectedTypeFilters.length) {
            selectedTypeFilters.pop();
            renderChips();
            return;
        }
        items.forEach((li, i) => li.classList.toggle("active", i === activeIdx));
        if (activeIdx >= 0) items[activeIdx].scrollIntoView({ block: "nearest" });
    });

    // Hide dropdown when focus leaves the whole autocomplete wrap
    const wrap = document.querySelector(".type-autocomplete-wrap");
    wrap.addEventListener("focusout", e => {
        if (!wrap.contains(e.relatedTarget)) {
            setTimeout(() => { list.hidden = true; }, 100);
        }
    });

    // Also hide on outside click
    document.addEventListener("click", e => {
        if (!wrap.contains(e.target)) {
            list.hidden = true;
        }
    });
}

let allCities = [];
let allMetroAreas = [];
let allCountries = [];
let allStates = [];

// US state names — kept in lockstep with app/api/_us_states.py.US_STATE_NAMES.
// Used to disambiguate cities whose name overlaps a state name (e.g. the
// city "New York" → display as "New York City"). The list is small enough
// to inline rather than fetch on every page load.
const US_STATE_NAMES_SET = new Set([
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado",
    "Connecticut","Delaware","District of Columbia","Florida","Georgia",
    "Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky",
    "Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota",
    "Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire",
    "New Jersey","New Mexico","New York","North Carolina","North Dakota",
    "Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island","South Carolina",
    "South Dakota","Tennessee","Texas","Utah","Vermont","Virginia",
    "Washington","West Virginia","Wisconsin","Wyoming",
]);

/**
 * Format a city's user-facing label.
 *
 * For US cities:
 *   - Append " City" to the displayed name when the bare name collides
 *     with a state name (e.g. the actual NYC row stored as name="New York"
 *     in NY → "New York City, New York State").
 *   - Qualifier is "{state} State" instead of "{country}" — the state is
 *     finer-grained and US-specific, so country becomes redundant.
 * For everything else: existing "{name}, {country}" format.
 */
function formatCityLabel(c) {
    if (!c) return "";
    if (c.country === "United States" && c.state) {
        const dispName = US_STATE_NAMES_SET.has(c.name) ? `${c.name} City` : c.name;
        return `${dispName}, ${c.state} State`;
    }
    return `${c.name}, ${c.country}`;
}

async function loadCities() {
    const [citiesResp, metroResp, countriesResp, statesResp] = await Promise.all([
        fetch("/api/cities"),
        fetch("/api/metro-areas"),
        fetch("/api/cities/countries"),
        fetch("/api/cities/states"),
    ]);
    allCities = await citiesResp.json();
    allMetroAreas = (await metroResp.json()).map(m => ({
        ...m,
        _isMeta: true,
        label: `🗺 ${m.name} (${m.city_count} cities)`,
    }));
    allCountries = (await countriesResp.json()).map(c => ({
        ...c,
        _isCountry: true,
        id: `COUNTRY:${c.name}`,
        label: `🌐 ${c.name} (${c.city_count} cities)`,
    }));

    // States behave like metros at the filter layer — their selection
    // value is the comma-joined city_ids of all cities in that state, so
    // the existing city-id/multi-id query path handles them transparently.
    // Build state → city_ids mapping client-side from the cities list,
    // since the cities endpoint normalises state codes to canonical names
    // (matching the /states endpoint output).
    const stateCityIds = {};
    for (const c of allCities) {
        if (c.country === "United States" && c.state) {
            (stateCityIds[c.state] ||= []).push(c.id);
        }
    }
    allStates = (await statesResp.json()).map(s => ({
        ...s,
        _isState: true,
        _isMeta: true,  // re-use the multi-city-id selection plumbing
        city_ids: (stateCityIds[s.name] || []).join(","),
        label: `🏛 ${s.name} State (${s.city_count} cities)`,
    }));

    setupCityAutocomplete();
    detectUserCity();
}

async function detectUserCity() {
    // Skip if city was pre-filled from homepage URL params
    if (window._citySetFromParams) return;
    // Try IP-based detection first (no permission needed)
    try {
        const r = await fetch("https://ipapi.co/json/");
        const geo = await r.json();
        // Re-check after async fetch — params may have been applied while we were waiting
        if (window._citySetFromParams) return;
        const cityName = geo.city || "";
        const countryName = geo.country_name || "";
        console.log("[GeoDetect] IP-based:", cityName, countryName);
        if (cityName && applyCityMatch(cityName, countryName)) return;
    } catch (e) {
        console.warn("[GeoDetect] IP lookup failed:", e);
    }

    // Fallback: precise GPS (requires browser permission)
    if (!navigator.geolocation) return;
    navigator.geolocation.getCurrentPosition(async pos => {
        try {
            const { latitude: lat, longitude: lon } = pos.coords;
            const r = await fetch(
                `https://api.bigdatacloud.net/data/reverse-geocode-client?latitude=${lat}&longitude=${lon}&localityLanguage=en`
            );
            const geo = await r.json();
            const cityName = geo.city || geo.locality || "";
            const countryName = geo.countryName || "";
            console.log("[GeoDetect] GPS-based:", cityName, countryName);
            applyCityMatch(cityName, countryName);
        } catch (e) {
            console.warn("[GeoDetect] GPS reverse-geocode failed:", e);
        }
    }, err => console.warn("[GeoDetect] GPS permission denied:", err));
}

function applyCityMatch(cityName, countryName) {
    const q = cityName.toLowerCase().trim();
    const qCountry = countryName.toLowerCase().trim();

    // Score-based matching: exact name + country > exact name > partial
    let best = null, bestScore = 0;
    for (const c of allCities) {
        const cName    = c.name.toLowerCase();
        const cCountry = c.country.toLowerCase();
        let score = 0;
        if (cName === q)                        score += 100;
        else if (cName.includes(q))             score += 50;
        else if (q.includes(cName))             score += 30;
        if (score > 0 && cCountry === qCountry) score += 20;
        if (score > bestScore) { bestScore = score; best = c; }
    }

    if (best && bestScore >= 30) {
        console.log("[GeoDetect] Matched:", best.name, best.country, "score:", bestScore);
        document.getElementById("city-input").value = formatCityLabel(best);
        document.getElementById("city-id").value    = best.id;
        updateCityClearBtn();
        offset = 0;
        document.getElementById("events-body").innerHTML = "";
        searchEvents();
        return true;
    }
    console.warn("[GeoDetect] No match found for:", cityName, countryName);
    return false;
}

const GLOBAL_CITY = { id: "", name: "🌍 Global", country: "All Cities", label: "🌍 Global — All Cities" };

function renderCityList(matches) {
    const list = document.getElementById("city-suggestions");
    list.innerHTML = matches.map(c => {
        const id    = c._isMeta ? c.city_ids : (c.id || "");  // country uses COUNTRY: prefix
        const label = c.label || formatCityLabel(c);
        // CSS classes by row kind. State rows reuse the metro selection
        // mechanism (_isMeta=true) but get their own class so styling
        // can distinguish them visually if desired.
        let cls = "";
        if (c._isState)        cls = " class=\"state-option\"";
        else if (c._isMeta)    cls = " class=\"metro-option\"";
        else if (c._isCountry) cls = " class=\"country-option\"";
        return `<li data-id="${id}" data-label="${label}"${cls}>${label}</li>`;
    }).join("");
    list.hidden = matches.length === 0;
}

function isMetroSelected() {
    const v = document.getElementById("city-id")?.value || "";
    return v.includes(",") && !v.startsWith("COUNTRY:");
}

function isCountrySelected() {
    return (document.getElementById("city-id")?.value || "").startsWith("COUNTRY:");
}

function getSelectedCountry() {
    const v = document.getElementById("city-id")?.value || "";
    return v.startsWith("COUNTRY:") ? v.slice(8) : null;
}

function updateCityClearBtn() {
    const input  = document.getElementById("city-input");
    const hidden = document.getElementById("city-id");
    const btn    = document.getElementById("city-clear");
    if (!btn) return;
    // Show clear button whenever there's any text in the city field
    btn.hidden = input.value.trim().length === 0;
}

function setupCityAutocomplete() {
    const input  = document.getElementById("city-input");
    const list   = document.getElementById("city-suggestions");
    const hidden = document.getElementById("city-id");
    const clearBtn = document.getElementById("city-clear");
    let activeIdx = -1;

    // Clear button — reset to Global (no filter)
    if (clearBtn) {
        clearBtn.addEventListener("click", () => {
            input.value  = "";
            hidden.value = "";
            clearBtn.hidden = true;
            list.hidden = true;
            searchEvents();   // re-run search without city filter
        });
    }

    input.addEventListener("focus", () => {
        const q = input.value.trim().toLowerCase();
        if (q.length < 3) {
            renderCityList([GLOBAL_CITY]);
            activeIdx = -1;
        }
    });

    input.addEventListener("input", () => {
        const q = input.value.trim().toLowerCase();
        hidden.value = "";           // clear selection when user types
        activeIdx = -1;
        updateCityClearBtn();

        if (q.length < 3) {
            renderCityList([GLOBAL_CITY]);
            return;
        }

        const metroMatches = allMetroAreas.filter(m =>
            m.name.toLowerCase().includes(q) ||
            m.city_names.some(cn => cn.toLowerCase().includes(q))
        ).slice(0, 3);

        const countryMatches = allCountries.filter(c =>
            c.name.toLowerCase().includes(q)
        ).slice(0, 3);

        // States: match against the bare name OR "<name> state" so users
        // can type either "California" or "California State" and land
        // on the same row.
        const stateMatches = allStates.filter(s => {
            const n = s.name.toLowerCase();
            return n.includes(q) || `${n} state`.includes(q);
        }).slice(0, 3);

        // Cities: match against the user-facing label (so "New York City"
        // matches the disambiguated form, and "California State" doesn't
        // accidentally match a city's qualifier). We then split into
        // two buckets:
        //   • cityMatchesExact — name starts with the query. These are
        //     direct hits ("tel av" → Tel Aviv) and beat the metro
        //     grouping that contains them ("Gush Dan (Tel Aviv Metro)").
        //   • cityMatchesOther — substring matches further down the
        //     label (country, state suffix, mid-name). Stay in the
        //     normal cascade slot.
        const cityMatchesAll = allCities.filter(c =>
            formatCityLabel(c).toLowerCase().includes(q)
        );
        const cityMatchesExact = cityMatchesAll
            .filter(c => c.name.toLowerCase().startsWith(q))
            .slice(0, 3);
        const exactSet = new Set(cityMatchesExact);
        const cityMatchesOther = cityMatchesAll
            .filter(c => !exactSet.has(c))
            .slice(0, 6);

        // Order:
        //   Global → Direct City Hits → Metros → Countries → States →
        //   Other Cities.
        // The direct-hit slot keeps the explicit city the user typed
        // visible at the top while preserving the cascade for all the
        // less-specific matches below.
        const matches = [GLOBAL_CITY, ...cityMatchesExact,
                         ...metroMatches, ...countryMatches,
                         ...stateMatches, ...cityMatchesOther];

        renderCityList(matches);
    });

    list.addEventListener("click", e => {
        const li = e.target.closest("li");
        if (!li) return;
        input.value  = li.dataset.label;
        hidden.value = li.dataset.id;
        list.innerHTML = "";
        list.hidden = true;
        updateCityClearBtn();
    });

    // Keyboard navigation
    input.addEventListener("keydown", e => {
        const items = list.querySelectorAll("li");
        if (!items.length) return;
        if (e.key === "ArrowDown") {
            e.preventDefault();
            activeIdx = Math.min(activeIdx + 1, items.length - 1);
        } else if (e.key === "ArrowUp") {
            e.preventDefault();
            activeIdx = Math.max(activeIdx - 1, 0);
        } else if (e.key === "Enter" && activeIdx >= 0) {
            e.preventDefault();
            items[activeIdx].click();
            return;
        } else if (e.key === "Escape") {
            list.hidden = true; return;
        }
        items.forEach((li, i) => li.classList.toggle("active", i === activeIdx));
        if (activeIdx >= 0) items[activeIdx].scrollIntoView({ block: "nearest" });
    });

    // Close on outside click
    document.addEventListener("click", e => {
        if (!e.target.closest(".city-autocomplete-wrap")) {
            list.hidden = true;
        }
    });
}

function getSelectedCityId() {
    const v = document.getElementById("city-id").value;
    return isCountrySelected() ? "" : v;  // country filter is separate; don't pass as city_ids
}

function bindEvents() {
    document.getElementById("search-btn").addEventListener("click", () => {
        offset = 0;
        document.getElementById("events-body").innerHTML = "";
        searchEvents();
    });

    document.getElementById("export-ics-btn").addEventListener("click", exportICS);
    document.getElementById("export-csv-btn").addEventListener("click", exportCSV);
    document.getElementById("load-more-btn").addEventListener("click", searchEvents);
    document.getElementById("subscribe-btn").addEventListener("click", openSubscribeModal);
    document.getElementById("modal-close-btn").addEventListener("click", closeSubscribeModal);
    document.getElementById("subscribe-modal").addEventListener("click", e => {
        if (e.target === e.currentTarget) closeSubscribeModal();
    });
    document.getElementById("copy-url-btn").addEventListener("click", () => {
        const input = document.getElementById("subscribe-url");
        navigator.clipboard.writeText(input.value).then(() => {
            const btn = document.getElementById("copy-url-btn");
            btn.textContent = "Copied!";
            setTimeout(() => { btn.textContent = "Copy"; }, 2000);
        });
    });

    document.getElementById("events-body").addEventListener("click", e => {
        // Buy / event-title click (track BEFORE navigation — the link's
        // target=_blank means we don't actually leave the page, but firing
        // first is good practice in case browser nav races the analytics
        // request).
        const buyLink = e.target.closest("a.track-buy");
        if (buyLink) {
            pushAnalytics("buy_click", {
                event_name: buyLink.dataset.eventName || null,
                artist:     buyLink.dataset.artist || null,
                venue:      buyLink.dataset.venue || null,
                genre:      buyLink.dataset.genre || null,
                city:       buyLink.dataset.city || null,
                country:    buyLink.dataset.country || null,
            });
            // Don't return — the artist-cell delegation below is mutually
            // exclusive (Buy/title links don't have data-artist).
        }

        const cell = e.target.closest("[data-artist]");
        if (!cell) return;
        const artist = cell.dataset.artist;
        selectedTypeFilters = [{ kind: "freetext", value: artist, badge: "Search" }];
        if (renderTypeChips) renderTypeChips();
        offset = 0;
        document.getElementById("events-body").innerHTML = "";
        searchEvents();
    });
}

// ── URL ↔ filter-state plumbing ───────────────────────────────────────────
// The /results.html URL is shareable: each filter knob is reflected as a
// query param matching the /api/events contract (genres, artist_exact,
// type_search, city_ids|country, start_date, end_date, search). The
// shareable params are written back via history.replaceState in
// searchEvents() on every first-page load. Read here on init.

function resolveCityLabel(cityId) {
    // Turn a stored cityId back into the human-readable string the city
    // input shows. Mirrors the four encodings the home and results UIs
    // produce:
    //   "COUNTRY:Israel"   → country
    //   "42,43,44"         → metro area OR US state (both store as
    //                        comma-joined city_ids — match metro first
    //                        because its labels are more specific
    //                        ("NYC Metro" beats "New York State"); fall
    //                        back to state.)
    //   "42"               → single city
    if (!cityId) return "";
    if (cityId.startsWith("COUNTRY:")) {
        const cn = cityId.slice("COUNTRY:".length);
        const c = allCountries.find(x => x.name === cn);
        return c?.label || cn;
    }
    if (cityId.includes(",")) {
        const m = allMetroAreas.find(x => x.city_ids === cityId);
        if (m?.label) return m.label;
        const s = allStates.find(x => x.city_ids === cityId);
        return s?.label || cityId;
    }
    const id = parseInt(cityId, 10);
    if (!Number.isFinite(id)) return cityId;
    const city = allCities.find(c => c.id === id);
    return city ? formatCityLabel(city) : cityId;
}

function applyFiltersFromURL(params) {
    // Reconstruct chips from URL params. Lossy on `type_search` round-trip:
    // the original chip kind (was it event_type? category? venue?) isn't
    // preserved in the URL — only the value. We restore as `freetext` chips,
    // which produces identical filter behavior (the type_search backend path
    // matches across event_type / category / artist / event / venue) and a
    // sensible "Search" badge.
    const pushChips = (paramName, kind, badge) => {
        const v = params.get(paramName);
        if (!v) return;
        for (const term of v.split(",").map(s => s.trim()).filter(Boolean)) {
            selectedTypeFilters.push({ kind, value: term, badge });
        }
    };
    pushChips("genres",       "genre",     "Genre");
    pushChips("artist_exact", "performer", "Artist");
    pushChips("type_search",  "freetext",  "Search");

    // City — country exclusive with city_ids; only one is set at a time.
    const country = params.get("country");
    if (country) {
        const cid = `COUNTRY:${country}`;
        document.getElementById("city-id").value = cid;
        document.getElementById("city-input").value = resolveCityLabel(cid);
    } else {
        const cityIds = params.get("city_ids");
        if (cityIds) {
            document.getElementById("city-id").value = cityIds;
            document.getElementById("city-input").value = resolveCityLabel(cityIds);
        }
    }
    if (typeof updateCityClearBtn === "function") updateCityClearBtn();

    // Dates — these inputs already have today/+30d defaults from before
    // applyFiltersFromURL runs, so we only overwrite when the URL specifies.
    const sd = params.get("start_date");
    if (sd) document.getElementById("start-date").value = sd;
    const ed = params.get("end_date");
    if (ed) document.getElementById("end-date").value = ed;

    // The dedicated `search` text box (separate from chip-based search).
    const s = params.get("search");
    if (s) document.getElementById("search").value = s;

    if (renderTypeChips) renderTypeChips();
}

// ── Share button ───────────────────────────────────────────────────────
// Copies the current URL (which already mirrors all active filters) to
// the clipboard. Works on prod over HTTPS; falls back to a manual
// document.execCommand("copy") on plain HTTP / older browsers.
async function copyShareableLink(btn) {
    const url = window.location.href;
    let ok = false;
    try {
        await navigator.clipboard.writeText(url);
        ok = true;
    } catch (e) {
        // Fallback: hidden textarea + execCommand. Crusty but reliable on
        // contexts where the async clipboard API isn't available.
        try {
            const ta = document.createElement("textarea");
            ta.value = url;
            ta.style.position = "fixed";
            ta.style.opacity = "0";
            document.body.appendChild(ta);
            ta.focus(); ta.select();
            ok = document.execCommand("copy");
            document.body.removeChild(ta);
        } catch (_) {}
    }
    if (!btn) return;
    const originalLabel = btn.querySelector(".btn-label");
    const originalLabelText = originalLabel?.textContent || "";
    if (originalLabel) originalLabel.textContent = ok ? " Copied!" : " Copy failed";
    setTimeout(() => {
        if (originalLabel) originalLabel.textContent = originalLabelText;
    }, 1600);
}

// ── Empty-state + lookahead helpers ────────────────────────────────────
// When a search returns 0 results in the user's chosen window, we don't
// just give up: we re-run the same query with end_date pushed out 2 years
// and, if there are matches in that extended range, adopt them and stretch
// the end_date input to cover. If nothing exists in the extended window
// either, we surface a friendly "no events" message and POST the search
// criteria to /api/events/zero-result so we can spot real catalog gaps.

const _LOOKAHEAD_YEARS = 2;

async function _runLookahead({ typeSearch, artistExact, genres, cityId, country, startDate, search }) {
    const lp = new URLSearchParams();
    if (typeSearch.length)  lp.set("type_search", typeSearch.join(","));
    if (artistExact.length) lp.set("artist_exact", artistExact.join(","));
    if (genres.length)      lp.set("genres", genres.join(","));
    if (cityId)             lp.set("city_ids", cityId);
    if (country)            lp.set("country", country);
    if (startDate)          lp.set("start_date", startDate);
    if (search)             lp.set("search", search);
    // Cap at +N years so we don't pull every event ever scraped.
    const cap = new Date();
    cap.setFullYear(cap.getFullYear() + _LOOKAHEAD_YEARS);
    lp.set("end_date", cap.toISOString().split("T")[0]);
    lp.set("limit", LIMIT);
    lp.set("offset", 0);
    try {
        const r = await fetch(`/api/events?${lp}`);
        if (!r.ok) return null;
        const events = await r.json();
        return { events, params: lp };
    } catch (e) {
        return null;
    }
}

function _logZeroResultSearch(payload) {
    // Best-effort. Don't await — a slow logger shouldn't block the UX.
    fetch("/api/events/zero-result", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
    }).catch(() => {});
}

function _fmtFriendlyDate(iso) {
    if (!iso) return "";
    const [y, m, d] = iso.split("-");
    const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    return `${months[parseInt(m,10)-1]} ${parseInt(d,10)}, ${y}`;
}

function clearSearchNotice() {
    const el = document.getElementById("search-notice");
    if (!el) return;
    el.hidden = true;
    el.className = "";
    el.innerHTML = "";
}

function renderSearchNotice(flavor, html) {
    // flavor: "info" (extended success) or "empty" (no results at all)
    const el = document.getElementById("search-notice");
    if (!el) return;
    el.className = `notice--${flavor}`;
    el.innerHTML = html;
    el.hidden = false;
}

function _cleanPlaceLabel() {
    // The city-input shows things like "🌐 France (407 cities)" /
    // "🗺 Greater London (5 cities)" / "Tel Aviv, Israel". For prose use,
    // strip the leading emoji and the trailing "(N cities)" suffix so the
    // message reads "in France" rather than "in 🌐 France (407 cities)".
    // Returns null for global (no city selected).
    const raw = (document.getElementById("city-input")?.value || "").trim();
    if (!raw) return null;
    return raw
        .replace(/^[🌐🗺]\s*/, "")
        .replace(/\s*\(\d+\s+cit(?:y|ies)\)$/, "")
        .trim() || null;
}

function renderEmptyStateMessage({ artistExact, genres, typeSearch, search, startDate, endDate }) {
    // Describe the search in terms the user used. Pluralize the noun based
    // on the dominant chip kind so the message reads naturally:
    //   1 artist chip → "artist events"
    //   1 genre chip  → "genre events"
    //   anything else → "events"
    const allChips = [...artistExact, ...genres, ...typeSearch];
    const free = (search || "").trim();
    const terms = allChips.length ? allChips : (free ? [free] : []);

    let kindWord = "events";
    if (artistExact.length === 1 && !genres.length && !typeSearch.length) kindWord = "artist events";
    else if (genres.length === 1 && !artistExact.length && !typeSearch.length) kindWord = `${genres[0]} events`;

    const termsHtml = terms.length
        ? `for <span class="notice__strong">${terms.map(esc).join(", ")}</span> `
        : "";
    const dateRange = (startDate && endDate)
        ? `between <span class="notice__strong">${_fmtFriendlyDate(startDate)}</span> ` +
          `and <span class="notice__strong">${_fmtFriendlyDate(endDate)}</span>`
        : "in this date range";

    // "in <Place>" tail only when scoped to a specific city/metro/country —
    // omitted for Global searches (the date range is the only scope).
    const place = _cleanPlaceLabel();
    const placeHtml = place
        ? ` in <span class="notice__strong">${esc(place)}</span>`
        : "";

    renderSearchNotice("empty",
        `No ${kindWord} ${termsHtml}${dateRange}${placeHtml}.`);
}

function getFilters() {
    // Partition chips by kind:
    //   "performer" → strict exact-match on artist_name (artist_exact)
    //   "genre"     → parent-genre filter (genres) — backend expands to all
    //                 sub-genres' artists. NEVER folded into type_search; the
    //                 fuzzy text path would match "Rock" against any event
    //                 with the word "rock" in its name, which defeats the
    //                 whole point of the curated taxonomy.
    //   everything else → looser word-aware type_search bucket.
    const artistExact = selectedTypeFilters
        .filter(f => f.kind === "performer")
        .map(f => f.value);
    const genres = selectedTypeFilters
        .filter(f => f.kind === "genre")
        .map(f => f.value);
    const chipTerms = selectedTypeFilters
        .filter(f => f.kind !== "performer" && f.kind !== "genre")
        .map(f => f.value);
    // Also pick up any uncommitted text still in the input (≥3 chars)
    const rawText = document.getElementById("type-search-input").value.trim();
    if (rawText.length >= 3 && !chipTerms.includes(rawText)) chipTerms.push(rawText);

    const cityId    = getSelectedCityId();
    const startDate = document.getElementById("start-date").value;
    const endDate   = document.getElementById("end-date").value;
    const search    = document.getElementById("search").value;
    return { typeSearch: chipTerms, artistExact, genres, cityId, startDate, endDate, search };
}

let totalEvents = null; // total matching count from /api/events/count

// Cross-page dedup registry — see renderResults below. Reset on every
// first-page render, accumulates as Load More appends pages.
const _renderedNameKeys = new Set();
const _renderedArtistKeys = new Set();
// Running count of rows filtered out by the dedup pass. Subtracted
// from the server-reported total in updateStats so the displayed
// total reflects what the user can actually see — otherwise the
// gap (e.g. "Showing 40 of 44") looks like a pagination bug.
let _dedupedDropped = 0;

async function searchEvents() {
    const isFirstPage = offset === 0;   // capture before any mutation
    const { typeSearch, artistExact, genres, cityId, startDate, endDate, search } = getFilters();
    const country = getSelectedCountry();
    const params = new URLSearchParams();
    if (typeSearch.length) params.set("type_search", typeSearch.join(","));
    if (artistExact.length) params.set("artist_exact", artistExact.join(","));
    if (genres.length) params.set("genres", genres.join(","));
    if (cityId) params.set("city_ids", cityId);
    if (country) params.set("country", country);
    if (startDate) params.set("start_date", startDate);
    if (endDate) params.set("end_date", endDate);
    if (search) params.set("search", search);

    // Sync the current filter set into the URL bar so the view is shareable.
    // Only on first-page loads (pagination doesn't change filters and would
    // be a no-op). replaceState (not push) keeps the back button going home,
    // not stepping through every chip change.
    // IMPORTANT: write before adding limit/offset, which don't belong in the
    // shareable URL.
    if (isFirstPage) {
        const qs = params.toString();
        history.replaceState(null, "", qs ? `?${qs}` : window.location.pathname);
    }

    // Clear any prior empty/extended notice — fresh search starts neutral.
    if (isFirstPage) clearSearchNotice();

    // Fetch total count on the first page
    if (isFirstPage) {
        totalEvents = null;
        fetch(`/api/events/count?${params}`)
            .then(r => r.json())
            .then(({ total }) => {
                totalEvents = total;
                updateStats(document.getElementById("events-body").children.length);
            })
            .catch(() => {});
    }

    params.set("limit", LIMIT);
    params.set("offset", offset);

    const resp = await fetch(`/api/events?${params}`);
    let events = await resp.json();

    // Empty-state handling — only on first page (paginated empties are
    // just "end of results", not a missing-data signal). Two branches:
    //   1. Lookahead succeeds → extend end_date, adopt those events.
    //   2. Lookahead empty → log + render "no events for X" message.
    let extendedTo = null;
    if (isFirstPage && events.length === 0) {
        const filterCriteria = {
            typeSearch, artistExact, genres,
            cityId, country, startDate, search,
        };
        const lookahead = await _runLookahead(filterCriteria);
        if (lookahead && lookahead.events.length > 0) {
            // Found in extended window. Adopt the events and update inputs.
            events = lookahead.events;
            extendedTo = lookahead.events[lookahead.events.length - 1].start_date;
            document.getElementById("end-date").value = extendedTo;
            // Rebuild params using the *actual* extended end_date (not the
            // +2yr lookahead cap we used internally) so the URL bar and the
            // count fetch reflect what the user actually sees.
            const adoptedParams = new URLSearchParams();
            if (typeSearch.length)  adoptedParams.set("type_search", typeSearch.join(","));
            if (artistExact.length) adoptedParams.set("artist_exact", artistExact.join(","));
            if (genres.length)      adoptedParams.set("genres", genres.join(","));
            if (cityId)             adoptedParams.set("city_ids", cityId);
            if (country)            adoptedParams.set("country", country);
            if (startDate)          adoptedParams.set("start_date", startDate);
            adoptedParams.set("end_date", extendedTo);
            if (search)             adoptedParams.set("search", search);
            fetch(`/api/events/count?${adoptedParams}`)
                .then(r => r.json())
                .then(({ total }) => {
                    totalEvents = total;
                    updateStats(document.getElementById("events-body").children.length);
                })
                .catch(() => {});
            history.replaceState(null, "", `?${adoptedParams.toString()}`);
            renderSearchNotice("info",
                `No matches in the original window — extended through ` +
                `<span class="notice__strong">${esc(_fmtFriendlyDate(extendedTo))}</span> ` +
                `to find events.`);
        } else {
            // Truly nothing — log + show empty message + bail out of render.
            _logZeroResultSearch({
                genres: genres.join(",") || null,
                artists: artistExact.join(",") || null,
                type_search: typeSearch.join(",") || null,
                free_search: search || null,
                city_ids: cityId || null,
                country: country || null,
                start_date: startDate || null,
                end_date: endDate || null,
            });
            // Mirror the same signal to GA4 so it shows up alongside other
            // funnel events. Backend log is the source of truth; this is
            // for live dashboards / segments / audiences.
            pushAnalytics("zero_result_search", {
                search_chip_kinds: _chipKindsSignature(),
                search_genres: genres.join(",") || null,
                search_artists: artistExact.join(",") || null,
                search_type: typeSearch.join(",") || null,
                search_free: search || null,
                search_city_id: cityId || null,
                search_country: country || null,
            });
            renderEmptyStateMessage({ artistExact, genres, typeSearch, search, startDate, endDate });
            // Reset paging UI so it doesn't dangle a "Load More" button.
            const tbody = document.getElementById("events-body");
            tbody.innerHTML = "";
            updateStats(0);
            document.getElementById("load-more-btn").style.display = "none";
            if (isFirstPage) showCompactMode();
            return;
        }
    }

    // ── Same-name + same-moment dedup ─────────────────────────────────
    // Backend dedup keys on (start_date, venue_id, identifier). If the
    // same physical venue exists as two rows (Hebrew + English name,
    // address normalization split, etc.), the venue_id differs so the
    // backend bucket misses and the event renders twice. Frontend rule:
    // collapse rows that share (name OR artist_name) + start_date +
    // start_time, picking the first occurrence (already ranked by the
    // API). Registry is cleared on first-page render and carries
    // across Load More so pagination can't re-introduce a dup.
    if (isFirstPage) {
        _renderedNameKeys.clear();
        _renderedArtistKeys.clear();
        _dedupedDropped = 0;
    }
    // Capture BEFORE dedup — pagination needs the API's row count, not
    // the post-filter count. Without this, offset stops advancing and
    // "Load More" disappears when any rows get deduped.
    const apiReturnedCount = events.length;
    events = events.filter(ev => {
        const d = ev.start_date || "";
        const t = ev.start_time || "";
        const nKey = ev.name ? `n|${ev.name.toLowerCase().trim()}|${d}|${t}` : null;
        const aKey = ev.artist_name ? `a|${ev.artist_name.toLowerCase().trim()}|${d}|${t}` : null;
        if (nKey && _renderedNameKeys.has(nKey)) return false;
        if (aKey && _renderedArtistKeys.has(aKey)) return false;
        if (nKey) _renderedNameKeys.add(nKey);
        if (aKey) _renderedArtistKeys.add(aKey);
        return true;
    });
    _dedupedDropped += apiReturnedCount - events.length;

    const tbody = document.getElementById("events-body");
    events.forEach(ev => {
        const tr = document.createElement("tr");
        // TV channels: show distinct channel names for sports events
        const tvHtml = (() => {
            const chs = ev.tv_channels;
            if (!chs || !chs.length) return "-";
            const names = [...new Set(chs.map(c => c.channel))].slice(0, 3);
            return `<span class="tv-channels">${names.map(n => esc(n)).join(", ")}</span>`;
        })();

        // Artist column: hide for sports (artist_name is null), show for music
        const artistHtml = (() => {
            const a = ev.artist_name && ev.artist_name.toLowerCase() !== ev.name.toLowerCase() ? ev.artist_name : null;
            return a ? `<span class="artist-link" data-artist="${esc(a)}">${esc(a)}</span>` : "-";
        })();

        // YouTube column: "Watch" for music (performer channel),
        // "Highlights" for sports (YouTube search for the matchup).
        const ytHtml = ev.artist_youtube_channel
            ? `<a href="${esc(ev.artist_youtube_channel)}" target="_blank">${ev.sport ? "Highlights" : "Watch"}</a>`
            : "-";

        // data-* attrs on Buy/title links so the delegated click handler
        // (in bindEvents) can fire a buy_click GA event without re-querying
        // anything. Both Event-name and Buy column lead to the same
        // purchase URL — both get class="track-buy" so either click counts.
        const buyAttrs = ev.purchase_link
            ? `class="track-buy" data-event-name="${esc(ev.name)}" `
              + `data-artist="${esc(ev.artist_name || "")}" `
              + `data-venue="${esc(ev.venue_name || "")}" `
              + `data-genre="${esc(ev.artist_genre || "")}" `
              + `data-city="${esc(ev.venue_city || "")}" `
              + `data-country="${esc(ev.venue_country || "")}"`
            : "";
        tr.innerHTML = `
            <td>${ev.purchase_link ? `<a href="${esc(ev.purchase_link)}" target="_blank" ${buyAttrs}>${esc(ev.name)}</a>` : esc(ev.name)}</td>
            <td data-col="artist">${artistHtml}</td>
            <td data-col="youtube">${ytHtml}</td>
            <td>${ev.start_date || "-"}</td>
            <td colspan="2" class="time-cell">
              <div class="time-row">
                <span class="time-val">${ev.start_time || "-"}</span>
                <span class="time-sep">${ev.start_time && ev.end_time ? "–" : ""}</span>
                <span class="time-val">${ev.end_time || ""}</span>
              </div>
              ${ev.venue_timezone ? `<div class="tz">${ev.venue_timezone}</div>` : ""}
            </td>
            <td>
              ${ev.venue_website_url
                ? `<a href="${esc(ev.venue_website_url)}" target="_blank">${esc(ev.venue_name || "-")}</a>`
                : esc(ev.venue_name || "-")}
              ${(!getSelectedCityId() || isMetroSelected() || isCountrySelected()) && (ev.venue_city || ev.venue_country)
                ? `<div class="venue-location">${esc([ev.venue_city, ev.venue_country].filter(Boolean).join(", "))}</div>`
                : ""}
            </td>
            <td data-col="price">${formatPrice(ev.price, ev.price_currency)}</td>
            <td data-col="category">${(ev.categories || []).join(", ") || "-"}</td>
            <td data-col="format">${(ev.event_types || []).join(", ") || "-"}</td>
            <td data-col="genre">${ev.artist_genre ? esc(ev.artist_genre) : "-"}</td>
            <td data-col="tv">${tvHtml}</td>
            <td data-col="link">${ev.purchase_link ? `<a href="${esc(ev.purchase_link)}" target="_blank" ${buyAttrs}>Buy</a>` : "-"}</td>
        `;
        tbody.appendChild(tr);
    });

    offset += apiReturnedCount;
    updateStats(tbody.children.length);

    // ── Sparse-column hiding ──────────────────────────────────────────
    // For columns marked [data-col]: if fewer than 25% of rows carry
    // real data (anything other than empty/"-"), drop the column. The
    // table's outer width is unaffected (#events-table has width:100%)
    // — remaining columns get more breathing room automatically. We
    // evaluate ONLY on the first page so the visibility decision is
    // stable as the user clicks Load More; otherwise columns could
    // flip back in mid-scroll, which is jarring.
    if (isFirstPage) {
        applySparseColumnHiding();
    }

    // ── search_submitted analytics event ─────────────────────────────
    // Fired only on the first page so pagination doesn't double-count.
    // result_count uses events.length here (page count); the precise total
    // comes back asynchronously via the count endpoint and could be added
    // as a follow-up event later if needed.
    if (isFirstPage) {
        pushAnalytics("search_submitted", {
            search_chip_kinds: _chipKindsSignature(),
            search_genres: genres.join(",") || null,
            search_artists: artistExact.join(",") || null,
            search_type: typeSearch.join(",") || null,
            search_free: search || null,
            search_city_id: cityId || null,
            search_country: country || null,
            search_has_date_filter: !!(startDate || endDate),
            result_count: events.length,
            result_was_extended: !!extendedTo,
        });
    }

    // Switch to compact mode after a fresh search (not "Load More")
    if (isFirstPage) showCompactMode();

    const hasMore = apiReturnedCount === LIMIT;
    const btn = document.getElementById("load-more-btn");
    btn.style.display = hasMore ? "" : "none";
    if (hasMore && totalEvents !== null) {
        const remaining = Math.min(totalEvents - offset, LIMIT);
        btn.textContent = `Load Next ${remaining} Events`;
    } else {
        btn.textContent = "Load More";
    }
}

// Sparse-column threshold: a column is hidden when fewer than 25% of
// rows carry real data (equivalently: more than 75% of rows are empty
// or "-"). Raised from 10% on 2026-05-11 — the 10% threshold kept too
// many sparse columns visible, e.g. an Artist column populated for
// 18% of NYC results survived even though it was mostly "-".
const SPARSE_COL_THRESHOLD = 0.25;

function applySparseColumnHiding() {
    // Idempotent: reset any previous hiding first so re-renders of the
    // same page (or a fresh search after a different one) don't carry
    // stale .col-hidden classes onto the new dataset.
    const table = document.getElementById("events-table");
    if (!table) return;
    table.querySelectorAll(".col-hidden").forEach(el => el.classList.remove("col-hidden"));
    table.classList.remove("has-hidden-cols");

    const tbody = document.getElementById("events-body");
    const rows = tbody ? tbody.children : [];
    if (!rows.length) return;

    // Count present cells per column. "Present" = cell has any text
    // other than empty or "-". That matches the convention used when
    // we build the row above (every empty-data path renders "-").
    const presentByCol = {};
    for (const row of rows) {
        for (const cell of row.querySelectorAll("td[data-col]")) {
            const col = cell.dataset.col;
            const txt = (cell.textContent || "").trim();
            if (txt && txt !== "-") {
                presentByCol[col] = (presentByCol[col] || 0) + 1;
            } else if (!(col in presentByCol)) {
                presentByCol[col] = 0;
            }
        }
    }

    const total = rows.length;
    const toHide = Object.entries(presentByCol)
        .filter(([, n]) => (n / total) < SPARSE_COL_THRESHOLD)
        .map(([col]) => col);
    if (!toHide.length) return;

    const hideSet = new Set(toHide);
    // Mark both header and body cells so display:none on the column
    // is consistent. Includes the .col-hidden class on the <th>; CSS
    // handles the actual hiding so we don't have to walk on every
    // scroll/repaint.
    table.querySelectorAll("[data-col]").forEach(el => {
        if (hideSet.has(el.dataset.col)) el.classList.add("col-hidden");
    });
    table.classList.add("has-hidden-cols");
}

function updateStats(shown) {
    // Reduce the displayed total by the running dedup-drop count.
    // The /api/events/count endpoint is a raw COUNT(*) on the matched
    // events table — it doesn't know about the frontend's name+moment
    // collapse, so without this adjustment the user sees a confusing
    // gap (e.g. "Showing 40 of 44" with no Load More button) that
    // looks like a pagination bug. Floor at `shown` so we never claim
    // a total smaller than what's rendered.
    const total = totalEvents !== null
        ? Math.max(shown, totalEvents - _dedupedDropped)
        : null;
    if (total !== null) {
        document.getElementById("stats").textContent =
            `Showing ${shown} of ${total.toLocaleString()} events`;
    } else {
        document.getElementById("stats").textContent = `Showing ${shown} events`;
    }
}

async function exportICS() {
    const { typeSearch, artistExact, genres, cityId, startDate, endDate } = getFilters();
    const body = {};
    if (typeSearch.length) body.type_search = typeSearch.join(",");
    if (artistExact.length) body.artist_exact = artistExact.join(",");
    if (genres.length) body.genres = genres.join(",");
    if (cityId) body.city_ids = cityId.split(",").map(Number).filter(Boolean);
    const country = getSelectedCountry();
    if (country) body.country = country;
    if (startDate) body.start_date = startDate;
    if (endDate) body.end_date = endDate;

    const resp = await fetch("/api/export/ics", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
    });

    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "events.ics";
    a.click();
    URL.revokeObjectURL(url);
}

async function exportCSV() {
    const btn = document.getElementById("export-csv-btn");
    btn.disabled = true;
    btn.textContent = "Downloading...";
    try {
        const { typeSearch, artistExact, genres, cityId, startDate, endDate } = getFilters();
        const body = {};
        if (typeSearch.length) body.type_search = typeSearch.join(",");
        if (artistExact.length) body.artist_exact = artistExact.join(",");
        if (genres.length) body.genres = genres.join(",");
        if (cityId) body.city_ids = cityId.split(",").map(Number).filter(Boolean);
        const country = getSelectedCountry();
        if (country) body.country = country;
        if (startDate) body.start_date = startDate;
        if (endDate) body.end_date = endDate;
        const resp = await fetch("/api/export/csv", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
        });
        if (!resp.ok) { alert("CSV export failed."); return; }
        const blob = await resp.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "supercaly_events.csv";
        a.click();
        URL.revokeObjectURL(url);
    } catch (err) {
        alert("CSV export failed: " + err.message);
    } finally {
        btn.disabled = false;
        btn.textContent = "Download CSV";
    }
}

async function exportSheets() {
    const { typeSearch, artistExact, genres, cityId, startDate, endDate } = getFilters();
    const body = {};
    if (typeSearch.length) body.type_search = typeSearch.join(",");
    if (artistExact.length) body.artist_exact = artistExact.join(",");
    if (genres.length) body.genres = genres.join(",");
    if (cityId) body.city_ids = cityId.split(",").map(Number).filter(Boolean);
    const country = getSelectedCountry();
    if (country) body.country = country;
    if (startDate) body.start_date = startDate;
    if (endDate) body.end_date = endDate;

    const btn = document.getElementById("export-sheets-btn");
    btn.textContent = "Exporting...";
    btn.disabled = true;

    try {
        const resp = await fetch("/api/export/sheets", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            credentials: "same-origin",
            body: JSON.stringify(body),
        });

        const result = await resp.json();
        console.log("Export response:", result);

        if (result.spreadsheet_url) {
            // Use location.href as fallback if popup blocked
            const win = window.open(result.spreadsheet_url, "_blank");
            if (!win) {
                alert("Sheet created! Your browser blocked the popup.\n\nURL: " + result.spreadsheet_url);
            }
        } else if (result.needs_auth && result.auth_url) {
            window.location.href = result.auth_url;
        } else {
            alert(result.message || result.detail || "Google Sheets export failed.");
        }
    } catch (err) {
        console.error("Export error:", err);
        alert("Export failed: " + err.message);
    } finally {
        btn.textContent = "Export to Google Sheet";
        btn.disabled = false;
    }
}

// ── Compact / search-mode helpers ────────────────────────────────────────────

function showCompactMode() {
    const filters    = document.querySelector(".filters");
    const compactBar = document.getElementById("compact-search");
    const compactTxt = document.getElementById("compact-text");
    const contextEl  = document.getElementById("compact-context");

    // Render selected filters as colored chips inside the compact bar so
    // the user sees *what kind of filter* is active, not just a flat string.
    // Reuses the same .type-chip palette as the full filter panel — Genre
    // chips show up as indigo "GENRE Rock", Artist chips as green
    // "ARTIST Sting", etc. Falls back to "All events" placeholder when
    // nothing's selected.
    const { startDate, endDate } = getFilters();
    const cityLabel = document.getElementById("city-input").value.trim();

    const chipHtml = selectedTypeFilters.map(f =>
        `<span class="type-chip type-chip--${esc(f.kind)}">`
        + `<span class="type-chip__badge">${esc(f.badge)}</span> `
        + `${esc(f.value)}`
        + `</span>`
    );
    // Surface uncommitted raw text (≥3 chars) as a freetext chip so the
    // compact view matches what the search will actually filter on.
    const rawText = document.getElementById("type-search-input").value.trim();
    if (rawText.length >= 3 && !selectedTypeFilters.find(f => f.value === rawText)) {
        chipHtml.push(
            `<span class="type-chip type-chip--freetext">`
            + `<span class="type-chip__badge">Search</span> `
            + `${esc(rawText)}`
            + `</span>`
        );
    }
    if (chipHtml.length > 0) {
        compactTxt.innerHTML = chipHtml.join(" ");
        compactTxt.classList.remove("compact-placeholder");
    } else {
        compactTxt.textContent = "All events";
        compactTxt.classList.add("compact-placeholder");
    }

    // Right-side annotation — quick "what am I searching across?" hint.
    // Format dates as "M/D/YY" to match the rest of the marketing UI; fall
    // back to "All dates" / "Global" if either is unset.
    const fmtMD = iso => {
        if (!iso) return null;
        const [y, m, d] = iso.split("-");
        return `${parseInt(m, 10)}/${parseInt(d, 10)}/${y.slice(2)}`;
    };
    const dateRange = (startDate && endDate)
        ? `${fmtMD(startDate)} to ${fmtMD(endDate)}`
        : (startDate ? `from ${fmtMD(startDate)}`
          : (endDate ? `until ${fmtMD(endDate)}` : "All dates"));
    const place = cityLabel || "Global";
    contextEl.textContent = `${dateRange}, ${place}`;

    // Swap visibility
    filters.classList.add("search-hidden");
    compactBar.classList.add("visible");

    // Scroll to the top of the results section
    document.querySelector(".results").scrollIntoView({ behavior: "smooth", block: "start" });
}

function showSearchMode() {
    const filters    = document.querySelector(".filters");
    const compactBar = document.getElementById("compact-search");

    compactBar.classList.remove("visible");
    filters.classList.remove("search-hidden");

    // Focus the type-search input after the panel has animated in
    setTimeout(() => {
        document.getElementById("type-search-input").focus();
    }, 80);

    // Scroll to top so the full filter panel is visible
    window.scrollTo({ top: 0, behavior: "smooth" });
}

function esc(str) {
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
}

function buildSubscribeUrl() {
    const { typeSearch, artistExact, genres, cityId } = getFilters();
    const params = new URLSearchParams();
    if (typeSearch.length) params.set("type_search", typeSearch.join(","));
    if (artistExact.length) params.set("artist_exact", artistExact.join(","));
    if (genres.length) params.set("genres", genres.join(","));
    if (cityId) params.set("city_ids", cityId);
    // Date range is intentionally excluded — subscriptions always show upcoming
    // events dynamically; baking in an end_date causes the feed to go empty.
    const path = `/api/export/subscribe?${params}`;
    const httpsUrl = `${location.protocol}//${location.host}${path}`;
    const webcalUrl = `webcal://${location.host}${path}`;
    return { httpsUrl, webcalUrl };
}

function openSubscribeModal() {
    const { httpsUrl, webcalUrl } = buildSubscribeUrl();
    document.getElementById("webcal-link").href = webcalUrl;
    document.getElementById("subscribe-url").value = httpsUrl;
    document.getElementById("subscribe-modal").hidden = false;
    document.body.style.overflow = "hidden";
}

function closeSubscribeModal() {
    document.getElementById("subscribe-modal").hidden = true;
    document.body.style.overflow = "";
}
