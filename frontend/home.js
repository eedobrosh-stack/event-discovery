// Supercaly homepage — autocomplete, geo-detection, navigation to results

let allCities = [];
let allMetroAreas = [];
let allCountries = [];
let allStates = [];
let selectedType = null; // { kind, value, badge } — set by autocomplete
let selectedCityId = "";
let selectedIsMetro = false;
let selectedIsCountry = false;

const GLOBAL_CITY = { id: "", name: "🌍 Global", country: "All Cities", label: "🌍 Global — All Cities" };

// Kept in lockstep with app/api/_us_states.py.US_STATE_NAMES and the
// matching set in app.js. Used to detect when a US city's name overlaps
// a state's name (e.g. "New York" the city in "New York" the state)
// and append " City" for disambiguation.
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

function formatCityLabel(c) {
    if (!c) return "";
    if (c.country === "United States" && c.state) {
        const dispName = US_STATE_NAMES_SET.has(c.name) ? `${c.name} City` : c.name;
        return `${dispName}, ${c.state} State`;
    }
    return `${c.name}, ${c.country}`;
}

function esc(str) {
    return String(str)
        .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

// ── Type / Performer autocomplete ─────────────────────────────────────────────
function setupTypeAutocomplete() {
    const input = document.getElementById("home-type-input");
    const list  = document.getElementById("home-type-suggestions");
    let activeIdx = -1;
    let debounceTimer = null;
    let suggestController = null;  // AbortController for the in-flight /api/suggestions fetch

    // Client-side response cache (mirrors app.js). Re-typed queries
    // return instantly with no network round-trip.
    const suggestionsCache = new Map();
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
            const oldest = suggestionsCache.keys().next().value;
            suggestionsCache.delete(oldest);
        }
    }
    // Mirror the keystroke-chaining derivation from app.js. SAFE_DERIVE_FLOOR=4
    // is the safety guard: shorter parents use whole-word matching while
    // children >=4 chars use word-start, so child can have legitimate hits
    // the parent matcher excluded. Filtering the parent set in that case
    // would falsely return empty (regression: typing "class" after "cla"
    // returned 0 results when "cla" had 0 due to whole-word strictness).
    const SERVER_LIMIT = 12;
    const SAFE_DERIVE_FLOOR = 4;
    function _deriveFromParent(q) {
        const qLower = q.toLowerCase();
        for (let n = q.length - 1; n >= SAFE_DERIVE_FLOOR; n--) {
            const parent = q.slice(0, n);
            const cachedParent = _cacheGet(parent);
            if (!cachedParent) continue;
            if (cachedParent.length >= SERVER_LIMIT) return null;
            return cachedParent.filter(item => {
                const hay = ((item.value || "") + " " + (item.label || "")).toLowerCase();
                return hay.includes(qLower);
            });
        }
        return null;
    }

    function showSuggestions(items) {
        if (!items.length) { list.hidden = true; return; }
        activeIdx = -1;
        list.innerHTML = items.map((item, i) =>
            `<li data-idx="${i}" data-kind="${item.kind}"
                 data-value="${esc(item.value)}" data-badge="${esc(item.badge)}">
                <span class="sugg-badge sugg-badge--${item.kind}">${item.badge}</span>
                ${esc(item.label)}
            </li>`
        ).join("");
        list.hidden = false;
    }

    function selectItem(li) {
        const kind = li.dataset.kind;
        // City picks aren't a type filter — they belong in the city box. Move
        // the selection over there and clear the type input so the user can
        // still add a type/performer/category on top of the city.
        if (kind === "city") {
            const cityInput  = document.getElementById("home-city-input");
            const cityHidden = document.getElementById("home-city-id");
            const cityClear  = document.getElementById("home-city-clear");
            const label = li.textContent.trim().replace(/^City\s*/, "");
            if (cityInput && cityHidden) {
                cityInput.value  = label;
                cityHidden.value = li.dataset.value;
                if (cityClear) cityClear.hidden = false;
            }
            input.value = "";
            selectedType = null;
            list.hidden = true;
            activeIdx = -1;
            return;
        }
        selectedType = { kind, value: li.dataset.value, badge: li.dataset.badge };
        input.value = li.dataset.value;
        list.hidden = true;
        activeIdx = -1;
    }

    input.addEventListener("input", () => {
        const q = input.value.trim();
        selectedType = null;
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

        // Forward-typing — derive from a cached shorter prefix when safe.
        const derived = _deriveFromParent(q);
        if (derived !== null) {
            _cacheSet(q, derived);
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

    list.addEventListener("mousedown", e => e.preventDefault());
    list.addEventListener("click", e => {
        const li = e.target.closest("li");
        if (li) selectItem(li);
    });

    input.addEventListener("keydown", e => {
        const lis = list.querySelectorAll("li");
        if (e.key === "ArrowDown") {
            e.preventDefault();
            activeIdx = Math.min(activeIdx + 1, lis.length - 1);
        } else if (e.key === "ArrowUp") {
            e.preventDefault();
            activeIdx = Math.max(activeIdx - 1, 0);
        } else if (e.key === "Enter") {
            e.preventDefault();
            if (activeIdx >= 0 && lis[activeIdx]) {
                selectItem(lis[activeIdx]);
            } else {
                navigateToResults();
            }
            return;
        } else if (e.key === "Escape") {
            list.hidden = true; return;
        }
        lis.forEach((li, i) => li.classList.toggle("active", i === activeIdx));
        if (activeIdx >= 0) lis[activeIdx].scrollIntoView({ block: "nearest" });
    });

    document.addEventListener("click", e => {
        if (!input.contains(e.target) && !list.contains(e.target)) list.hidden = true;
    });
}

// ── City autocomplete ─────────────────────────────────────────────────────────
function setupCityAutocomplete() {
    const input    = document.getElementById("home-city-input");
    const list     = document.getElementById("home-city-suggestions");
    const hidden   = document.getElementById("home-city-id");
    const clearBtn = document.getElementById("home-city-clear");
    let activeIdx = -1;

    function renderList(items) {
        list.innerHTML = items.map(c => {
            const id    = c._isMeta ? c.city_ids : (c.id || "");
            const label = c.label || formatCityLabel(c);
            // States behave like metros at the selection layer (city_ids
            // list); give them their own class for styling distinction.
            let cls = "";
            if (c._isState)        cls = " class=\"state-option\"";
            else if (c._isMeta)    cls = " class=\"metro-option\"";
            else if (c._isCountry) cls = " class=\"country-option\"";
            return `<li data-id="${id}" data-label="${esc(label)}" data-ismeta="${c._isMeta ? '1' : ''}" data-iscountry="${c._isCountry ? '1' : ''}"${cls}>${esc(label)}</li>`;
        }).join("");
        list.hidden = items.length === 0;
        activeIdx = -1;
    }

    function matchCities(q) {
        const metroMatches = allMetroAreas.filter(m =>
            m.name.toLowerCase().includes(q) ||
            m.city_names.some(cn => cn.toLowerCase().includes(q))
        ).slice(0, 3);

        const countryMatches = allCountries.filter(c =>
            c.name.toLowerCase().includes(q)
        ).slice(0, 3);

        const stateMatches = allStates.filter(s => {
            const n = s.name.toLowerCase();
            return n.includes(q) || `${n} state`.includes(q);
        }).slice(0, 3);

        // Cities split into two buckets:
        //   • exact (name starts with the query) — promoted above
        //     metros so a literal city hit isn't hidden by the
        //     parent metro group ("tel av" → Tel Aviv before Gush Dan).
        //   • other (substring matches further down the label) — stay
        //     in the normal cascade slot.
        const cityAll = allCities.filter(c =>
            formatCityLabel(c).toLowerCase().includes(q)
        );
        const cityExact = cityAll
            .filter(c => c.name.toLowerCase().startsWith(q))
            .slice(0, 3)
            .map(c => ({ ...c, label: formatCityLabel(c) }));
        const exactSet = new Set(cityExact.map(c => c.id));
        const cityOther = cityAll
            .filter(c => !exactSet.has(c.id))
            .slice(0, 5)
            .map(c => ({ ...c, label: formatCityLabel(c) }));

        // Order: ExactCity → Metro → Country → State → OtherCity.
        return [...cityExact, ...metroMatches, ...countryMatches,
                ...stateMatches, ...cityOther];
    }

    clearBtn.addEventListener("click", () => {
        input.value = hidden.value = "";
        selectedCityId = "";
        clearBtn.hidden = true;
        list.hidden = true;
    });

    input.addEventListener("focus", () => {
        if (input.value.trim().length < 2) renderList([GLOBAL_CITY]);
    });

    input.addEventListener("input", () => {
        const q = input.value.trim().toLowerCase();
        hidden.value = "";
        selectedCityId = "";
        clearBtn.hidden = !input.value.trim();
        if (q.length < 2) { renderList([GLOBAL_CITY]); return; }
        const matches = matchCities(q);
        // Always keep Global as the first option so it's never hidden by city matches
        renderList([GLOBAL_CITY, ...matches]);
    });

    list.addEventListener("mousedown", e => e.preventDefault());
    list.addEventListener("click", e => {
        const li = e.target.closest("li");
        if (!li) return;
        input.value      = li.dataset.label;
        hidden.value     = li.dataset.id;
        selectedCityId   = li.dataset.id;
        selectedIsMetro  = li.dataset.ismeta === "1";
        selectedIsCountry = li.dataset.iscountry === "1";
        clearBtn.hidden = !input.value.trim();
        list.hidden = true;
    });

    input.addEventListener("keydown", e => {
        const lis = list.querySelectorAll("li");
        if (e.key === "ArrowDown") {
            e.preventDefault();
            activeIdx = Math.min(activeIdx + 1, lis.length - 1);
        } else if (e.key === "ArrowUp") {
            e.preventDefault();
            activeIdx = Math.max(activeIdx - 1, 0);
        } else if (e.key === "Enter") {
            e.preventDefault();
            if (activeIdx >= 0 && lis[activeIdx]) lis[activeIdx].click();
            navigateToResults();
            return;
        } else if (e.key === "Escape") {
            list.hidden = true; return;
        }
        lis.forEach((li, i) => li.classList.toggle("active", i === activeIdx));
        if (activeIdx >= 0) lis[activeIdx].scrollIntoView({ block: "nearest" });
    });

    input.addEventListener("blur", () => setTimeout(() => { list.hidden = true; }, 150));
}

// ── Geo-detect city (placeholder only — don't auto-select) ───────────────────
async function detectCityPlaceholder() {
    const input = document.getElementById("home-city-input");
    try {
        const r   = await fetch("https://ipapi.co/json/");
        const geo = await r.json();
        const city    = geo.city || "";
        const country = geo.country_name || "";
        if (city) input.placeholder = `e.g. ${city}, ${country}`;
    } catch {}
}

// ── Navigate to results ───────────────────────────────────────────────────────
function navigateToResults() {
    const typeInput = document.getElementById("home-type-input");
    const cityInput = document.getElementById("home-city-input");
    const cityId    = document.getElementById("home-city-id").value;

    const state = {};

    // Type / performer filter
    const typeVal = typeInput.value.trim();
    if (selectedType) {
        state.typeKind  = selectedType.kind;
        state.typeValue = selectedType.value;
        state.typeBadge = selectedType.badge;
    } else if (typeVal) {
        state.typeKind  = "freetext";
        state.typeValue = typeVal;
        state.typeBadge = "Search";
    }

    // City filter — prefer explicitly selected ID, then try text match
    if (cityId) {
        state.cityId       = cityId;
        state.cityLabel    = cityInput.value.trim();
        state.cityIsMeta   = selectedIsMetro;
        state.cityIsCountry = selectedIsCountry;
    } else if (cityInput.value.trim()) {
        const q = cityInput.value.trim().toLowerCase();
        // Try metro area match first
        const metroMatch = allMetroAreas.find(m => m.name.toLowerCase() === q);
        if (metroMatch) {
            state.cityId       = metroMatch.city_ids;
            state.cityLabel    = metroMatch.label;
            state.cityIsMeta   = true;
            state.cityIsCountry = false;
        } else {
            // Try country match
            const countryMatch = allCountries.find(c => c.name.toLowerCase() === q);
            if (countryMatch) {
                state.cityId       = `COUNTRY:${countryMatch.name}`;
                state.cityLabel    = countryMatch.label;
                state.cityIsMeta   = false;
                state.cityIsCountry = true;
            } else {
                // Try US state match — accept either "California" or
                // "California State" since both lead to the same row.
                const stateMatch = allStates.find(s => {
                    const n = s.name.toLowerCase();
                    return n === q || `${n} state` === q;
                });
                if (stateMatch) {
                    state.cityId       = stateMatch.city_ids;
                    state.cityLabel    = stateMatch.label;
                    state.cityIsMeta   = true;     // selection plumbing
                    state.cityIsCountry = false;
                } else {
                    const match = allCities.find(c =>
                        c.name.toLowerCase() === q ||
                        formatCityLabel(c).toLowerCase() === q
                    );
                    if (match) {
                        state.cityId       = String(match.id);
                        state.cityLabel    = formatCityLabel(match);
                        state.cityIsCountry = false;
                    }
                }
            }
        }
    }

    // Always write state so results page knows it came from the homepage
    // (even if empty — signals "user explicitly chose Global / no city")
    state._fromHome = true;
    sessionStorage.setItem("supercaly_search", JSON.stringify(state));
    window.location.href = "/results.html";
}

// ── Init ─────────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", async () => {
    setupTypeAutocomplete();
    detectCityPlaceholder();

    try {
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

        // Build the state → city_ids mapping client-side from the
        // already-loaded cities list (which has state codes normalised
        // to canonical full names by the backend). Mirror the metro
        // selection plumbing: state row carries city_ids so existing
        // multi-city filter paths handle it transparently.
        const stateCityIds = {};
        for (const c of allCities) {
            if (c.country === "United States" && c.state) {
                (stateCityIds[c.state] ||= []).push(c.id);
            }
        }
        allStates = (await statesResp.json()).map(s => ({
            ...s,
            _isState: true,
            _isMeta: true,
            city_ids: (stateCityIds[s.name] || []).join(","),
            label: `🏛 ${s.name} State (${s.city_count} cities)`,
        }));
    } catch {}
    setupCityAutocomplete();

    document.getElementById("home-search-btn").addEventListener("click", navigateToResults);

    // Land with the search box focused — saves the user a click.
    // (The `autofocus` HTML attribute also covers this, but Safari sometimes
    // ignores it after navigations; calling .focus() explicitly is robust.)
    const typeInput = document.getElementById("home-type-input");
    if (typeInput) typeInput.focus();
});
