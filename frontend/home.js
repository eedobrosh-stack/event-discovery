// Supercaly homepage — autocomplete, geo-detection, navigation to results

let allCities = [];
let allMetroAreas = [];
let allCountries = [];
let selectedType = null; // { kind, value, badge } — set by autocomplete
let selectedCityId = "";
let selectedIsMetro = false;
let selectedIsCountry = false;

const GLOBAL_CITY = { id: "", name: "🌍 Global", country: "All Cities", label: "🌍 Global — All Cities" };

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
        if (q.length < 2) { list.hidden = true; return; }
        debounceTimer = setTimeout(async () => {
            try {
                const resp = await fetch(`/api/suggestions?q=${encodeURIComponent(q)}`);
                showSuggestions(await resp.json());
            } catch {}
        }, 80);
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
            const label = c.label || `${c.name}, ${c.country}`;
            const cls   = c._isMeta ? " class=\"metro-option\"" : c._isCountry ? " class=\"country-option\"" : "";
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

        const cityMatches = allCities
            .filter(c =>
                c.name.toLowerCase().includes(q) ||
                c.country.toLowerCase().includes(q)
            )
            .slice(0, 5)
            .map(c => ({ ...c, label: `${c.name}, ${c.country}` }));

        return [...metroMatches, ...countryMatches, ...cityMatches];
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
                const match = allCities.find(c =>
                    c.name.toLowerCase() === q ||
                    `${c.name}, ${c.country}`.toLowerCase() === q
                );
                if (match) {
                    state.cityId       = String(match.id);
                    state.cityLabel    = `${match.name}, ${match.country}`;
                    state.cityIsCountry = false;
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
        const [citiesResp, metroResp, countriesResp] = await Promise.all([
            fetch("/api/cities"),
            fetch("/api/metro-areas"),
            fetch("/api/cities/countries"),
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
    } catch {}
    setupCityAutocomplete();

    document.getElementById("home-search-btn").addEventListener("click", navigateToResults);

    // Land with the search box focused — saves the user a click.
    // (The `autofocus` HTML attribute also covers this, but Safari sometimes
    // ignores it after navigations; calling .focus() explicitly is robust.)
    const typeInput = document.getElementById("home-type-input");
    if (typeInput) typeInput.focus();
});
