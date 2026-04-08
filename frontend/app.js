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

document.addEventListener("DOMContentLoaded", async () => {
    // Set default date range: today → today + 30 days
    const today = new Date();
    const future = new Date(today);
    future.setDate(future.getDate() + 30);
    const fmt = d => d.toISOString().split("T")[0];
    document.getElementById("start-date").value = fmt(today);
    document.getElementById("end-date").value = fmt(future);

    // Read search state passed from the homepage via sessionStorage
    const homeSearch = JSON.parse(sessionStorage.getItem("supercaly_search") || "null");
    if (homeSearch) sessionStorage.removeItem("supercaly_search");

    // Set flag BEFORE loadCities so detectUserCity() skips geo-detection.
    // Suppress if user came from homepage at all — they made an explicit city
    // choice (including Global / blank), so geo-detection should not override.
    if (homeSearch?._fromHome) window._citySetFromParams = true;

    setupTypeAutocomplete();
    await loadCities();
    bindEvents();

    // Apply filters from homepage
    if (homeSearch?.typeValue) {
        selectedTypeFilters.push({
            kind:  homeSearch.typeKind  || "freetext",
            value: homeSearch.typeValue,
            badge: homeSearch.typeBadge || "Search",
        });
        if (renderTypeChips) renderTypeChips();
    }
    if (homeSearch?.cityId && homeSearch?.cityLabel) {
        document.getElementById("city-input").value = homeSearch.cityLabel;
        document.getElementById("city-id").value    = homeSearch.cityId;
        updateCityClearBtn();
    }

    await searchEvents();

    // ── Compact / search-mode toggle ──────────────────────────────────────────
    // After a search: compact bar is shown, filter panel is hidden.
    // Clicking the compact bar: filter panel slides back in, compact bar hides.

    const compactBar = document.getElementById("compact-search");

    compactBar.addEventListener("click", showSearchMode);
    compactBar.addEventListener("keydown", e => {
        if (e.key === "Enter" || e.key === " ") { e.preventDefault(); showSearchMode(); }
    });

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
        if (q.length < 3) { list.hidden = true; return; }
        debounceTimer = setTimeout(async () => {
            const resp = await fetch(`/api/suggestions?q=${encodeURIComponent(q)}`);
            const items = await resp.json();
            showSuggestions(items);
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

async function loadCities() {
    const resp = await fetch("/api/cities");
    allCities = await resp.json();
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
        document.getElementById("city-input").value = `${best.name}, ${best.country}`;
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
    list.innerHTML = matches.map(c =>
        `<li data-id="${c.id}" data-label="${c.label || (c.name + ', ' + c.country)}">${c.label || (c.name + ', ' + c.country)}</li>`
    ).join("");
    list.hidden = matches.length === 0;
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

        const cityMatches = allCities.filter(c =>
            `${c.name}, ${c.country}`.toLowerCase().includes(q)
        ).slice(0, 10);

        // Always show Global as first option
        const matches = [GLOBAL_CITY, ...cityMatches];

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
    return document.getElementById("city-id").value;
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

function getFilters() {
    // All chip values (category, event_type, performer, freetext) become type_search terms
    const chipTerms = selectedTypeFilters.map(f => f.value);
    // Also pick up any uncommitted text still in the input (≥3 chars)
    const rawText = document.getElementById("type-search-input").value.trim();
    if (rawText.length >= 3 && !chipTerms.includes(rawText)) chipTerms.push(rawText);

    const cityId    = getSelectedCityId();
    const startDate = document.getElementById("start-date").value;
    const endDate   = document.getElementById("end-date").value;
    const search    = document.getElementById("search").value;
    return { typeSearch: chipTerms, cityId, startDate, endDate, search };
}

let totalEvents = null; // total matching count from /api/events/count

async function searchEvents() {
    const isFirstPage = offset === 0;   // capture before any mutation
    const { typeSearch, cityId, startDate, endDate, search } = getFilters();
    const params = new URLSearchParams();
    if (typeSearch.length) params.set("type_search", typeSearch.join(","));
    if (cityId) params.set("city_ids", cityId);
    if (startDate) params.set("start_date", startDate);
    if (endDate) params.set("end_date", endDate);
    if (search) params.set("search", search);

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
    const events = await resp.json();

    const tbody = document.getElementById("events-body");
    events.forEach(ev => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td>${ev.purchase_link ? `<a href="${esc(ev.purchase_link)}" target="_blank">${esc(ev.name)}</a>` : esc(ev.name)}</td>
            <td>${(() => { const a = ev.artist_name && ev.artist_name.toLowerCase() !== ev.name.toLowerCase() ? ev.artist_name : null; return a ? `<span class="artist-link" data-artist="${esc(a)}">${esc(a)}</span>` : "-"; })()}</td>
            <td>${ev.artist_youtube_channel ? `<a href="${esc(ev.artist_youtube_channel)}" target="_blank">Watch</a>` : "-"}</td>
            <td>${ev.start_date || "-"}</td>
            <td colspan="2" class="time-cell">
              <div class="time-row">
                <span class="time-val">${ev.start_time || "-"}</span>
                <span class="time-sep">${ev.start_time && ev.end_time ? "–" : ""}</span>
                <span class="time-val">${ev.end_time || ""}</span>
              </div>
              ${!getSelectedCityId() && ev.venue_timezone ? `<div class="tz">${ev.venue_timezone}</div>` : ""}
            </td>
            <td>
              ${ev.venue_website_url
                ? `<a href="${esc(ev.venue_website_url)}" target="_blank">${esc(ev.venue_name || "-")}</a>`
                : esc(ev.venue_name || "-")}
              ${!getSelectedCityId() && (ev.venue_city || ev.venue_country)
                ? `<div class="venue-location">${esc([ev.venue_city, ev.venue_country].filter(Boolean).join(", "))}</div>`
                : ""}
            </td>
            <td>${formatPrice(ev.price, ev.price_currency)}</td>
            <td>${(ev.categories || []).join(", ") || "-"}</td>
            <td>${(ev.event_types || []).join(", ") || "-"}</td>
            <td>${ev.purchase_link ? `<a href="${esc(ev.purchase_link)}" target="_blank">Buy</a>` : "-"}</td>
        `;
        tbody.appendChild(tr);
    });

    offset += events.length;
    updateStats(tbody.children.length);

    // Switch to compact mode after a fresh search (not "Load More")
    if (isFirstPage) showCompactMode();

    const hasMore = events.length === LIMIT;
    const btn = document.getElementById("load-more-btn");
    btn.style.display = hasMore ? "" : "none";
    if (hasMore && totalEvents !== null) {
        const remaining = Math.min(totalEvents - offset, LIMIT);
        btn.textContent = `Load Next ${remaining} Events`;
    } else {
        btn.textContent = "Load More";
    }
}

function updateStats(shown) {
    const total = totalEvents;
    if (total !== null) {
        document.getElementById("stats").textContent =
            `Showing ${shown} of ${total.toLocaleString()} events`;
    } else {
        document.getElementById("stats").textContent = `Showing ${shown} events`;
    }
}

async function exportICS() {
    const { typeSearch, cityId, startDate, endDate } = getFilters();
    const body = {};
    if (typeSearch.length) body.type_search = typeSearch.join(",");
    if (cityId) body.city_ids = [parseInt(cityId)];
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
        const { typeSearch, cityId, startDate, endDate } = getFilters();
        const body = {};
        if (typeSearch.length) body.type_search = typeSearch.join(",");
        if (cityId) body.city_ids = [parseInt(cityId)];
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
    const { typeSearch, cityId, startDate, endDate } = getFilters();
    const body = {};
    if (typeSearch.length) body.type_search = typeSearch.join(",");
    if (cityId) body.city_ids = [parseInt(cityId)];
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
    const cityBadge  = document.getElementById("compact-city-badge");

    // Build summary text from current active filters
    const { typeSearch } = getFilters();
    const cityLabel = document.getElementById("city-input").value.trim();

    if (typeSearch.length > 0) {
        compactTxt.textContent = typeSearch.join(" · ");
        compactTxt.classList.remove("compact-placeholder");
    } else {
        compactTxt.textContent = "All events";
        compactTxt.classList.add("compact-placeholder");
    }
    cityBadge.textContent = cityLabel || "";

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
    const { typeSearch, cityId } = getFilters();
    const params = new URLSearchParams();
    if (typeSearch.length) params.set("type_search", typeSearch.join(","));
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
