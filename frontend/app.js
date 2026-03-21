let categoriesChoice;
let offset = 0;
const LIMIT = 50;

document.addEventListener("DOMContentLoaded", async () => {
    await loadCategories();
    await loadCities();
    bindEvents();
    await searchEvents();

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

async function loadCategories() {
    const resp = await fetch("/api/event-types/categories");
    const categories = await resp.json();
    const sel = document.getElementById("categories");
    categories.forEach(cat => {
        const opt = document.createElement("option");
        opt.value = cat;
        opt.textContent = cat;
        sel.appendChild(opt);
    });
    categoriesChoice = new Choices(sel, {
        removeItemButton: true,
        placeholder: true,
        placeholderValue: "Select categories...",
    });
}

async function loadCities() {
    const resp = await fetch("/api/cities");
    const cities = await resp.json();
    const sel = document.getElementById("city");
    cities.forEach(city => {
        const opt = document.createElement("option");
        opt.value = city.id;
        opt.textContent = `${city.name}, ${city.country}`;
        sel.appendChild(opt);
    });
}

function bindEvents() {
    document.getElementById("search-btn").addEventListener("click", () => {
        offset = 0;
        document.getElementById("events-body").innerHTML = "";
        searchEvents();
    });

    document.getElementById("select-all-btn").addEventListener("click", () => {
        const allSelected = categoriesChoice.getValue(true).length ===
            categoriesChoice._store.choices.filter(c => !c.disabled).length;
        if (allSelected) {
            categoriesChoice.removeActiveItems();
        } else {
            categoriesChoice.removeActiveItems();
            categoriesChoice._store.choices
                .filter(c => !c.disabled)
                .forEach(c => categoriesChoice.setChoiceByValue(c.value));
        }
    });

    document.getElementById("export-ics-btn").addEventListener("click", exportICS);
    document.getElementById("export-sheets-btn").addEventListener("click", exportSheets);
    document.getElementById("load-more-btn").addEventListener("click", searchEvents);
}

function getFilters() {
    const cats = categoriesChoice.getValue(true);
    const cityId = document.getElementById("city").value;
    const startDate = document.getElementById("start-date").value;
    const endDate = document.getElementById("end-date").value;
    const search = document.getElementById("search").value;
    return { cats, cityId, startDate, endDate, search };
}

async function searchEvents() {
    const { cats, cityId, startDate, endDate, search } = getFilters();
    const params = new URLSearchParams();
    if (cats.length) params.set("categories", cats.join(","));
    if (cityId) params.set("city_ids", cityId);
    if (startDate) params.set("start_date", startDate);
    if (endDate) params.set("end_date", endDate);
    if (search) params.set("search", search);
    params.set("limit", LIMIT);
    params.set("offset", offset);

    const resp = await fetch(`/api/events?${params}`);
    const events = await resp.json();

    const tbody = document.getElementById("events-body");
    events.forEach(ev => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td>${esc(ev.name)}</td>
            <td>${esc(ev.artist_name || "-")}</td>
            <td>${ev.artist_youtube_channel ? `<a href="${esc(ev.artist_youtube_channel)}" target="_blank">Watch</a>` : "-"}</td>
            <td>${ev.start_date || "-"}</td>
            <td>${ev.start_time || "-"}</td>
            <td>${esc(ev.venue_name || "-")}</td>
            <td>${ev.price ? `${ev.price_currency || "$"}${ev.price}` : "-"}</td>
            <td>${(ev.categories || []).join(", ") || "-"}</td>
            <td>${ev.purchase_link ? `<a href="${esc(ev.purchase_link)}" target="_blank">Buy</a>` : "-"}</td>
        `;
        tbody.appendChild(tr);
    });

    offset += events.length;
    document.getElementById("stats").textContent = `Showing ${tbody.children.length} events`;
    document.getElementById("load-more-btn").style.display = events.length === LIMIT ? "" : "none";
}

async function exportICS() {
    const { cats, cityId, startDate, endDate } = getFilters();
    const body = {};
    if (cats.length) body.categories = cats;
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

async function exportSheets() {
    const { cats, cityId, startDate, endDate } = getFilters();
    const body = {};
    if (cats.length) body.categories = cats;
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

function esc(str) {
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
}
