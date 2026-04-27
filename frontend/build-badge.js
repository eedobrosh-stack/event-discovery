// Build-version badge — fetches /api/version and renders the short SHA
// in the bottom-right of any page that includes a #build-badge container.
// Click → the GitHub commit page for the deployed build.
//
// Self-contained: injects its own styles so it works on every page,
// including those (home, admin) that don't load style.css.
(function () {
    const el = document.getElementById("build-badge");
    if (!el) return;

    // Inject styles once. Idempotent in case multiple pages somehow share state.
    if (!document.getElementById("build-badge-styles")) {
        const css = document.createElement("style");
        css.id = "build-badge-styles";
        css.textContent = `
            .build-badge {
                position: fixed;
                right: 8px;
                bottom: 6px;
                z-index: 10;
                font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
                font-size: 11px;
                line-height: 1;
                color: #888;
                opacity: 0.35;
                transition: opacity 0.15s ease;
            }
            .build-badge:hover { opacity: 1; }
            .build-badge a { color: inherit; text-decoration: none; }
            .build-badge a:hover { text-decoration: underline; }
        `;
        document.head.appendChild(css);
    }

    fetch("/api/version", { cache: "no-store" })
        .then(r => r.ok ? r.json() : Promise.reject())
        .then(v => {
            const short = v.short || "dev";
            const title = v.sha
                ? `Deployed commit ${v.sha} on ${v.branch}`
                : "Local dev build";
            if (v.url) {
                const a = document.createElement("a");
                a.href = v.url;
                a.target = "_blank";
                a.rel = "noopener";
                a.title = title;
                a.textContent = short;
                el.appendChild(a);
            } else {
                el.textContent = short;
                el.title = title;
            }
        })
        .catch(() => {
            // Endpoint missing or unreachable — leave badge invisible.
            el.style.display = "none";
        });
})();
