(function () {
  const input = document.getElementById("q");
  const status = document.getElementById("search-status");
  const results = document.getElementById("search-results");
  if (!input || !results) return;

  let index = null;
  let loading = null;

  async function loadIndex() {
    if (index) return index;
    if (loading) return loading;
    status.textContent = "Loading search index…";
    loading = fetch("search-index.json")
      .then((r) => r.json())
      .then((data) => {
        index = data;
        status.textContent = "";
        return index;
      })
      .catch(() => {
        status.textContent = "Could not load search index.";
        loading = null;
        return [];
      });
    return loading;
  }

  function render(hits) {
    if (!hits.length) {
      results.innerHTML = "<p class=\"meta\">No matching threads.</p>";
      return;
    }
    const ul = document.createElement("ul");
    ul.className = "list";
    for (const h of hits.slice(0, 50)) {
      const li = document.createElement("li");
      const authors = (h.teachers || []).join(", ") || "—";
      li.innerHTML =
        "<a class=\"title\" href=\"" + h.url + "\">" + escapeHtml(h.title) + "</a>" +
        "<div class=\"meta\">" + escapeHtml(h.subreddit || "") +
        " · " + escapeHtml(String(h.year)) +
        " · " + escapeHtml(authors) + "</div>";
      ul.appendChild(li);
    }
    results.innerHTML = "";
    results.appendChild(ul);
    status.textContent = hits.length + " result(s)" + (hits.length > 50 ? " (showing 50)" : "");
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function search(q) {
    q = q.trim().toLowerCase();
    if (!q) {
      results.innerHTML = "";
      status.textContent = "";
      return;
    }
    const terms = q.split(/\s+/).filter(Boolean);
    const hits = index.filter((item) => {
      const hay = item.text;
      return terms.every((t) => hay.includes(t));
    });
    render(hits);
  }

  let timer = null;
  input.addEventListener("input", () => {
    clearTimeout(timer);
    timer = setTimeout(async () => {
      await loadIndex();
      search(input.value);
    }, 150);
  });

  const params = new URLSearchParams(location.search);
  if (params.get("q")) {
    input.value = params.get("q");
    loadIndex().then(() => search(input.value));
  }
})();
