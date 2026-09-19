(function () {
  const KEY = "va-show-parents";
  const box = document.getElementById("show-parents");
  if (!box) return;
  const parents = document.querySelectorAll(".comment.parent");
  if (!parents.length) {
    const wrap = box.closest(".parent-toggle");
    if (wrap) wrap.hidden = true;
    return;
  }
  function apply(on) {
    document.documentElement.classList.toggle("show-parents", on);
    box.checked = on;
  }
  let stored = false;
  try { stored = localStorage.getItem(KEY) === "1"; } catch (e) {}
  apply(stored);
  // a "replying to" link may point at a hidden user reply: reveal it
  function revealTarget() {
    const t = location.hash && document.querySelector(location.hash);
    if (t && t.classList.contains("parent") && !box.checked) {
      apply(true);
      t.scrollIntoView({ block: "start" });
    }
  }
  revealTarget();
  window.addEventListener("hashchange", revealTarget);
  box.addEventListener("change", function () {
    const on = box.checked;
    try { localStorage.setItem(KEY, on ? "1" : "0"); } catch (e) {}
    apply(on);
    if (!on) return;
    const first = parents[0];
    const r = first.getBoundingClientRect();
    if (r.top < 0 || r.top > window.innerHeight * 0.45) {
      first.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  });
})();
