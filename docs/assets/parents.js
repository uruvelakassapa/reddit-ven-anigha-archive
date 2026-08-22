(function () {
  const KEY = "va-show-parents";
  const box = document.getElementById("show-parents");
  if (!box) return;
  if (!document.querySelector(".comment.parent")) {
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
  box.addEventListener("change", function () {
    const on = box.checked;
    try { localStorage.setItem(KEY, on ? "1" : "0"); } catch (e) {}
    apply(on);
  });
})();
