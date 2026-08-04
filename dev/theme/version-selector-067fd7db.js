// Lightweight, framework-free version switcher for the AgentENV mdbook site.
//
// The site is deployed as a GitHub Pages *project* page, so every published
// version lives under a fixed base path:
//   https://<org>.github.io/AgentENV/{version}/...
// where {version} is one of: "dev" (unreleased preview built from `main`),
// "latest" (alias of the most recently released tag), or a release tag such
// as "v0.1.0".
//
// A JSON manifest at "/AgentENV/versions.json" (maintained by
// .github/workflows/docs.yml) lists all released tag versions. "dev" and
// "latest" always exist and are not required to be present in that manifest.
(function () {
  "use strict";

  var REPO_BASE = "/AgentENV/";

  function currentVersionAndRest() {
    var path = window.location.pathname;
    if (path.indexOf(REPO_BASE) !== 0) {
      return { version: "", rest: "" };
    }
    var remainder = path.slice(REPO_BASE.length);
    var parts = remainder.split("/");
    var version = parts.shift() || "";
    return { version: version, rest: parts.join("/") };
  }

  function insertPreviewBanner() {
    var banner = document.createElement("div");
    banner.className = "version-preview-banner";

    var text = document.createElement("span");
    text.textContent =
      "You are viewing a preview built from the latest main branch. Some features described here may not be released yet.";
    banner.appendChild(text);

    var link = document.createElement("a");
    link.href = REPO_BASE + "latest/";
    link.textContent = "View latest released docs \u2192";
    banner.appendChild(link);

    var content = document.getElementById("content") || document.body;
    content.insertBefore(banner, content.firstChild);
  }

  function navigateToVersion(version, rest) {
    var target = REPO_BASE + version + "/" + rest;
    fetch(target, { method: "HEAD" })
      .then(function (res) {
        window.location.href = res.ok ? target : REPO_BASE + version + "/";
      })
      .catch(function () {
        window.location.href = REPO_BASE + version + "/";
      });
  }

  function insertVersionSelector(versions, current) {
    var known = versions.slice();
    if (current.version && known.indexOf(current.version) === -1) {
      known.unshift(current.version);
    }

    var wrapper = document.createElement("div");
    wrapper.className = "version-selector";

    var select = document.createElement("select");
    select.setAttribute("aria-label", "Documentation version");

    known.forEach(function (v) {
      var opt = document.createElement("option");
      opt.value = v;
      opt.textContent = v;
      if (v === current.version) {
        opt.selected = true;
      }
      select.appendChild(opt);
    });

    select.addEventListener("change", function () {
      navigateToVersion(select.value, current.rest);
    });

    wrapper.appendChild(select);

    var menuBar =
      document.querySelector(".menu-bar .right-buttons") ||
      document.querySelector(".right-buttons") ||
      document.querySelector(".menu-bar");
    if (menuBar) {
      menuBar.insertBefore(wrapper, menuBar.firstChild);
    } else {
      document.body.appendChild(wrapper);
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    var current = currentVersionAndRest();

    if (current.version === "dev") {
      insertPreviewBanner();
    }

    fetch(REPO_BASE + "versions.json", { cache: "no-cache" })
      .then(function (res) {
        if (!res.ok) {
          throw new Error("versions.json not available");
        }
        return res.json();
      })
      .then(function (manifest) {
        var releasedVersions = (manifest.versions || []).map(function (entry) {
          return entry.version;
        });
        var versions = ["latest", "dev"].concat(releasedVersions);
        insertVersionSelector(versions, current);
      })
      .catch(function () {
        insertVersionSelector(["latest", "dev"], current);
      });
  });
})();
