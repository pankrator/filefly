const refreshBtn = document.getElementById("refresh-servers");
const tbody = document.getElementById("servers-body");
const emptyState = document.getElementById("servers-empty");
const statusEl = document.getElementById("servers-status");

refreshBtn?.addEventListener("click", loadServers);
document.addEventListener("DOMContentLoaded", loadServers);

async function loadServers() {
  setStatus("Loading...", "status");
  try {
    const response = await fetch("/api/data-servers");
    if (!response.ok) throw new Error("Unable to fetch data servers");
    const data = await response.json();
    renderServers(data.servers ?? []);
    setStatus(`Last updated ${new Date().toLocaleTimeString()}`, "status");
  } catch (err) {
    renderServers([]);
    setStatus(err?.message || "Failed to load server status.", "status error");
  }
}

function renderServers(servers) {
  tbody.innerHTML = "";
  if (!servers.length) {
    emptyState.classList.remove("hidden");
    return;
  }
  emptyState.classList.add("hidden");
  servers.forEach((server) => {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td class="mono">${server.address}</td>
      <td>${renderHealthPill(server)}</td>
      <td>${formatTime(server.last_pong || server.lastPong)}</td>
      <td>${formatTime(server.last_checked || server.lastChecked)}</td>
      <td>${server.error || ""}</td>
    `;
    tbody.appendChild(row);
  });
}

function renderHealthPill(server) {
  const healthy = Boolean(server.healthy);
  const label = healthy ? "Healthy" : "Unhealthy";
  const className = healthy ? "health-pill healthy" : "health-pill unhealthy";
  return `<span class="${className}">${label}</span>`;
}

function formatTime(value) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleString();
}

function setStatus(message, className) {
  statusEl.textContent = message;
  statusEl.className = className;
}
