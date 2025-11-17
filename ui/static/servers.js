const refreshBtn = document.getElementById("refresh-servers");
const tbody = document.getElementById("servers-body");
const emptyState = document.getElementById("servers-empty");
const statusEl = document.getElementById("servers-status");

refreshBtn?.addEventListener("click", loadServers);
tbody?.addEventListener("click", handleTableClick);
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
      <td>${formatTime(server.verification?.last_scan || server.verification?.lastScan)}</td>
      <td>${renderCorruption(server)}</td>
      <td>${renderNotes(server)}</td>
      <td>${renderActions(server)}</td>
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

function renderCorruption(server) {
  const summary = server.verification;
  if (!summary) {
    return server.verification_error || server.verificationError || "—";
  }

  const corrupted = summary.corrupted_blocks || summary.corruptedBlocks || [];
  if (!corrupted.length) {
    return "None";
  }

  const count = summary.unhealthy_blocks ?? summary.unhealthyBlocks ?? corrupted.length;
  const details = corrupted
    .slice(0, 3)
    .map((entry) => {
      const id = entry.block_id || entry.blockId;
      const message = entry.error || "corrupted";
      return `<div class="mono small">${id}: ${message}</div>`;
    })
    .join("");

  const extra = corrupted.length > 3 ? `<div class="small">+${corrupted.length - 3} more…</div>` : "";

  return `<div><strong>${count}</strong> block(s)</div>${details}${extra}`;
}

function renderNotes(server) {
  const messages = [];
  if (server.error) messages.push(server.error);
  if (server.verification_error || server.verificationError) {
    messages.push(server.verification_error || server.verificationError);
  }
  return messages.join("; ") || "";
}

function renderActions(server) {
  if (!server.address) return "";
  return `<button type="button" class="link-button" data-verify="${server.address}">Verify now</button>`;
}

function handleTableClick(event) {
  const button = event.target.closest("[data-verify]");
  if (!button) return;

  const address = button.dataset.verify;
  if (!address) return;

  triggerVerification(button, address);
}

async function triggerVerification(button, address) {
  const originalLabel = button.textContent;
  button.disabled = true;
  button.textContent = "Verifying...";
  setStatus(`Verifying ${address}...`, "status");

  try {
    const response = await fetch("/api/data-servers/verify", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ address }),
    });

    if (!response.ok) {
      const message = await response.text();
      throw new Error(message || "verification failed");
    }

    setStatus(`Verification completed for ${address}`, "status");
    await loadServers();
  } catch (err) {
    setStatus(err?.message || `Failed to verify ${address}`, "status error");
  } finally {
    button.disabled = false;
    button.textContent = originalLabel;
  }
}

function setStatus(message, className) {
  statusEl.textContent = message;
  statusEl.className = className;
}
