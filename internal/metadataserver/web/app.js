const fileListEl = document.getElementById("file-list");
const fileEmptyEl = document.getElementById("file-empty");
const fileDetailsEl = document.getElementById("file-details");
const refreshBtn = document.getElementById("refresh-files");
const uploadForm = document.getElementById("upload-form");
const uploadStatus = document.getElementById("upload-status");
const fileInput = document.getElementById("file-input");
const fileNameInput = document.getElementById("file-name");
const fileTemplate = document.getElementById("file-template");
const blockTemplate = document.getElementById("block-template");

let selectedFile = null;

refreshBtn.addEventListener("click", loadFiles);
uploadForm.addEventListener("submit", handleUpload);

document.addEventListener("DOMContentLoaded", loadFiles);

async function loadFiles() {
  try {
    const response = await fetch("/api/files");
    if (!response.ok) throw new Error("Unable to fetch files");
    const data = await response.json();
    renderFileList(data.files ?? []);
  } catch (err) {
    console.error(err);
    fileListEl.innerHTML = "";
    fileEmptyEl.textContent = "Failed to load files.";
    fileEmptyEl.classList.remove("hidden");
  }
}

function renderFileList(files) {
  fileListEl.innerHTML = "";
  if (!files.length) {
    fileEmptyEl.textContent = "No files stored yet.";
    fileEmptyEl.classList.remove("hidden");
    fileDetailsEl.innerHTML = '<p class="empty-state">Upload a file to see details.</p>';
    selectedFile = null;
    return;
  }

  fileEmptyEl.classList.add("hidden");
  files.forEach((file) => {
    const node = fileTemplate.content.firstElementChild.cloneNode(true);
    const button = node.querySelector(".file-item");
    button.textContent = `${file.name} (${file.total_size ?? file.totalSize} bytes)`;
    button.dataset.name = file.name;
    button.addEventListener("click", () => showFileDetails(file.name));
    if (selectedFile === file.name) {
      button.classList.add("active");
    }
    fileListEl.appendChild(node);
  });
}

async function showFileDetails(name) {
  try {
    const response = await fetch(`/api/files/${encodeURIComponent(name)}`);
    if (!response.ok) throw new Error("Unable to load file metadata");
    const { file } = await response.json();
    selectedFile = name;
    renderFileDetails(file);
    highlightSelected(name);
  } catch (err) {
    console.error(err);
    fileDetailsEl.innerHTML = `<p class="status error">${err.message}</p>`;
  }
}

function highlightSelected(name) {
  fileListEl.querySelectorAll(".file-item").forEach((item) => {
    item.classList.toggle("active", item.dataset.name === name);
  });
}

function renderFileDetails(file) {
  if (!file) {
    fileDetailsEl.innerHTML = '<p class="empty-state">File not found.</p>';
    return;
  }

  const blockRows = (file.blocks || [])
    .map((block) => {
      const row = blockTemplate.content.firstElementChild.cloneNode(true);
      const [idCell, serverCell, sizeCell] = row.children;
      idCell.textContent = block.id;
      serverCell.textContent = block.data_server || block.dataServer;
      sizeCell.textContent = `${block.size} B`;
      return row;
    });

  fileDetailsEl.innerHTML = `
    <div class="detail-header">
      <h3>${file.name}</h3>
      <p>Total size: ${file.total_size ?? file.totalSize} bytes</p>
      <button type="button" id="download-btn">Download</button>
    </div>
    <table>
      <thead>
        <tr>
          <th>Block ID</th>
          <th>Data server</th>
          <th>Size</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  `;

  const tbody = fileDetailsEl.querySelector("tbody");
  blockRows.forEach((row) => tbody.appendChild(row));

  const downloadBtn = document.getElementById("download-btn");
  downloadBtn.addEventListener("click", () => downloadFile(file.name));
}

async function downloadFile(name) {
  const btn = document.getElementById("download-btn");
  btn.disabled = true;
  try {
    const response = await fetch(`/api/files/${encodeURIComponent(name)}/content`);
    if (!response.ok) throw new Error("Download failed");
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = name;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  } catch (err) {
    alert(err.message);
  } finally {
    btn.disabled = false;
  }
}

async function handleUpload(event) {
  event.preventDefault();
  if (!fileInput.files.length) {
    uploadStatus.textContent = "Please choose a file.";
    uploadStatus.classList.add("error");
    return;
  }

  uploadStatus.textContent = "Uploading...";
  uploadStatus.className = "status";

  const formData = new FormData();
  formData.append("file", fileInput.files[0]);
  if (fileNameInput.value.trim()) {
    formData.append("name", fileNameInput.value.trim());
  }

  try {
    const response = await fetch("/api/files", {
      method: "POST",
      body: formData,
    });
    if (!response.ok) throw new Error(await response.text());
    uploadStatus.textContent = "Upload complete.";
    uploadStatus.classList.add("success");
    uploadForm.reset();
    await loadFiles();
  } catch (err) {
    uploadStatus.textContent = err.message || "Upload failed.";
    uploadStatus.classList.add("error");
  }
}
