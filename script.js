const KB_BASE = 'https://github.com/OCHA-DAP/ds-knowledge-base/blob/main/pipelines/';

async function loadData() {
  const response = await fetch('data/pipelines.json');
  return await response.json();
}

function formatDateTime(isoString) {
  const date = new Date(isoString);
  return date.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZone: 'UTC'
  }) + ' UTC';
}

function renderMarkdown(text) {
  if (!text) return '';
  return text.replace(/\[(.+?)\]\((.+?)\)/g, '<a href="$2" target="_blank">$1</a>');
}

function formatColumnType(col) {
  let type = col.type;
  if (col.max_length) {
    type += `(${col.max_length})`;
  } else if (col.precision) {
    type += col.scale ? `(${col.precision},${col.scale})` : `(${col.precision})`;
  }
  return type;
}

function formatNumber(num) {
  return num.toLocaleString();
}

function formatDateOnly(isoString) {
  const date = new Date(isoString);
  return date.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    timeZone: 'UTC'
  });
}

function formatTimestampRange(ranges) {
  if (!ranges || Object.keys(ranges).length === 0) return '';
  const items = Object.entries(ranges).map(([col, stats]) => {
    const parts = [];
    if (stats.min) parts.push(`min: ${formatDateOnly(stats.min)}`);
    if (stats.max) parts.push(`max: ${formatDateOnly(stats.max)}`);
    return `<span class="ts-range"><strong>${col}</strong>: ${parts.join(', ')}</span>`;
  });
  return items.join('');
}

function stageBadge(stage) {
  return stage === 'dev' ? '<span class="stage-badge">dev</span>' : '';
}

function writesToDev(pipeline) {
  const outputs = [...(pipeline.output_schemas || []), ...(pipeline.blob_storage || [])];
  return outputs.length ? outputs.some(o => o.stage === 'dev') : pipeline.data_mode === 'dev';
}

function renderBlobStorage(blobStorage) {
  if (!blobStorage) return '';

  const blobSize = blobStorage.blob_size_gb !== undefined ? `${blobStorage.blob_size_gb} GB` :
                   blobStorage.blob_size_mb !== undefined ? `${blobStorage.blob_size_mb} MB` : null;
  const blobCount = blobStorage.blob_count !== undefined ? formatNumber(blobStorage.blob_count) : null;

  return `
    <div class="blob-storage-info">
      <h3>Azure Blob Storage ${stageBadge(blobStorage.stage)}</h3>
      <div class="schema-stats">
        <span class="stat-item"><strong>Container:</strong> ${blobStorage.container}</span>
        ${blobStorage.prefix ? `<span class="stat-item"><strong>Prefix:</strong> ${blobStorage.prefix}</span>` : ''}
        ${blobSize ? `<span class="stat-item"><strong>Total Size:</strong> ${blobSize}</span>` : ''}
        ${blobCount ? `<span class="stat-item"><strong>Blob Count:</strong> ${blobCount}</span>` : ''}
      </div>
    </div>
  `;
}

function renderSchemaTable(schema) {
  const columnsHtml = schema.columns.map(col => `
    <tr>
      <td>${col.name}</td>
      <td>${formatColumnType(col)}</td>
      <td>${col.nullable ? 'Yes' : 'No'}</td>
      <td>${col.comment || ''}</td>
    </tr>
  `).join('');

  const rowCount = schema.row_count !== undefined ? formatNumber(schema.row_count) : null;
  const tsRanges = formatTimestampRange(schema.timestamp_ranges);
  const tableSize = schema.size_gb !== undefined ? `${schema.size_gb} GB` :
                    schema.size_mb !== undefined ? `${schema.size_mb} MB` : null;

  const statsHtml = (rowCount || tsRanges || tableSize) ? `
    <div class="schema-stats">
      ${rowCount ? `<span class="stat-item"><strong>Rows:</strong> ${rowCount}</span>` : ''}
      ${tableSize ? `<span class="stat-item"><strong>Size:</strong> ${tableSize}</span>` : ''}
      ${tsRanges}
    </div>
  ` : '';

  return `
    <div class="schema-table">
      <h3>${schema.table} ${stageBadge(schema.stage)}</h3>
      ${statsHtml}
      <table>
        <thead>
          <tr>
            <th>Column</th>
            <th>Type</th>
            <th>Nullable</th>
            <th>Description</th>
          </tr>
        </thead>
        <tbody>
          ${columnsHtml}
        </tbody>
      </table>
    </div>
  `;
}

function showSchemaModal(pipeline) {
  const modal = document.getElementById('schema-modal');
  const title = document.getElementById('modal-title');
  const body = document.getElementById('modal-body');

  title.textContent = `${pipeline.name} - Output Schema`;

  let content = '';

  // Add blob storage info if available
  if (pipeline.blob_storage?.length) {
    content += pipeline.blob_storage.map(renderBlobStorage).join('');
  }

  // Add schema tables
  if (pipeline.output_schemas && pipeline.output_schemas.length > 0) {
    content += pipeline.output_schemas.map(renderSchemaTable).join('');
  } else if (!pipeline.blob_storage?.length) {
    content = '<p>No schema information available.</p>';
  }

  body.innerHTML = content;
  modal.classList.add('active');
}

function hideSchemaModal() {
  const modal = document.getElementById('schema-modal');
  modal.classList.remove('active');
}

function setupModalListeners() {
  const modal = document.getElementById('schema-modal');
  const closeBtn = modal.querySelector('.modal-close');

  closeBtn.addEventListener('click', hideSchemaModal);

  modal.addEventListener('click', (e) => {
    if (e.target === modal) {
      hideSchemaModal();
    }
  });

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      hideSchemaModal();
    }
  });
}

function formatDuration(sec) {
  if (sec < 60) return `${sec}s`;
  const min = Math.round(sec / 60);
  return min < 60 ? `${min}m` : `${Math.floor(min / 60)}h ${String(min % 60).padStart(2, '0')}m`;
}

function renderRuntime(pipeline) {
  const d = pipeline.duration;
  if (!d) return '<span class="muted">-</span>';
  return `<span class="runtime">${formatDuration(d.median_sec)}</span> <span class="muted">${formatDuration(d.p25_sec)}–${formatDuration(d.p75_sec)}</span>`;
}

let runtimeSort = null;

function sortByRuntime(pipelines) {
  if (!runtimeSort) return pipelines;
  const dir = runtimeSort === 'desc' ? -1 : 1;
  return [...pipelines].sort((a, b) => {
    if (!a.duration) return b.duration ? 1 : 0;
    if (!b.duration) return -1;
    return dir * (a.duration.median_sec - b.duration.median_sec);
  });
}

function setupRuntimeSort() {
  const header = document.getElementById('sort-runtime');
  header.querySelector('button').addEventListener('click', () => {
    runtimeSort = runtimeSort === 'desc' ? 'asc' : runtimeSort === 'asc' ? null : 'desc';
    header.setAttribute('aria-sort', { desc: 'descending', asc: 'ascending' }[runtimeSort] || 'none');
    header.querySelector('.sort-arrow').textContent = { desc: '↓', asc: '↑' }[runtimeSort] || '↕';
    applyFilters();
  });
}

function renderTable(pipelines) {
  const tbody = document.getElementById('table-body');
  tbody.innerHTML = '';

  pipelines.forEach(pipeline => {
    const row = document.createElement('tr');

    const lastRun = pipeline.last_run;

    const tasks = pipeline.tasks || [];
    const hasSchemas = pipeline.output_schemas?.length > 0 || pipeline.blob_storage?.length > 0;

    // Render tasks with links to their git repos
    const tasksHtml = tasks.map(task => {
      if (task.git_url) {
        return `<a href="${task.git_url}" class="task-link" target="_blank">${task.name}</a>`;
      }
      return `<span class="task-item">${task.name}</span>`;
    }).join('');

    row.innerHTML = `
      <td class="pipeline-name ${hasSchemas ? 'clickable' : ''}">${pipeline.name}</td>
      <td class="description">${renderMarkdown(pipeline.description)}</td>
      <td>
        <div class="tasks-list">
          ${tasksHtml}
        </div>
      </td>
      <td class="schedule">${pipeline.schedule || '-'}</td>
      <td>${renderRuntime(pipeline)}</td>
      <td>
        <span class="status ${lastRun?.status || 'unknown'}">
          <span class="status-dot"></span>
          ${lastRun?.status || 'Unknown'}
        </span>
      </td>
      <td>
        <div class="tags">
          ${pipeline.tags.map(t => `<span class="tag type" data-filter="type" data-value="${t}">${t}</span>`).join('')}
          ${(pipeline.hazard || []).map(t => `<span class="tag hazard" data-filter="hazard" data-value="${t}">${t}</span>`).join('')}
          ${pipeline.kb ? `<a class="tag kb" href="${KB_BASE}${pipeline.kb}.md" target="_blank">${pipeline.kb}</a>` : ''}
          ${writesToDev(pipeline) ? '<span class="tag stage">dev</span>' : ''}
        </div>
      </td>
    `;

    // Add click handler for pipelines with schemas
    if (hasSchemas) {
      const nameCell = row.querySelector('.pipeline-name');
      nameCell.addEventListener('click', () => showSchemaModal(pipeline));
    }

    row.querySelectorAll('[data-filter]').forEach(chip => {
      chip.addEventListener('click', () => {
        document.getElementById(`filter-${chip.dataset.filter}`).value = chip.dataset.value;
        applyFilters();
      });
    });

    tbody.appendChild(row);
  });
}

let allPipelines = [];

function fillSelect(id, values) {
  const select = document.getElementById(id);
  [...new Set(values)].sort().forEach(v => select.add(new Option(v, v)));
}

function readFilters() {
  return {
    search: document.getElementById('filter-search').value.trim().toLowerCase(),
    type: document.getElementById('filter-type').value,
    hazard: document.getElementById('filter-hazard').value,
    status: document.getElementById('filter-status').value,
  };
}

function matchesFilters(pipeline, f) {
  if (f.search && !pipeline.name.toLowerCase().includes(f.search)) return false;
  if (f.type && !pipeline.tags.includes(f.type)) return false;
  if (f.hazard && !(pipeline.hazard || []).includes(f.hazard)) return false;
  if (f.status && (pipeline.last_run?.status || 'unknown') !== f.status) return false;
  return true;
}

function applyFilters() {
  const f = readFilters();
  const shown = allPipelines.filter(p => matchesFilters(p, f));
  renderTable(sortByRuntime(shown));
  const active = Object.values(f).some(Boolean);
  document.getElementById('filter-clear').hidden = !active;
  document.getElementById('filter-count').textContent =
    active ? `${shown.length} of ${allPipelines.length} jobs` : `${allPipelines.length} jobs`;
}

function setupFilters() {
  fillSelect('filter-type', allPipelines.flatMap(p => p.tags));
  fillSelect('filter-hazard', allPipelines.flatMap(p => p.hazard || []));
  fillSelect('filter-status', allPipelines.map(p => p.last_run?.status || 'unknown'));
  ['filter-search', 'filter-type', 'filter-hazard', 'filter-status'].forEach(id => {
    document.getElementById(id).addEventListener('input', applyFilters);
  });
  document.getElementById('filter-clear').addEventListener('click', () => {
    document.getElementById('filter-search').value = '';
    ['filter-type', 'filter-hazard', 'filter-status'].forEach(id => { document.getElementById(id).value = ''; });
    applyFilters();
  });
}

async function init() {
  const data = await loadData();
  document.getElementById('last-updated').textContent =
    `Last updated: ${formatDateTime(data.generated_at)}`;
  allPipelines = data.pipelines;
  setupFilters();
  setupRuntimeSort();
  applyFilters();
  setupModalListeners();
}

init();
