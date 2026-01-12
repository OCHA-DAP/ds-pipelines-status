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

function formatRelativeTime(isoString) {
  const date = new Date(isoString);
  const now = new Date();
  const diffMs = now - date;
  const diffMins = Math.floor(diffMs / 60000);
  const diffHours = Math.floor(diffMins / 60);
  const diffDays = Math.floor(diffHours / 24);

  if (diffMins < 60) return `${diffMins}m ago`;
  if (diffHours < 24) return `${diffHours}h ago`;
  return `${diffDays}d ago`;
}

function formatFutureTime(isoString) {
  const date = new Date(isoString);
  const now = new Date();
  const diffMs = date - now;
  const diffMins = Math.floor(diffMs / 60000);
  const diffHours = Math.floor(diffMins / 60);
  const diffDays = Math.floor(diffHours / 24);

  if (diffMins < 0) return 'now';
  if (diffMins < 60) return `in ${diffMins}m`;
  if (diffHours < 24) return `in ${diffHours}h`;
  return `in ${diffDays}d`;
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

  const statsHtml = (rowCount || tsRanges) ? `
    <div class="schema-stats">
      ${rowCount ? `<span class="stat-item"><strong>Rows:</strong> ${rowCount}</span>` : ''}
      ${tsRanges}
    </div>
  ` : '';

  return `
    <div class="schema-table">
      <h3>${schema.table}</h3>
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

  if (pipeline.output_schemas && pipeline.output_schemas.length > 0) {
    body.innerHTML = pipeline.output_schemas.map(renderSchemaTable).join('');
  } else {
    body.innerHTML = '<p>No schema information available.</p>';
  }

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

function renderTable(data) {
  const tbody = document.getElementById('table-body');
  tbody.innerHTML = '';

  document.getElementById('last-updated').textContent =
    `Last updated: ${formatDateTime(data.generated_at)}`;

  data.pipelines.forEach(pipeline => {
    const row = document.createElement('tr');

    // Add development class if job has status: development tag
    if (pipeline.job_status === 'development') {
      row.classList.add('development');
    }

    const lastRun = pipeline.last_run;
    const runTime = lastRun ? formatRelativeTime(lastRun.end || lastRun.start) : 'Never';
    const duration = lastRun?.duration_min ? `${lastRun.duration_min}m` : '';
    const nextRun = pipeline.next_run ? formatFutureTime(pipeline.next_run) : '-';

    const tasks = pipeline.tasks || [];
    const hasSchemas = pipeline.output_schemas && pipeline.output_schemas.length > 0;

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
      <td>
        <div class="timing">${runTime}</div>
        ${duration ? `<div class="duration">${duration}</div>` : ''}
      </td>
      <td>
        <div class="timing">${nextRun}</div>
      </td>
      <td>
        <span class="status ${lastRun?.status || 'unknown'}">
          <span class="status-dot"></span>
          ${lastRun?.status || 'Unknown'}
        </span>
      </td>
      <td>
        <div class="tags">
          ${pipeline.tags.map(t => `<span class="tag ${t}">${t}</span>`).join('')}
        </div>
      </td>
    `;

    // Add click handler for pipelines with schemas
    if (hasSchemas) {
      const nameCell = row.querySelector('.pipeline-name');
      nameCell.addEventListener('click', () => showSchemaModal(pipeline));
    }

    tbody.appendChild(row);
  });
}

async function init() {
  const data = await loadData();
  renderTable(data);
  setupModalListeners();
}

init();
