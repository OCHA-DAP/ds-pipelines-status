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

    // Render tasks with links to their git repos
    const tasksHtml = tasks.map(task => {
      if (task.git_url) {
        return `<a href="${task.git_url}" class="task-link" target="_blank">${task.name}</a>`;
      }
      return `<span class="task-item">${task.name}</span>`;
    }).join('');

    row.innerHTML = `
      <td class="pipeline-name">${pipeline.name}</td>
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

    tbody.appendChild(row);
  });
}

async function init() {
  const data = await loadData();
  renderTable(data);
}

init();
