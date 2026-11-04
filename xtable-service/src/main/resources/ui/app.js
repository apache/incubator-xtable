/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

var API = '/v1/conversion';
var POLL_MS = 1500;
var timers = [];

function api(path, options) {
  return fetch(API + path, options).then(function (res) {
    if (!res.ok && res.status !== 202) {
      return res.text().then(function (body) {
        throw new Error(body || res.status + ' ' + res.statusText);
      });
    }
    if (res.status === 204 || res.status === 202) {
      return null;
    }
    return res.json();
  });
}

function el(tag, attrs, children) {
  var node = document.createElement(tag);
  Object.keys(attrs || {}).forEach(function (k) {
    if (k === 'class') {
      node.className = attrs[k];
    } else if (k === 'text') {
      node.textContent = attrs[k];
    } else {
      node.setAttribute(k, attrs[k]);
    }
  });
  (children || []).forEach(function (c) {
    node.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
  });
  return node;
}

function clearTimers() {
  timers.forEach(clearInterval);
  timers = [];
}

function mount(templateId) {
  clearTimers();
  var view = document.getElementById('view');
  view.innerHTML = '';
  view.appendChild(document.getElementById(templateId).content.cloneNode(true));
  return view;
}

function duration(ms) {
  if (ms === null || ms === undefined) {
    return '';
  }
  if (ms < 1000) {
    return ms + 'ms';
  }
  if (ms < 60000) {
    return (ms / 1000).toFixed(1) + 's';
  }
  return Math.floor(ms / 60000) + 'm ' + Math.round((ms % 60000) / 1000) + 's';
}

function clockTime(iso) {
  return iso ? new Date(iso).toLocaleTimeString() : '';
}

/* ---------------------------------------------------------------- runs list */

function renderRuns(runs) {
  var host = document.getElementById('runs');
  if (!host) {
    return;
  }
  host.innerHTML = '';
  if (!runs.length) {
    host.appendChild(
      el('p', {
        class: 'empty',
        text: 'No conversions yet. Start one from "New conversion".'
      })
    );
    return;
  }
  var head = el('tr', {}, [
    el('th', { text: 'Status' }),
    el('th', { text: 'Source' }),
    el('th', { text: 'Targets' }),
    el('th', { text: 'Started' }),
    el('th', { text: 'Duration' })
  ]);
  var body = el('tbody');
  runs.forEach(function (run) {
    var targets = el('td');
    (run['target-formats'] || []).forEach(function (f) {
      targets.appendChild(el('span', { class: 'fmt', text: f }));
    });
    var row = el('tr', {}, [
      el('td', {}, [el('span', { class: 'badge ' + run.status, text: run.status })]),
      el('td', {}, [
        el('span', { class: 'fmt', text: run['source-format'] }),
        el('span', { text: ' ' + (run['source-table-name'] || '') })
      ]),
      targets,
      el('td', { text: clockTime(run['started-at']) }),
      el('td', { text: duration(run['duration-millis']) })
    ]);
    row.addEventListener('click', function () {
      window.location.hash = '#/runs/' + run['conversion-id'];
    });
    body.appendChild(row);
  });
  host.appendChild(el('table', {}, [el('thead', {}, [head]), body]));
}

function viewConversions() {
  mount('tpl-conversions');
  document.querySelector('[data-action=new]').addEventListener('click', function () {
    window.location.hash = '#/new';
  });
  var refresh = function () {
    api('/runs').then(renderRuns).catch(function (e) {
      console.error(e);
    });
  };
  refresh();
  timers.push(
    setInterval(function () {
      if (document.getElementById('auto-refresh').checked) {
        refresh();
      }
    }, 3000)
  );
}

/* ----------------------------------------------------------- new conversion */

function viewNew() {
  mount('tpl-new');
  var form = document.getElementById('convert-form');
  var partitionField = document.getElementById('partition-spec-field');
  var sourceSelect = form.elements['source-format'];

  // partition-spec is read only for a Hudi source, so do not offer it otherwise.
  var syncPartitionVisibility = function () {
    partitionField.style.display = sourceSelect.value === 'HUDI' ? '' : 'none';
  };
  sourceSelect.addEventListener('change', syncPartitionVisibility);
  syncPartitionVisibility();

  form.addEventListener('submit', function (event) {
    event.preventDefault();
    var errorBox = document.getElementById('form-error');
    errorBox.textContent = '';

    var targets = Array.prototype.slice
      .call(form.querySelectorAll('input[name=target]:checked'))
      .map(function (cb) {
        return cb.value;
      });
    if (!targets.length) {
      errorBox.textContent = 'Pick at least one target format.';
      return;
    }

    var dataPath = form.elements['source-data-path'].value.trim();
    var tablePath = form.elements['source-table-path'].value.trim();
    var body = {
      'source-format': sourceSelect.value,
      'source-table-name': form.elements['source-table-name'].value.trim(),
      'source-table-path': tablePath,
      // The service writes target metadata at the data path, so default it to the table path.
      'source-data-path': dataPath || tablePath,
      'target-formats': targets
    };
    var partitionSpec = form.elements['partition-spec'].value.trim();
    if (sourceSelect.value === 'HUDI' && partitionSpec) {
      body.configurations = { 'partition-spec': partitionSpec };
    }

    api('/table', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Prefer: 'respond-async' },
      body: JSON.stringify(body)
    })
      .then(function () {
        // 202 has no parsed body here, so pick the run up from the list.
        return api('/runs');
      })
      .then(function (runs) {
        window.location.hash = runs && runs.length
          ? '#/runs/' + runs[0]['conversion-id']
          : '#/conversions';
      })
      .catch(function (e) {
        errorBox.textContent = e.message;
      });
  });
}

/* ------------------------------------------------------------- run detail */

function renderSummary(run) {
  var host = document.getElementById('run-summary');
  host.innerHTML = '';
  var grid = el('dl', { class: 'summary-grid' });
  var add = function (label, node) {
    grid.appendChild(el('dt', { text: label }));
    grid.appendChild(el('dd', {}, [node]));
  };
  add('Status', el('span', { class: 'badge ' + run.status, text: run.status }));
  add('Source', el('span', {
    text: run['source-format'] + ' ' + (run['source-table-name'] || '')
  }));
  add('Path', el('span', { class: 'mono', text: run['source-table-path'] || '' }));
  add('Targets', el('span', { text: (run['target-formats'] || []).join(', ') }));
  add('Started', el('span', { text: clockTime(run['started-at']) }));
  add('Duration', el('span', { text: duration(run['duration-millis']) || 'running' }));
  host.appendChild(grid);
  if (run.error) {
    host.appendChild(el('p', { class: 'error', text: run.error }));
  }
}

function renderResult(run) {
  var host = document.getElementById('run-result');
  host.innerHTML = '';
  var result = run.result;
  if (!result || !result.convertedTables || !result.convertedTables.length) {
    return;
  }
  host.appendChild(el('h2', { text: 'Converted tables' }));
  var body = el('tbody');
  result.convertedTables.forEach(function (t) {
    body.appendChild(
      el('tr', {}, [
        el('td', {}, [el('span', { class: 'fmt', text: t['target-format'] })]),
        el('td', { class: 'mono', text: t['target-metadata-path'] || '' })
      ])
    );
  });
  host.appendChild(
    el('table', {}, [
      el('thead', {}, [
        el('tr', {}, [el('th', { text: 'Format' }), el('th', { text: 'Metadata path' })])
      ]),
      body
    ])
  );
}

function viewRun(conversionId) {
  mount('tpl-run');
  document.getElementById('run-id').textContent = conversionId;
  var eventsHost = document.getElementById('run-events');
  var lastSequence = 0;
  var finished = false;

  var pollEvents = function () {
    return api('/runs/' + conversionId + '/events?after=' + lastSequence).then(function (events) {
      (events || []).forEach(function (ev) {
        lastSequence = Math.max(lastSequence, ev.sequence);
        eventsHost.appendChild(
          el('div', { class: 'ev' }, [
            el('span', { class: 'ts', text: clockTime(ev.timestamp) }),
            el('span', { class: ev.level, text: ev.message })
          ])
        );
      });
      eventsHost.scrollTop = eventsHost.scrollHeight;
    });
  };

  var poll = function () {
    api('/runs/' + conversionId)
      .then(function (run) {
        renderSummary(run);
        renderResult(run);
        return pollEvents().then(function () {
          if (run.status !== 'RUNNING' && !finished) {
            finished = true;
            clearTimers();
          }
        });
      })
      .catch(function (e) {
        eventsHost.appendChild(el('div', { class: 'ev' }, [
          el('span', { class: 'ERROR', text: e.message })
        ]));
        clearTimers();
      });
  };

  poll();
  timers.push(setInterval(poll, POLL_MS));
}

/* ------------------------------------------------------------------ router */

function route() {
  var hash = window.location.hash || '#/conversions';
  var runMatch = hash.match(/^#\/runs\/(.+)$/);

  document.querySelectorAll('.topbar nav a').forEach(function (a) {
    a.classList.remove('active');
  });

  if (runMatch) {
    viewRun(runMatch[1]);
    return;
  }
  if (hash === '#/new') {
    document.querySelector('[data-route=new]').classList.add('active');
    viewNew();
    return;
  }
  document.querySelector('[data-route=conversions]').classList.add('active');
  viewConversions();
}

window.addEventListener('hashchange', route);
window.addEventListener('DOMContentLoaded', route);
