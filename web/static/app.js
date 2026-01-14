let currentTable = '';
let currentCursor = '';
let cursorHistory = [];
let currentPage = 1;
let isSearchMode = false;
let currentSearchTerm = '';
let tableSchema = null;
let searchableColumns = [];
let clickhouseAvailable = false;

async function loadTables() {
    try {
        const response = await fetch('/api/tables');
        const data = await response.json();
        
        document.getElementById('total-tables').textContent = data.count;
        
        const select = document.getElementById('table-select');
        select.innerHTML = '<option value="">Select a table...</option>';
        
        for (const table of data.tables) {
            const option = document.createElement('option');
            option.value = table.name;
            option.textContent = `${table.name} (${table.columns} columns)`;
            select.appendChild(option);
        }

        await loadHealthStatus();
        await loadCDCStatus();
    } catch (error) {
        console.error('Error loading tables:', error);
    }
}

async function loadHealthStatus() {
    try {
        const response = await fetch('/api/health');
        const health = await response.json();
        
        clickhouseAvailable = health.clickhouse;
        
        const redisStatus = document.getElementById('redis-status');
        redisStatus.textContent = health.redis ? 'Connected' : 'Not Available';
        redisStatus.className = 'status-badge ' + (health.redis ? 'status-ok' : 'status-off');
        
        const chStatus = document.getElementById('clickhouse-status');
        chStatus.textContent = health.clickhouse ? 'Connected' : 'Not Available';
        chStatus.className = 'status-badge ' + (health.clickhouse ? 'status-ok' : 'status-off');
    } catch (error) {
        console.error('Error loading health status:', error);
    }
}

async function loadCDCStatus() {
    try {
        const response = await fetch('/api/cdc/status');
        const cdc = await response.json();
        
        const cdcStatus = document.getElementById('cdc-status');
        const cdcTables = document.getElementById('cdc-tables');
        
        if (cdc.is_running) {
            cdcStatus.textContent = 'Running';
            cdcStatus.className = 'status-badge status-ok';
            cdcTables.textContent = `(${cdc.total_tables} tables)`;
        } else if (cdc.available === false) {
            cdcStatus.textContent = 'Disabled';
            cdcStatus.className = 'status-badge status-off';
            cdcTables.textContent = '';
        } else {
            cdcStatus.textContent = 'Stopped';
            cdcStatus.className = 'status-badge status-off';
            cdcTables.textContent = '';
        }
    } catch (error) {
        console.error('Error loading CDC status:', error);
        const cdcStatus = document.getElementById('cdc-status');
        cdcStatus.textContent = 'Unknown';
        cdcStatus.className = 'status-badge status-unknown';
    }
}

async function selectTable() {
    const select = document.getElementById('table-select');
    currentTable = select.value;
    
    if (!currentTable) {
        document.getElementById('current-table').textContent = '-';
        document.getElementById('total-records').textContent = '-';
        document.getElementById('total-columns').textContent = '-';
        return;
    }
    
    document.getElementById('current-table').textContent = currentTable;
    
    cursorHistory = [];
    currentCursor = '';
    currentPage = 1;
    isSearchMode = false;
    
    await loadTableSchema();
    await loadTableStats();
    await loadRecords();
}

async function loadTableSchema() {
    try {
        const response = await fetch(`/api/tables/${currentTable}/schema`);
        tableSchema = await response.json();
        
        document.getElementById('total-columns').textContent = tableSchema.columns.length;
        
        searchableColumns = tableSchema.searchable || [];
        
        const sortSelect = document.getElementById('sort-by');
        sortSelect.innerHTML = '<option value="">Sort by...</option>';
        for (const col of tableSchema.sortable || []) {
            const option = document.createElement('option');
            option.value = col;
            option.textContent = col;
            sortSelect.appendChild(option);
        }
        
        const thead = document.getElementById('table-head');
        thead.innerHTML = '<tr>' + tableSchema.columns.map(col => 
            `<th>${escapeHtml(col.name)}<br><small>${col.data_type}</small></th>`
        ).join('') + '</tr>';
        
    } catch (error) {
        console.error('Error loading schema:', error);
    }
}

async function loadTableStats() {
    try {
        const response = await fetch(`/api/tables/${currentTable}/stats`);
        const stats = await response.json();
        
        const count = stats.estimated_count || stats.total_count || 0;
        const countType = stats.count_type === 'estimated' ? '~' : '';
        document.getElementById('total-records').textContent = countType + formatNumber(count);
    } catch (error) {
        console.error('Error loading stats:', error);
    }
}

async function loadRecords(cursor = '') {
    if (!currentTable) return;
    
    isSearchMode = false;
    currentSearchTerm = '';
    
    const sortBy = document.getElementById('sort-by').value;
    const sortDir = document.getElementById('sort-dir').value;
    
    let url = `/api/tables/${currentTable}/records?limit=20`;
    if (sortBy) url += `&sort_by=${sortBy}`;
    if (sortDir) url += `&sort_dir=${sortDir}`;
    if (cursor) url += `&cursor=${encodeURIComponent(cursor)}`;
    
    try {
        const response = await fetch(url);
        const data = await response.json();
        renderRecords(data);
        updatePagination(data, cursor);
    } catch (error) {
        console.error('Error loading records:', error);
    }
}

async function searchRecords() {
    if (!currentTable) {
        alert('Please select a table first.');
        return;
    }
    
    const searchInput = document.getElementById('search-input');
    const term = searchInput.value.trim();
    
    if (term.length < 2) {
        alert('Please enter at least 2 characters to search.');
        return;
    }
    
    isSearchMode = true;
    currentSearchTerm = term;
    currentCursor = '';
    cursorHistory = [];
    currentPage = 1;
    
    await performSearch(term, '');
}

function getSelectedEngine() {
    const radios = document.querySelectorAll('input[name="search-engine"]');
    for (const radio of radios) {
        if (radio.checked) {
            return radio.value;
        }
    }
    return 'auto';
}

async function performSearch(term, cursor) {
    let url = `/api/tables/${currentTable}/search?q=${encodeURIComponent(term)}&limit=20`;
    if (cursor) url += `&cursor=${encodeURIComponent(cursor)}`;
    
    const engine = getSelectedEngine();
    if (engine === 'postgresql') {
        url += '&engine=postgresql';
    } else if (engine === 'clickhouse' && clickhouseAvailable) {
        url += '&engine=clickhouse';
    }
    
    try {
        const response = await fetch(url);
        const data = await response.json();
        
        renderRecords(data);
        updatePagination(data, cursor);
        
        const searchInfo = document.getElementById('search-info');
        if (searchInfo && data.search_engine) {
            searchInfo.textContent = `Searched via ${data.search_engine.toUpperCase()}`;
            searchInfo.className = data.search_engine === 'clickhouse' ? 'search-info search-fast' : 'search-info search-pg';
        }
    } catch (error) {
        console.error('Error searching records:', error);
    }
}

function renderRecords(data) {
    const tbody = document.getElementById('records-body');
    
    if (!data.data || data.data.length === 0) {
        tbody.innerHTML = '<tr><td colspan="20" style="text-align: center; padding: 2rem;">No records found</td></tr>';
        return;
    }
    
    if (!tableSchema) {
        tbody.innerHTML = '<tr><td colspan="20" style="text-align: center; padding: 2rem;">Loading schema...</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.data.map(record => {
        const cells = tableSchema.columns.map(col => {
            const value = record[col.name];
            return `<td>${formatValue(value, col.data_type)}</td>`;
        }).join('');
        return `<tr>${cells}</tr>`;
    }).join('');
}

function formatValue(value, dataType) {
    if (value === null || value === undefined) {
        return '<span style="color:#666">null</span>';
    }
    
    if (dataType.includes('timestamp') || dataType.includes('date')) {
        const date = new Date(value);
        return date.toLocaleString();
    }
    
    if (dataType.includes('numeric') || dataType.includes('decimal')) {
        return parseFloat(value).toFixed(2);
    }
    
    if (typeof value === 'object') {
        return escapeHtml(JSON.stringify(value));
    }
    
    const strVal = String(value);
    if (strVal.length > 100) {
        return escapeHtml(strVal.substring(0, 100)) + '...';
    }
    
    return escapeHtml(strVal);
}

function updatePagination(data, cursor) {
    const prevBtn = document.getElementById('prev-btn');
    const nextBtn = document.getElementById('next-btn');
    const pageInfo = document.getElementById('page-info');
    
    prevBtn.disabled = cursorHistory.length === 0;
    nextBtn.disabled = !data.has_more;
    
    if (data.next_cursor) {
        currentCursor = data.next_cursor;
    }
    
    pageInfo.textContent = `Page ${currentPage}`;
}

function prevPage() {
    if (cursorHistory.length > 0) {
        currentPage--;
        cursorHistory.pop();
        const prevCursor = cursorHistory.length > 0 ? cursorHistory[cursorHistory.length - 1] : '';
        
        if (isSearchMode) {
            performSearch(currentSearchTerm, prevCursor);
        } else {
            loadRecords(prevCursor);
        }
    }
}

function nextPage() {
    if (currentCursor) {
        currentPage++;
        cursorHistory.push(currentCursor);
        
        if (isSearchMode) {
            performSearch(currentSearchTerm, currentCursor);
        } else {
            loadRecords(currentCursor);
        }
    }
}

function formatNumber(num) {
    return new Intl.NumberFormat().format(num);
}

function escapeHtml(text) {
    if (text === null || text === undefined) return '';
    const div = document.createElement('div');
    div.textContent = String(text);
    return div.innerHTML;
}

document.getElementById('search-input').addEventListener('keypress', function(e) {
    if (e.key === 'Enter') {
        searchRecords();
    }
});

document.addEventListener('DOMContentLoaded', function() {
    loadTables();
});
