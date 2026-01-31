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
        select.innerHTML = '<option value="">-- Select a table to view records --</option>';
        
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
        const response = await fetch('/health');
        const health = await response.json();
        
        clickhouseAvailable = health.clickhouse || false;
        
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
    
    // Hide global results when selecting a table
    document.getElementById('global-results').style.display = 'none';
    document.getElementById('table-container').style.display = 'block';
    
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
        sortSelect.innerHTML = '<option value="">Default</option>';
        
        for (const col of tableSchema.sortable || []) {
            const option = document.createElement('option');
            option.value = col;
            option.textContent = col;
            sortSelect.appendChild(option);
        }
        
        const thead = document.getElementById('table-head');
        thead.innerHTML = '';
        const headerRow = document.createElement('tr');
        
        for (const col of tableSchema.columns) {
            const th = document.createElement('th');
            th.textContent = col.name;
            const small = document.createElement('small');
            small.textContent = col.type;
            th.appendChild(small);
            headerRow.appendChild(th);
        }
        
        thead.appendChild(headerRow);
        
        document.getElementById('sort-controls').style.display = 'flex';
        document.getElementById('search-options').style.display = 'flex';
    } catch (error) {
        console.error('Error loading table schema:', error);
    }
}

async function loadTableStats() {
    try {
        const response = await fetch(`/api/tables/${currentTable}/stats`);
        const stats = await response.json();
        
        const totalRecords = stats.total_records || stats.count || 0;
        document.getElementById('total-records').textContent = totalRecords.toLocaleString();
    } catch (error) {
        console.error('Error loading table stats:', error);
        document.getElementById('total-records').textContent = 'Error';
    }
}

async function loadRecords() {
    if (!currentTable) return;
    
    try {
        const sortBy = document.getElementById('sort-by').value;
        const sortOrder = document.getElementById('sort-order').value;
        
        let url = `/api/tables/${currentTable}/records?limit=20`;
        if (currentCursor) url += `&cursor=${currentCursor}`;
        if (sortBy) url += `&sort=${sortBy}&order=${sortOrder}`;
        
        const response = await fetch(url);
        const data = await response.json();
        
        displayRecords(data.data || data.records || []);
        
        document.getElementById('prev-btn').disabled = currentPage === 1;
        document.getElementById('next-btn').disabled = !data.next_cursor;
        document.getElementById('page-info').textContent = `Page ${currentPage} (${data.count || data.data?.length || 0} records)`;
        
        if (data.next_cursor) {
            currentCursor = data.next_cursor;
        }
    } catch (error) {
        console.error('Error loading records:', error);
    }
}

async function searchRecords() {
    const searchTerm = document.getElementById('search-input').value.trim();
    
    if (!searchTerm || !currentTable) {
        await loadRecords();
        return;
    }
    
    const engine = document.querySelector('input[name="engine"]:checked')?.value || 'auto';
    const caseSensitive = document.getElementById('case-sensitive')?.checked || false;
    
    try {
        const startTime = performance.now();
        let url = `/api/tables/${currentTable}/search?q=${encodeURIComponent(searchTerm)}&engine=${engine}&limit=20`;
        if (caseSensitive) url += '&case_sensitive=true';
        
        const response = await fetch(url);
        const data = await response.json();
        const endTime = performance.now();
        
        displayRecords(data.results || data.data || []);
        
        const searchSpeed = document.getElementById('search-speed');
        const time = (endTime - startTime).toFixed(2);
        const engineUsed = data.engine || engine || 'unknown';
        searchSpeed.innerHTML = `Search completed in <span class="${engineUsed === 'clickhouse' ? 'search-fast' : 'search-pg'}">${time}ms</span> using <strong>${engineUsed}</strong>`;
        
        document.getElementById('page-info').textContent = `${data.count || data.results?.length || 0} results found`;
        document.getElementById('prev-btn').disabled = true;
        document.getElementById('next-btn').disabled = true;
        
        isSearchMode = true;
        currentSearchTerm = searchTerm;
    } catch (error) {
        console.error('Error searching records:', error);
    }
}

// ✅ NEW: GLOBAL SEARCH FUNCTION
// ✅ FIXED: GLOBAL SEARCH FUNCTION with better error handling
async function performGlobalSearch() {
    const searchTerm = document.getElementById('global-search-input').value.trim();
    
    if (!searchTerm) {
        alert('Please enter a search term');
        return;
    }
    
    const globalResults = document.getElementById('global-results');
    const tableContainer = document.getElementById('table-container');
    
    globalResults.style.display = 'block';
    tableContainer.style.display = 'none';
    globalResults.innerHTML = '<p style="text-align:center;color:#888;padding:2rem;">🔍 Searching across all tables...</p>';
    
    try {
        const startTime = performance.now();
        const response = await fetch(`/api/search?q=${encodeURIComponent(searchTerm)}`);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        const endTime = performance.now();
        const time = (endTime - startTime).toFixed(2);
        
        // 🔍 DEBUG: Log the response
        console.log('Global Search Response:', data);
        
        // ✅ Handle different response formats
        let results = {};
        let totalResults = 0;
        let tablesSearched = 0;
        
        // Format 1: {results: {table1: [...], table2: [...]}, total_results: N}
        if (data.results && typeof data.results === 'object') {
            results = data.results;
            totalResults = data.total_results || 0;
            tablesSearched = data.tables_searched || Object.keys(results).length;
        }
        // Format 2: {data: {table1: [...], table2: [...]}}
        else if (data.data && typeof data.data === 'object') {
            results = data.data;
            for (const table in results) {
                if (Array.isArray(results[table])) {
                    totalResults += results[table].length;
                }
            }
            tablesSearched = Object.keys(results).length;
        }
        // Format 3: Direct {table1: [...], table2: [...]}
        else {
            // Remove metadata fields
            const metadataFields = ['total_results', 'tables_searched', 'search_term', 'timestamp', 'engine'];
            results = {...data};
            metadataFields.forEach(field => delete results[field]);
            
            for (const table in results) {
                if (Array.isArray(results[table])) {
                    totalResults += results[table].length;
                }
            }
            tablesSearched = Object.keys(results).length;
            totalResults = data.total_results || totalResults;
            tablesSearched = data.tables_searched || tablesSearched;
        }
        
        // ✅ Build HTML
        let html = `
            <div style="background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.1); border-radius: 12px; padding: 1.5rem; margin-bottom: 2rem;">
                <h2 style="color: #00d9ff; margin-bottom: 0.5rem;">🌐 Global Search Results</h2>
                <p style="color: #888;">
                    Found <strong style="color: #00d9ff;">${totalResults}</strong> results 
                    across <strong style="color: #00d9ff;">${tablesSearched}</strong> tables 
                    in <strong style="color: #00ff00;">${time}ms</strong>
                </p>
            </div>
        `;
        
        // ✅ Display results by table
        let hasResults = false;
        
        for (const [tableName, tableResults] of Object.entries(results)) {
            if (!Array.isArray(tableResults) || tableResults.length === 0) {
                continue;
            }
            
            hasResults = true;
            
            html += `
                <div class="table-container" style="margin-bottom: 2rem;">
                    <h3 style="color: #00d9ff; margin-bottom: 1rem; padding: 0 1rem;">
                        📋 Table: <strong>${tableName}</strong>
                        <span style="color: #888; font-size: 0.9rem; font-weight: normal;">(${tableResults.length} results)</span>
                    </h3>
                    <div style="overflow-x: auto;">
                        <table>
                            <thead><tr>`;
            
            // Get column names from first row
            if (tableResults.length > 0) {
                const cols = Object.keys(tableResults[0]);
                cols.forEach(col => {
                    html += `<th>${col.toUpperCase()}</th>`;
                });
                html += '</tr></thead><tbody>';
                
                // Display rows (max 10 per table)
                tableResults.slice(0, 10).forEach(row => {
                    html += '<tr>';
                    cols.forEach(col => {
                        const val = row[col];
                        let displayVal;
                        
                        if (val === null || val === undefined) {
                            displayVal = '<em style="color:#666;">NULL</em>';
                        } else if (typeof val === 'object') {
                            displayVal = JSON.stringify(val);
                        } else {
                            // Highlight search term
                            displayVal = String(val);
                            const regex = new RegExp(`(${searchTerm})`, 'gi');
                            displayVal = displayVal.replace(regex, '<mark style="background:#00d9ff;color:#000;padding:0 2px;border-radius:2px;">$1</mark>');
                        }
                        
                        html += `<td>${displayVal}</td>`;
                    });
                    html += '</tr>';
                });
                
                if (tableResults.length > 10) {
                    html += `<tr><td colspan="${cols.length}" style="text-align:center;color:#888;padding:1rem;">
                        ... and ${tableResults.length - 10} more results
                    </td></tr>`;
                }
            }
            
            html += '</tbody></table></div></div>';
        }
        
        if (!hasResults) {
            html += `
                <div style="text-align:center;padding:3rem;">
                    <div style="font-size:3rem;margin-bottom:1rem;">🔍</div>
                    <p style="color:#888;font-size:1.2rem;">No results found for "${searchTerm}"</p>
                    <p style="color:#666;font-size:0.9rem;margin-top:0.5rem;">Try a different search term</p>
                </div>
            `;
        }
        
        globalResults.innerHTML = html;
        
    } catch (error) {
        console.error('Global search error:', error);
        globalResults.innerHTML = `
            <div style="background:rgba(255,82,82,0.1);border:1px solid rgba(255,82,82,0.3);border-radius:12px;padding:2rem;text-align:center;">
                <div style="font-size:2rem;margin-bottom:1rem;">⚠️</div>
                <p style="color:#ff5252;font-size:1.1rem;margin-bottom:0.5rem;">Search Failed</p>
                <p style="color:#888;font-size:0.9rem;">${error.message}</p>
                <button onclick="performGlobalSearch()" style="margin-top:1rem;padding:0.5rem 1rem;background:#00d9ff;border:none;border-radius:6px;color:#000;cursor:pointer;">
                    Try Again
                </button>
            </div>
        `;
    }
}


function displayRecords(rows) {
    const tbody = document.getElementById('table-body');
    tbody.innerHTML = '';
    
    if (!rows || rows.length === 0) {
        const row = document.createElement('tr');
        const cell = document.createElement('td');
        cell.colSpan = tableSchema?.columns.length || 1;
        cell.textContent = 'No records found';
        cell.style.textAlign = 'center';
        cell.style.padding = '2rem';
        cell.style.color = '#666';
        row.appendChild(cell);
        tbody.appendChild(row);
        return;
    }
    
    for (const record of rows) {
        const row = document.createElement('tr');
        
        for (const col of tableSchema.columns) {
            const cell = document.createElement('td');
            const value = record[col.name];
            
            if (value === null || value === undefined) {
                cell.innerHTML = '<em style="color:#666;">NULL</em>';
            } else if (typeof value === 'object') {
                cell.textContent = JSON.stringify(value);
            } else {
                cell.textContent = String(value);
            }
            
            cell.title = String(value);
            row.appendChild(cell);
        }
        
        tbody.appendChild(row);
    }
}

function nextPage() {
    if (isSearchMode) return;
    
    cursorHistory.push(currentCursor);
    currentPage++;
    loadRecords();
}

function prevPage() {
    if (currentPage === 1 || isSearchMode) return;
    
    cursorHistory.pop();
    currentCursor = cursorHistory[cursorHistory.length - 1] || '';
    currentPage--;
    loadRecords();
}

// Initialize on page load
window.addEventListener('DOMContentLoaded', loadTables);
