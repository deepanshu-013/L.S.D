let currentCursor = '';
let cursorHistory = [];
let currentPage = 1;
let isSearchMode = false;
let currentSearchTerm = '';

async function loadStats() {
    try {
        const response = await fetch('/api/stats');
        const stats = await response.json();
        
        document.getElementById('total-records').textContent = formatNumber(stats.total_count);
        document.getElementById('categories').textContent = stats.category_count;
        document.getElementById('total-value').textContent = '$' + formatNumber(stats.total_value.toFixed(2));
        document.getElementById('avg-value').textContent = '$' + stats.avg_value.toFixed(2);
    } catch (error) {
        console.error('Error loading stats:', error);
    }
}

async function loadRecords(cursor = '') {
    isSearchMode = false;
    currentSearchTerm = '';
    
    const category = document.getElementById('category-filter').value;
    const status = document.getElementById('status-filter').value;
    const sortBy = document.getElementById('sort-by').value;
    const sortDir = document.getElementById('sort-dir').value;
    
    let url = `/api/records?limit=20&sort_by=${sortBy}&sort_dir=${sortDir}`;
    if (category) url += `&category=${category}`;
    if (status) url += `&status=${status}`;
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

async function performSearch(term, cursor) {
    let url = `/api/search?q=${encodeURIComponent(term)}&limit=20`;
    if (cursor) url += `&cursor=${encodeURIComponent(cursor)}`;
    
    try {
        const response = await fetch(url);
        const data = await response.json();
        renderRecords(data);
        updatePagination(data, cursor);
    } catch (error) {
        console.error('Error searching records:', error);
    }
}

function renderRecords(data) {
    const tbody = document.getElementById('records-body');
    
    if (!data.data || data.data.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; padding: 2rem;">No records found</td></tr>';
        return;
    }
    
    tbody.innerHTML = data.data.map(record => `
        <tr>
            <td>${record.id}</td>
            <td>${escapeHtml(record.name)}</td>
            <td>${escapeHtml(record.category)}</td>
            <td><span class="status-${record.status}">${escapeHtml(record.status)}</span></td>
            <td>$${record.value.toFixed(2)}</td>
            <td>${formatDate(record.created_at)}</td>
        </tr>
    `).join('');
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
        const prevCursor = cursorHistory.pop();
        
        if (isSearchMode) {
            performSearch(currentSearchTerm, cursorHistory.length > 0 ? cursorHistory[cursorHistory.length - 1] : '');
        } else {
            loadRecords(cursorHistory.length > 0 ? cursorHistory[cursorHistory.length - 1] : '');
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

function formatDate(dateStr) {
    const date = new Date(dateStr);
    return date.toLocaleDateString() + ' ' + date.toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'});
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

document.getElementById('search-input').addEventListener('keypress', function(e) {
    if (e.key === 'Enter') {
        searchRecords();
    }
});

document.addEventListener('DOMContentLoaded', function() {
    loadStats();
    loadRecords();
});
