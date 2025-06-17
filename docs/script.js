class RunSummaryApp {
    constructor() {
        this.data = [];
        this.filteredData = [];
        this.sortColumn = 'last_updated_timestamp';
        this.sortDirection = 'desc';
        this.init();
    }

    async init() {
        try {
            this.showLoading(true);
            await this.loadData();
            this.setupEventListeners();
            this.populateFilters();
            this.updateSummaryCards();
            this.renderTable();
            this.showLoading(false);
            this.updateLastUpdated();
        } catch (error) {
            this.showError(`Failed to initialize: ${error.message}`);
        }
    }

    async loadData() {
        try {
            const response = await fetch('./data/runs.json');
            if (response.ok) {
                this.data = await response.json();
                // Filter out null/empty entries
                this.data = this.data.filter(row => row && row.site_id);
            } else {
                throw new Error('Failed to load runs data');
            }
        } catch (error) {
            console.error('Error loading data:', error);
            this.showError('Failed to load data. Please check if the data files exist.');
            this.data = [];
        }
        this.filteredData = [...this.data];
    }

    setupEventListeners() {
        document.querySelectorAll('th[data-sort]').forEach(th => {
            th.addEventListener('click', () => {
                const column = th.getAttribute('data-sort');
                this.sortData(column);
            });
        });

        document.getElementById('siteFilter').addEventListener('change', () => this.applyFilters());
        document.getElementById('statusFilter').addEventListener('change', () => this.applyFilters());
        document.getElementById('actorFilter').addEventListener('change', () => this.applyFilters());
        document.getElementById('refreshBtn').addEventListener('click', () => this.init());
    }

    populateFilters() {
        const sites = [...new Set(this.data.map(row => row.site_id))].sort();
        const siteFilter = document.getElementById('siteFilter');
        siteFilter.innerHTML = '<option value="">All Sites</option>';
        sites.forEach(site => {
            if (site) {
                siteFilter.innerHTML += `<option value="${site}">${site}</option>`;
            }
        });

        const actors = [...new Set(this.data.map(row => row.actor))].sort();
        const actorFilter = document.getElementById('actorFilter');
        actorFilter.innerHTML = '<option value="">All Actors</option>';
        actors.forEach(actor => {
            if (actor) {
                actorFilter.innerHTML += `<option value="${actor}">${actor}</option>`;
            }
        });
    }

    applyFilters() {
        const siteFilter = document.getElementById('siteFilter').value;
        const statusFilter = document.getElementById('statusFilter').value;
        const actorFilter = document.getElementById('actorFilter').value;

        this.filteredData = this.data.filter(row => {
            return (!siteFilter || row.site_id === siteFilter) &&
                   (!statusFilter || row.state === statusFilter) &&
                   (!actorFilter || row.actor === actorFilter);
        });

        this.updateSummaryCards();
        this.renderTable();
    }

    sortData(column) {
        if (this.sortColumn === column) {
            this.sortDirection = this.sortDirection === 'asc' ? 'desc' : 'asc';
        } else {
            this.sortColumn = column;
            this.sortDirection = 'desc';
        }

        this.filteredData.sort((a, b) => {
            let aVal = a[column] || '';
            let bVal = b[column] || '';

            if (column === 'last_updated_timestamp') {
                aVal = new Date(aVal);
                bVal = new Date(bVal);
            }
            
            if (column === 'state' || column === 'process_id') {
                aVal = parseFloat(aVal) || 0;
                bVal = parseFloat(bVal) || 0;
            }

            if (aVal < bVal) return this.sortDirection === 'asc' ? -1 : 1;
            if (aVal > bVal) return this.sortDirection === 'asc' ? 1 : -1;
            return 0;
        });

        this.updateSortIndicators();
        this.renderTable();
    }

    updateSortIndicators() {
        document.querySelectorAll('th').forEach(th => {
            th.classList.remove('sort-asc', 'sort-desc');
        });

        const currentTh = document.querySelector(`th[data-sort="${this.sortColumn}"]`);
        if (currentTh) {
            currentTh.classList.add(this.sortDirection === 'asc' ? 'sort-asc' : 'sort-desc');
        }
    }

    updateSummaryCards() {
        const total = this.filteredData.length;
        const running = this.filteredData.filter(row => row.state === '1').length;
        const completed = this.filteredData.filter(row => row.state === '2').length;
        const failed = this.filteredData.filter(row => row.state === '3').length;

        document.getElementById('totalRuns').textContent = total;
        document.getElementById('runningCount').textContent = running;
        document.getElementById('completedCount').textContent = completed;
        document.getElementById('failedCount').textContent = failed;
    }

    renderTable() {
        const tbody = document.getElementById('runsTableBody');
        tbody.innerHTML = '';

        this.filteredData.forEach(row => {
            const tr = document.createElement('tr');
            tr.className = 'fade-in';

            tr.innerHTML = `
                <td>${row.site_id || '-'}</td>
                <td title="${row.run_uuid || '-'}">${this.truncateText(row.run_uuid || '-', 20)}</td>
                <td>${this.formatTimestamp(row.last_updated_timestamp)}</td>
                <td>${this.formatStatus(row.state)}</td>
                <td>${row.actor || '-'}</td>
                <td>${row.process_id || '-'}</td>
                <td title="${row.additional_info || '-'}">${this.truncateText(row.additional_info || '-', 30)}</td>
            `;

            tbody.appendChild(tr);
        });
    }

    formatStatus(state) {
        const statusMap = {
            '0': { text: 'Pending', class: 'status-pending' },
            '1': { text: 'Running', class: 'status-running' },
            '2': { text: 'Completed', class: 'status-completed' },
            '3': { text: 'Failed', class: 'status-failed' }
        };

        const status = statusMap[state] || { text: 'Unknown', class: 'status-pending' };
        return `<span class="status-badge ${status.class}">${status.text}</span>`;
    }

    formatTimestamp(timestamp) {
        if (!timestamp) return '-';
        try {
            const date = new Date(timestamp);
            return date.toLocaleString();
        } catch {
            return timestamp;
        }
    }

    truncateText(text, maxLength) {
        if (!text || text.length <= maxLength) return text;
        return text.substring(0, maxLength) + '...';
    }

    showLoading(show) {
        const spinner = document.getElementById('loadingSpinner');
        const table = document.querySelector('.table-container');
        
        spinner.style.display = show ? 'block' : 'none';
        table.style.display = show ? 'none' : 'block';
    }

    showError(message) {
        const errorDiv = document.getElementById('errorMessage');
        const errorText = document.getElementById('errorText');
        
        errorText.textContent = message;
        errorDiv.style.display = 'block';
        this.showLoading(false);
    }

    updateLastUpdated() {
        const now = new Date();
        document.getElementById('lastUpdated').textContent = now.toLocaleString();
    }
}

document.addEventListener('DOMContentLoaded', () => {
    new RunSummaryApp();
});

// Auto-refresh every 5 minutes
setInterval(() => {
    if (document.visibilityState === 'visible') {
        new RunSummaryApp();
    }
}, 5 * 60 * 1000); 