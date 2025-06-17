class RunSummaryApp {
    constructor() {
        this.data = [];
        this.filteredData = [];
        this.sortColumn = 'last_updated_timestamp';
        this.sortDirection = 'desc';
        // GitHub repository configuration
        this.githubBaseUrl = 'https://github.com/toanngosy/test_workflow';
        this.githubBranch = 'report';
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
        document.getElementById('serverFilter').addEventListener('change', () => this.applyFilters());
        document.getElementById('statusFilter').addEventListener('change', () => this.applyFilters());
        document.getElementById('actorFilter').addEventListener('change', () => this.applyFilters());
        document.getElementById('refreshBtn').addEventListener('click', () => this.init());
    }

    populateFilters() {
        const sites = [...new Set(this.data.map(row => row.site_id))].filter(Boolean).sort();
        const siteFilter = document.getElementById('siteFilter');
        siteFilter.innerHTML = '<option value="">All Sites</option>';
        sites.forEach(site => {
            siteFilter.innerHTML += `<option value="${site}">${site}</option>`;
        });

        const servers = [...new Set(this.data.map(row => row.server))].filter(Boolean).sort();
        const serverFilter = document.getElementById('serverFilter');
        serverFilter.innerHTML = '<option value="">All Servers</option>';
        servers.forEach(server => {
            serverFilter.innerHTML += `<option value="${server}">${server}</option>`;
        });

        const actors = [...new Set(this.data.map(row => row.actor))].filter(Boolean).sort();
        const actorFilter = document.getElementById('actorFilter');
        actorFilter.innerHTML = '<option value="">All Actors</option>';
        actors.forEach(actor => {
            actorFilter.innerHTML += `<option value="${actor}">${actor}</option>`;
        });
    }

    applyFilters() {
        const siteFilter = document.getElementById('siteFilter').value;
        const serverFilter = document.getElementById('serverFilter').value;
        const statusFilter = document.getElementById('statusFilter').value;
        const actorFilter = document.getElementById('actorFilter').value;

        this.filteredData = this.data.filter(row => {
            return (!siteFilter || row.site_id === siteFilter) &&
                   (!serverFilter || row.server === serverFilter) &&
                   (!statusFilter || row.status === statusFilter) &&
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
        const running = this.filteredData.filter(row => row.status === 'running').length;
        const succeed = this.filteredData.filter(row => row.status === 'succeed').length;
        const failed = this.filteredData.filter(row => row.status === 'failed').length;

        document.getElementById('totalRuns').textContent = total;
        document.getElementById('runningCount').textContent = running;
        document.getElementById('succeedCount').textContent = succeed;
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
                <td>${this.formatServerLink(row.server)}</td>
                <td title="${row.run_uuid || '-'}">${this.truncateText(row.run_uuid || '-', 20)}</td>
                <td>${this.formatTimestamp(row.last_updated_timestamp)}</td>
                <td>${this.formatStatus(row.status)}</td>
                <td>${this.formatParamsLink(row.params)}</td>
                <td>${this.formatOutputLink(row.output)}</td>
            `;

            tbody.appendChild(tr);
        });
    }

    formatStatus(status) {
        const statusMap = {
            'pending': { text: 'Pending', class: 'status-pending' },
            'running': { text: 'Running', class: 'status-running' },
            'completed': { text: 'Completed', class: 'status-completed' },
            'succeed': { text: 'Succeed', class: 'status-succeed' },
            'failed': { text: 'Failed', class: 'status-failed' }
        };

        const statusInfo = statusMap[status] || { text: status || 'Unknown', class: 'status-pending' };
        return `<span class="status-badge ${statusInfo.class}">${statusInfo.text}</span>`;
    }

    formatServerLink(server) {
        if (!server) return '-';
        const serverPath = `${this.githubBaseUrl}/tree/${this.githubBranch}/report/server/${server}`;
        const truncatedServer = this.truncateText(server, 15);
        return `<a href="${serverPath}" target="_blank" class="data-link" title="View ${server} server files">${truncatedServer}</a>`;
    }

    formatParamsLink(params) {
        if (!params) return '-';
        const paramsPath = `${this.githubBaseUrl}/blob/${this.githubBranch}/${params}`;
        const truncatedParams = this.truncateText(params, 30);
        return `<a href="${paramsPath}" target="_blank" class="data-link" title="${params}">${truncatedParams}</a>`;
    }

    formatOutputLink(output) {
        if (!output) return '-';
        const outputPath = `${this.githubBaseUrl}/tree/${this.githubBranch}/${output}`;
        const truncatedOutput = this.truncateText(output, 30);
        return `<a href="${outputPath}" target="_blank" class="data-link" title="${output}">${truncatedOutput}</a>`;
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