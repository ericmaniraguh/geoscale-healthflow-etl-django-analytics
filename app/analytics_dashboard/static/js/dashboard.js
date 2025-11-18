/**
 * ===================================================================
 * GEOSPATIAL ADMIN DASHBOARD JAVASCRIPT
 * File: app/geospatial_merger/static/js/dashboard.js
 * ===================================================================
 */

// Global variables
let uploadedFiles = { boundaries: null, geotiff: null };
let currentProcessId = null;
let progressInterval = null;
let startTime = null;

// Initialize dashboard
document.addEventListener('DOMContentLoaded', function() {
    initializeDashboard();
    setupFileUpload();
    checkMongoStatus();
});

function initializeDashboard() {
    console.log('Initializing enhanced admin dashboard...');
    showAlert('Admin dashboard initialized successfully!', 'info');
    updateStatistics();
}

function setupFileUpload() {
    // Setup drag and drop for shapefile
    const shapefileUpload = document.getElementById('shapefileUpload');
    const shapefileInput = document.getElementById('shapefileInput');
    
    setupDragDrop(shapefileUpload, shapefileInput, 'boundaries');
    
    // Setup drag and drop for geotiff
    const geotiffUpload = document.getElementById('geotiffUpload');
    const geotiffInput = document.getElementById('geotiffInput');
    
    setupDragDrop(geotiffUpload, geotiffInput, 'geotiff');
    
    // File change events
    shapefileInput.addEventListener('change', (e) => handleFileSelect(e, 'boundaries'));
    geotiffInput.addEventListener('change', (e) => handleFileSelect(e, 'geotiff'));
    
    // Form submission
    document.getElementById('uploadForm').addEventListener('submit', handleFormSubmit);
}

function setupDragDrop(uploadArea, fileInput, type) {
    uploadArea.addEventListener('dragover', (e) => {
        e.preventDefault();
        uploadArea.classList.add('dragover');
    });
    
    uploadArea.addEventListener('dragleave', () => {
        uploadArea.classList.remove('dragover');
    });
    
    uploadArea.addEventListener('drop', (e) => {
        e.preventDefault();
        uploadArea.classList.remove('dragover');
        
        const files = e.dataTransfer.files;
        if (files.length > 0) {
            fileInput.files = files;
            handleFileSelect({ target: fileInput }, type);
        }
    });
}

function handleFileSelect(event, type) {
    const file = event.target.files[0];
    if (!file) return;

    uploadedFiles[type] = file;
    
    // Show file info
    const infoDiv = document.getElementById(type === 'boundaries' ? 'shapefileInfo' : 'geotiffInfo');
    infoDiv.style.display = 'block';
    infoDiv.innerHTML = `
        <h4><i class="fas fa-file"></i> ${file.name}</h4>
        <div class="file-details">
            <p>Size: ${formatFileSize(file.size)}</p>
            <p>Type: ${file.type || 'Unknown'}</p>
            <p>Last Modified: ${new Date(file.lastModified).toLocaleDateString()}</p>
        </div>
    `;

    // Validate file
    if (validateFileType(file, type)) {
        infoDiv.classList.add('status-success');
        showAlert(`${type === 'boundaries' ? 'Boundary shapefile' : 'Slope GeoTIFF'} uploaded successfully!`, 'success');
    } else {
        infoDiv.classList.add('status-error');
        showAlert(`Invalid file type for ${type}`, 'error');
    }

    // Enable buttons if both files are uploaded
    updateButtonStates();
}

function validateFileType(file, type) {
    const fileName = file.name.toLowerCase();
    
    if (type === 'boundaries') {
        return fileName.endsWith('.zip');
    } else if (type === 'geotiff') {
        return fileName.endsWith('.tif') || fileName.endsWith('.tiff');
    }
    
    return false;
}

function updateButtonStates() {
    const hasBoundaries = uploadedFiles.boundaries !== null;
    const hasGeotiff = uploadedFiles.geotiff !== null;
    const bothUploaded = hasBoundaries && hasGeotiff;
    
    document.getElementById('validateBtn').disabled = !bothUploaded;
    document.getElementById('uploadBtn').disabled = !bothUploaded;
}

function validateFiles() {
    if (!uploadedFiles.boundaries || !uploadedFiles.geotiff) {
        showAlert('Please upload both files first', 'error');
        return;
    }

    showAlert('Files validated successfully! Ready to process.', 'success');
    
    // Show coordinate info panel
    document.getElementById('coordinateInfo').style.display = 'block';
    updateCoordinateInfo();
}

function updateCoordinateInfo() {
    const boundsGrid = document.getElementById('boundsGrid');
    boundsGrid.innerHTML = `
        <div class="bounds-item">
            <div class="bounds-label">Coordinate System</div>
            <div class="bounds-value">WGS84 (EPSG:4326)</div>
        </div>
        <div class="bounds-item">
            <div class="bounds-label">Storage Format</div>
            <div class="bounds-value">Decimal Degrees</div>
        </div>
        <div class="bounds-item">
            <div class="bounds-label">Processing Status</div>
            <div class="bounds-value">Files ready for processing</div>
        </div>
    `;
}

async function handleFormSubmit(event) {
    event.preventDefault();
    
    if (!uploadedFiles.boundaries || !uploadedFiles.geotiff) {
        showAlert('Please upload both files', 'error');
        return;
    }

    // Switch to processing tab
    switchTab('process');
    startTime = Date.now();
    
    // Show progress
    document.getElementById('progressPanel').style.display = 'block';
    document.getElementById('coordinateInfo').style.display = 'block';
    updateProgress(0, 'Starting upload...');

    try {
        // Upload files
        const formData = new FormData();
        formData.append('geojson', uploadedFiles.boundaries);
        formData.append('geotiff', uploadedFiles.geotiff);

        updateProgress(25, 'Uploading files...');

        const response = await fetch('/geospatial/upload_files/', {
            method: 'POST',
            headers: {
                'X-CSRFToken': getCsrfToken()
            },
            body: formData
        });

        const result = await response.json();

        if (result.success) {
            currentProcessId = result.process_id;
            updateProgress(50, 'Files uploaded successfully');
            
            // Start processing
            await startProcessing();
        } else {
            throw new Error(result.message || 'Upload failed');
        }

    } catch (error) {
        console.error('Upload error:', error);
        showAlert('Upload failed: ' + error.message, 'error');
        updateProgress(0, 'Upload failed');
    }
}

async function startProcessing() {
    try {
        const formData = new FormData();
        formData.append('process_id', currentProcessId);

        const response = await fetch('/geospatial/api/start_merge/', {
            method: 'POST',
            headers: {
                'X-CSRFToken': getCsrfToken()
            },
            body: formData
        });

        const result = await response.json();

        if (result.success) {
            updateProgress(75, 'Processing started...');
            startProgressMonitoring();
        } else {
            throw new Error(result.message || 'Processing failed to start');
        }

    } catch (error) {
        console.error('Processing error:', error);
        showAlert('Processing failed: ' + error.message, 'error');
    }
}

function startProgressMonitoring() {
    progressInterval = setInterval(async () => {
        try {
            const response = await fetch(`/geospatial/api/status/?process_id=${currentProcessId}`);
            const status = await response.json();

            updateProgress(status.progress || 0, status.message || 'Processing...');
            
            // Update statistics with real-time data
            if (status.file_statistics) {
                updateStatisticsFromStatus(status.file_statistics);
            }

            // Update coordinate information
            if (status.coordinate_info) {
                updateCoordinateDisplay(status.coordinate_info);
            }

            if (status.completed) {
                clearInterval(progressInterval);
                if (status.error) {
                    showAlert('Processing failed: ' + status.error, 'error');
                } else {
                    updateProgress(100, 'Processing completed successfully!');
                    showAlert('Processing completed successfully!', 'success');
                    updateFinalStatistics(status);
                    switchTab('statistics');
                }
            }

        } catch (error) {
            console.error('Progress check error:', error);
        }
    }, 2000);
}

function updateProgress(percentage, message) {
    const progressFill = document.getElementById('progressFill');
    const progressText = document.getElementById('progressText');
    
    progressFill.style.width = percentage + '%';
    progressText.textContent = message;
    
    // Update processing status
    document.getElementById('processingStatus').innerHTML = `
        <p><strong>Status:</strong> ${message}</p>
        <p><strong>Progress:</strong> ${percentage}%</p>
        <p><strong>Process ID:</strong> ${currentProcessId || 'Not assigned'}</p>
    `;
}

function updateStatisticsFromStatus(stats) {
    document.getElementById('totalBoundaryFeatures').textContent = stats.total_boundary_features || '-';
    document.getElementById('totalSlopePoints').textContent = (stats.total_slope_points || 0).toLocaleString();
    document.getElementById('processedFeatures').textContent = stats.processed_features || '-';
    document.getElementById('failedFeatures').textContent = stats.failed_features || '-';
    document.getElementById('batchesCompleted').textContent = `${stats.batches_completed || 0}/${stats.total_batches || 0}`;
    
    // Calculate success rate
    const total = stats.total_boundary_features || 0;
    const processed = stats.processed_features || 0;
    const successRate = total > 0 ? ((processed / total) * 100).toFixed(1) : 0;
    document.getElementById('successRate').textContent = successRate + '%';
}

function updateCoordinateDisplay(coordInfo) {
    if (coordInfo.boundary_bounds) {
        document.getElementById('boundaryBounds').textContent = coordInfo.boundary_bounds;
    }
    if (coordInfo.slope_bounds) {
        document.getElementById('slopeBounds').textContent = coordInfo.slope_bounds;
    }
    if (coordInfo.overlap_coverage) {
        document.getElementById('overlapCoverage').textContent = coordInfo.overlap_coverage;
    }
    
    // Update overlap status
    const overlapStatus = document.getElementById('overlapStatus');
    if (coordInfo.overlap_status === 'OVERLAP_ACHIEVED') {
        overlapStatus.className = 'overlap-status';
        overlapStatus.innerHTML = '<i class="fas fa-check-circle"></i> Spatial overlap confirmed - Processing can proceed';
    } else {
        overlapStatus.className = 'overlap-status no-overlap';
        overlapStatus.innerHTML = '<i class="fas fa-exclamation-triangle"></i> No spatial overlap detected';
    }
}

function updateFinalStatistics(status) {
    // Update processing time
    if (startTime) {
        const processingTimeMs = Date.now() - startTime;
        const minutes = Math.floor(processingTimeMs / 60000);
        const seconds = Math.floor((processingTimeMs % 60000) / 1000);
        document.getElementById('processingTime').textContent = `${minutes}m ${seconds}s`;
    }

    // Update processing summary
    const summary = document.getElementById('processingSummary');
    const stats = status.file_statistics || {};
    
    summary.innerHTML = `
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px;">
            <div style="background: #e3f2fd; padding: 15px; border-radius: 8px; border-left: 3px solid #2196F3;">
                <strong>File Processing</strong><br>
                Boundaries: ${stats.boundary_files_uploaded || 0} file(s)<br>
                Slope Data: ${stats.slope_files_uploaded || 0} file(s)<br>
                CRS Fix Applied: ${stats.crs_fix_applied ? 'Yes' : 'No'}
            </div>
            <div style="background: #e8f5e8; padding: 15px; border-radius: 8px; border-left: 3px solid #4CAF50;">
                <strong>Processing Results</strong><br>
                Success Rate: ${((stats.processed_features || 0) / (stats.total_boundary_features || 1) * 100).toFixed(1)}%<br>
                Batches: ${stats.batches_completed || 0}/${stats.total_batches || 0}<br>
                Storage: WGS84 Coordinates
            </div>
            <div style="background: #fff3e0; padding: 15px; border-radius: 8px; border-left: 3px solid #FF9800;">
                <strong>Data Quality</strong><br>
                Slope Points: ${(stats.slope_points_after_conversion || 0).toLocaleString()}<br>
                Overlap Status: ${stats.final_overlap_status || 'Unknown'}<br>
                Atlas Storage: ${status.mongodb_atlas_status?.available ? 'Success' : 'Failed'}
            </div>
        </div>
    `;
}

function updateStatistics() {
    // Initialize with default values
    document.getElementById('totalBoundaryFeatures').textContent = '-';
    document.getElementById('totalSlopePoints').textContent = '-';
    document.getElementById('processedFeatures').textContent = '-';
    document.getElementById('failedFeatures').textContent = '-';
    document.getElementById('successRate').textContent = '-%';
    document.getElementById('batchesCompleted').textContent = '-';
}

async function checkMongoStatus() {
    try {
        // This would be an API call to check MongoDB status
        document.getElementById('mongoStatusText').innerHTML = `
            <p><strong>Status:</strong> <span style="color: #27ae60;">Connected</span></p>
            <p><strong>Database:</strong> geospatial_wgs84_boundaries_db</p>
            <p><strong>Collection:</strong> boundaries_slope_wgs84</p>
        `;
        document.getElementById('atlasStatus').textContent = 'Connected';
    } catch (error) {
        document.getElementById('mongoStatusText').innerHTML = `
            <p><strong>Status:</strong> <span style="color: #e74c3c;">Disconnected</span></p>
            <p><strong>Error:</strong> ${error.message}</p>
        `;
        document.getElementById('atlasStatus').textContent = 'Disconnected';
    }
}

function switchTab(tabName) {
    // Hide all tab contents
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });
    
    // Remove active class from all tabs
    document.querySelectorAll('.tab').forEach(tab => {
        tab.classList.remove('active');
    });
    
    // Show selected tab content
    document.getElementById(tabName + 'Tab').classList.add('active');
    
    // Mark selected tab as active
    event.target.classList.add('active');
}

function showAlert(message, type) {
    const alertContainer = document.getElementById('alertContainer');
    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${type}`;
    alertDiv.innerHTML = `
        <i class="fas fa-${type === 'success' ? 'check-circle' : type === 'error' ? 'exclamation-circle' : 'info-circle'}"></i>
        ${message}
    `;
    
    alertContainer.appendChild(alertDiv);
    
    // Auto remove after 5 seconds
    setTimeout(() => {
        if (alertDiv.parentNode) {
            alertDiv.parentNode.removeChild(alertDiv);
        }
    }, 5000);
}

function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function getCsrfToken() {
    const token = document.querySelector('[name=csrfmiddlewaretoken]');
    return token ? token.value : '';
}

function resetDashboard() {
    // Reset all variables
    uploadedFiles = { boundaries: null, geotiff: null };
    currentProcessId = null;
    startTime = null;
    
    if (progressInterval) {
        clearInterval(progressInterval);
    }

    // Reset form
    document.getElementById('uploadForm').reset();
    
    // Hide file info
    document.getElementById('shapefileInfo').style.display = 'none';
    document.getElementById('geotiffInfo').style.display = 'none';
    document.getElementById('progressPanel').style.display = 'none';
    document.getElementById('coordinateInfo').style.display = 'none';
    
    // Reset buttons
    updateButtonStates();
    
    // Clear alerts
    document.getElementById('alertContainer').innerHTML = '';
    
    // Reset statistics
    updateStatistics();
    
    // Switch to upload tab
    switchTab('upload');
    
    showAlert('Dashboard reset successfully', 'info');
}

function logoutUser() {
    if (confirm('Are you sure you want to logout?')) {
        fetch('/geospatial/api/logout/', {
            method: 'POST',
            headers: {
                'X-CSRFToken': getCsrfToken()
            }
        }).then(response => response.json())
        .then(data => {
            if (data.success) {
                window.location.href = data.redirect || '/auth/login/';
            }
        });
    }
}

// Auto-refresh statistics periodically
setInterval(() => {
    if (currentProcessId && progressInterval) {
        updateStatistics();
    }
}, 5000);

// Add keyboard shortcuts
document.addEventListener('keydown', function(e) {
    // Ctrl+R for reset
    if (e.ctrlKey && e.key === 'r') {
        e.preventDefault();
        resetDashboard();
    }
    
    // Tab navigation with numbers
    if (e.altKey) {
        if (e.key === '1') switchTab('upload');
        if (e.key === '2') switchTab('process');
        if (e.key === '3') switchTab('statistics');
    }
});

// In your browser console, try this to check current status:
if (currentProcessId) {
    fetch(`/geospatial/api/status/?process_id=${currentProcessId}`)
        .then(response => response.text())
        .then(text => {
            console.log("Raw response:", text);
            try {
                const data = JSON.parse(text);
                console.log("Parsed data:", data);
            } catch (e) {
                console.error("JSON parse error:", e);
            }
        })
        .catch(error => console.error("Fetch error:", error));
}