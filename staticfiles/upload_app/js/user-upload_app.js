/**
 * User Dashboard Upload App JavaScript
 * Handles sidebar navigation, form interactions, upload status tracking, and UI feedback
 */

// Upload status tracking - Initialize first!
let uploadStatus = {
    'health-center': 'available',
    'hmis-api': 'available',
    'temperature': 'available',
    'precipitation': 'available'
};

// DOM Content Loaded Event
document.addEventListener('DOMContentLoaded', function() {
    initializeDashboard();
    loadUploadStatus();
});

/**
 * Initialize dashboard functionality
 */
function initializeDashboard() {
    setupSidebarNavigation();
    setupMessageHandling();
    setupFormSubmissionHandlers();
    initializeDragAndDrop();
    initializeKeyboardNavigation();
    addLoadingAnimation();
    createProgressIndicator();
    createSessionStatusIndicator();
    
    console.log('User Dashboard initialized successfully');
}

/**
 * Load upload status from sessionStorage
 */
function loadUploadStatus() {
    const savedStatus = sessionStorage.getItem('uploadStatus');
    if (savedStatus) {
        uploadStatus = JSON.parse(savedStatus);
        updateAllUploadStates();
    }
}

/**
 * Save upload status to sessionStorage
 */
function saveUploadStatus() {
    sessionStorage.setItem('uploadStatus', JSON.stringify(uploadStatus));
}

/**
 * Update all upload states based on current status
 */
function updateAllUploadStates() {
    Object.keys(uploadStatus).forEach(uploadType => {
        const status = uploadStatus[uploadType];
        updateUploadState(uploadType, status);
    });
    
    // Update active section if current one is completed
    const activeItem = document.querySelector('.upload-item.active');
    if (activeItem) {
        const currentUploadType = activeItem.dataset.upload;
        if (uploadStatus[currentUploadType] === 'completed') {
            selectNextAvailableUpload();
        }
    }
}

/**
 * Update upload state for a specific upload type
 * @param {string} uploadType - Type of upload (health-center, hmis-api, etc.)
 * @param {string} status - Status (available, processing, completed, disabled)
 */
function updateUploadState(uploadType, status) {
    const uploadItem = document.querySelector(`[data-upload="${uploadType}"]`);
    const uploadSection = document.getElementById(`${uploadType}-form`);
    
    if (!uploadItem || !uploadSection) {
        console.warn(`Could not find elements for upload type: ${uploadType}`);
        return;
    }
    
    // Remove all status classes
    uploadItem.classList.remove('available', 'processing', 'completed', 'disabled');
    uploadSection.classList.remove('available', 'processing', 'completed', 'disabled');
    
    // Add new status class
    uploadItem.classList.add(status);
    uploadSection.classList.add(status);
    
    // Update status badge and icons
    updateStatusBadge(uploadItem, status);
    updateStatusIcon(uploadItem, status);
    
    // Update form state
    updateFormState(uploadSection, status);
    
    // Update upload status tracking
    uploadStatus[uploadType] = status;
    saveUploadStatus();
    
    // Update progress indicators
    updateProgressBar();
    updateSessionStatus();
}

/**
 * Update status badge text and styling
 * @param {HTMLElement} uploadItem - Upload item element
 * @param {string} status - Current status
 */
function updateStatusBadge(uploadItem, status) {
    const badge = uploadItem.querySelector('.status-badge');
    const statusTexts = {
        available: 'AVAILABLE',
        processing: 'UPLOADING',
        completed: 'COMPLETED',
        disabled: 'DISABLED'
    };
    
    if (badge) {
        badge.textContent = statusTexts[status] || 'AVAILABLE';
        badge.className = `status-badge ${status}`;
    }
}

/**
 * Update status icon visibility
 * @param {HTMLElement} uploadItem - Upload item element
 * @param {string} status - Current status
 */
function updateStatusIcon(uploadItem, status) {
    const icons = uploadItem.querySelectorAll('.status-icon');
    
    icons.forEach(icon => {
        icon.style.display = 'none';
    });
    
    const activeIcon = uploadItem.querySelector(`.status-icon.${status}`);
    if (activeIcon) {
        activeIcon.style.display = 'inline-block';
    }
}

/**
 * Update form state based on upload status
 * @param {HTMLElement} uploadSection - Upload section element
 * @param {string} status - Current status
 */
function updateFormState(uploadSection, status) {
    const submitBtn = uploadSection.querySelector('.submit-btn');
    const formInputs = uploadSection.querySelectorAll('input, select, textarea');
    
    if (status === 'completed' || status === 'disabled') {
        // Disable form elements
        formInputs.forEach(input => {
            input.disabled = true;
        });
        
        if (submitBtn) {
            submitBtn.disabled = true;
            if (status === 'completed') {
                submitBtn.innerHTML = '✅ Upload Completed';
                submitBtn.style.background = '#28a745';
            } else {
                submitBtn.innerHTML = '🚫 Upload Disabled';
                submitBtn.style.background = '#6c757d';
            }
        }
    } else {
        // Enable form elements
        formInputs.forEach(input => {
            input.disabled = false;
        });
        
        if (submitBtn) {
            submitBtn.disabled = false;
            const originalText = getOriginalButtonText(submitBtn.dataset.uploadType);
            submitBtn.innerHTML = originalText;
            submitBtn.style.background = '';
        }
    }
}

/**
 * Get original button text for upload type
 * @param {string} uploadType - Type of upload
 * @returns {string} - Original button text
 */
function getOriginalButtonText(uploadType) {
    const buttonTexts = {
        'health-center': '📤 Upload Health Records',
        'hmis-api': '📤 Upload HMIS Data',
        'temperature': '📤 Upload Temperature Data',
        'precipitation': '📤 Upload Precipitation Data'
    };
    
    return buttonTexts[uploadType] || '📤 Upload Data';
}

/**
 * Select next available upload section
 */
function selectNextAvailableUpload() {
    const availableItems = document.querySelectorAll('.upload-item:not(.completed):not(.disabled)');
    
    if (availableItems.length > 0) {
        // Remove active class from all items
        document.querySelectorAll('.upload-item').forEach(item => {
            item.classList.remove('active');
        });
        
        // Activate first available item
        availableItems[0].classList.add('active');
        handleSectionSwitch(availableItems[0]);
    }
}

/**
 * Handle sidebar navigation between upload sections
 */
function setupSidebarNavigation() {
    const uploadItems = document.querySelectorAll('.upload-item');
    
    uploadItems.forEach(item => {
        item.addEventListener('click', function() {
            const uploadType = this.dataset.upload;
            const status = uploadStatus[uploadType];
            
            // Only allow switching if not completed or disabled
            if (status !== 'completed' && status !== 'disabled') {
                handleSectionSwitch(this);
            } else {
                // Show feedback for completed/disabled sections
                if (status === 'completed') {
                    showToast('This upload has already been completed!', 'info');
                } else {
                    showToast('This upload section is currently disabled.', 'warning');
                }
            }
        });
    });
}

/**
 * Switch between different upload sections
 * @param {HTMLElement} clickedItem - The clicked sidebar item
 */
function handleSectionSwitch(clickedItem) {
    // Remove active class from all items and forms
    const allItems = document.querySelectorAll('.upload-item');
    const allSections = document.querySelectorAll('.upload-section');
    
    allItems.forEach(item => item.classList.remove('active'));
    allSections.forEach(section => section.classList.remove('active'));
    
    // Add active class to clicked item
    clickedItem.classList.add('active');
    
    // Get the corresponding form and show it
    const uploadType = clickedItem.dataset.upload;
    const targetForm = document.getElementById(uploadType + '-form');
    
    if (targetForm) {
        targetForm.classList.add('active');
    }
}

/**
 * Setup form submission handlers with loading states
 */
function setupFormSubmissionHandlers() {
    const uploadForms = document.querySelectorAll('.upload-form');
    
    uploadForms.forEach(form => {
        form.addEventListener('submit', function(event) {
            event.preventDefault(); // Prevent default form submission
            
            if (validateForm(this)) {
                handleFormSubmission(this);
            }
        });
    });
}

/**
 * Handle form submission with loading state and status tracking
 * @param {HTMLFormElement} form - The form being submitted
 */
function handleFormSubmission(form) {
    const submitButton = form.querySelector('.submit-btn');
    const uploadType = submitButton.dataset.uploadType;
    
    if (!uploadType) {
        console.error('Upload type not found on submit button');
        showToast('Error: Upload type not specified', 'error');
        return;
    }
    
    if (submitButton) {
        // Store original button text
        const originalText = submitButton.innerHTML;
        
        // Set processing state
        updateUploadState(uploadType, 'processing');
        setButtonLoadingState(submitButton, originalText);
        
        // Simulate upload (replace with actual Django form submission)
        handleRealDjangoSubmission(form, uploadType);
    }
}

/**
 * Simulate upload process (replace with actual Django integration)
 * @param {HTMLFormElement} form - The form being submitted
 * @param {string} uploadType - Type of upload
 */
// Replace the simulation with this for real Django integration:
function handleRealDjangoSubmission(form, uploadType) {
    const formData = new FormData(form);
    
    fetch(form.action, {
        method: 'POST',
        body: formData,
        headers: {
            'X-Requested-With': 'XMLHttpRequest',
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            handleUploadSuccess(uploadType);
        } else {
            handleUploadFailure(uploadType);
        }
    })
    .catch(error => {
        handleUploadFailure(uploadType);
    });
}

/**
 * Handle successful upload completion
 * @param {string} uploadType - Type of upload that completed
 */
function handleUploadSuccess(uploadType) {
    // Mark as completed
    updateUploadState(uploadType, 'completed');
    
    // Show success message
    showToast(`${getUploadDisplayName(uploadType)} uploaded successfully! ✅`, 'success');
    
    // Auto-switch to next available upload after delay
    setTimeout(() => {
        selectNextAvailableUpload();
    }, 1500);
    
    // Check if all uploads are completed
    checkAllUploadsCompleted();
}

/**
 * Handle upload failure
 * @param {string} uploadType - Type of upload that failed
 */
function handleUploadFailure(uploadType) {
    updateUploadState(uploadType, 'available');
    showToast(`${getUploadDisplayName(uploadType)} upload failed. Please try again.`, 'error');
}

/**
 * Get display name for upload type
 * @param {string} uploadType - Upload type identifier
 * @returns {string} - Human readable name
 */
function getUploadDisplayName(uploadType) {
    const displayNames = {
        'health-center': 'Health Center Data',
        'hmis-api': 'HMIS API Data',
        'temperature': 'Temperature Data',
        'precipitation': 'Precipitation Data'
    };
    
    return displayNames[uploadType] || 'Data';
}

/**
 * Check if all uploads are completed and show completion message
 */
function checkAllUploadsCompleted() {
    const allCompleted = Object.values(uploadStatus).every(status => 
        status === 'completed' || status === 'disabled'
    );
    
    if (allCompleted) {
        setTimeout(() => {
            showToast('🎉 All uploads completed successfully!', 'success');
            showCompletionModal();
        }, 2000);
    }
}

/**
 * Show completion modal
 */
function showCompletionModal() {
    const modal = document.createElement('div');
    modal.className = 'completion-modal';
    modal.innerHTML = `
        <div class="modal-content">
            <h3>🎉 Upload Process Complete!</h3>
            <p>All your data has been successfully uploaded and processed.</p>
            <div class="modal-actions">
                <button onclick="this.parentElement.parentElement.parentElement.remove()" class="btn-secondary">Close</button>
                <a href="/etl/" class="btn-primary">Go to ETL Processing</a>
            </div>
        </div>
    `;
    
    modal.style.cssText = `
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: rgba(0,0,0,0.5);
        display: flex;
        align-items: center;
        justify-content: center;
        z-index: 1000;
        animation: fadeIn 0.3s ease;
    `;
    
    const style = document.createElement('style');
    style.textContent = `
        .modal-content {
            background: white;
            padding: 30px;
            border-radius: 12px;
            text-align: center;
            max-width: 400px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
        }
        .modal-content h3 {
            color: #27ae60;
            margin-bottom: 15px;
        }
        .modal-actions {
            display: flex;
            gap: 10px;
            margin-top: 20px;
            justify-content: center;
        }
        .btn-primary, .btn-secondary {
            padding: 8px 16px;
            border: none;
            border-radius: 6px;
            text-decoration: none;
            cursor: pointer;
            font-weight: 500;
        }
        .btn-primary {
            background: #27ae60;
            color: white;
        }
        .btn-secondary {
            background: #6c757d;
            color: white;
        }
        @keyframes fadeIn {
            from { opacity: 0; }
            to { opacity: 1; }
        }
    `;
    
    document.head.appendChild(style);
    document.body.appendChild(modal);
}

/**
 * Reset all upload statuses (for testing or admin reset)
 */
function resetAllUploads() {
    Object.keys(uploadStatus).forEach(uploadType => {
        updateUploadState(uploadType, 'available');
    });
    
    showToast('All upload statuses have been reset.', 'info');
}

// Add reset function to window for debugging
window.resetAllUploads = resetAllUploads;

/**
 * Create progress indicator
 */
function createProgressIndicator() {
    const progressBar = document.createElement('div');
    progressBar.className = 'upload-progress';
    progressBar.innerHTML = '<div class="progress-bar"></div>';
    document.body.appendChild(progressBar);
    
    updateProgressBar();
}

/**
 * Update progress bar based on completed uploads
 */
function updateProgressBar() {
    const totalUploads = Object.keys(uploadStatus).length;
    const completedUploads = Object.values(uploadStatus).filter(status => status === 'completed').length;
    const progress = (completedUploads / totalUploads) * 100;
    
    const progressBar = document.querySelector('.progress-bar');
    if (progressBar) {
        progressBar.style.width = `${progress}%`;
    }
}

/**
 * Create session status indicator
 */
function createSessionStatusIndicator() {
    const statusIndicator = document.createElement('div');
    statusIndicator.className = 'session-status';
    statusIndicator.innerHTML = getSessionStatusText();
    document.body.appendChild(statusIndicator);
    
    // Update every 30 seconds
    setInterval(() => {
        statusIndicator.innerHTML = getSessionStatusText();
    }, 30000);
}

/**
 * Get session status text
 * @returns {string} - Status text
 */
function getSessionStatusText() {
    const completedCount = Object.values(uploadStatus).filter(status => status === 'completed').length;
    const totalCount = Object.keys(uploadStatus).length;
    const sessionTime = new Date().toLocaleTimeString();
    
    return `Session: ${completedCount}/${totalCount} completed • ${sessionTime}`;
}

/**
 * Update session status indicator
 */
function updateSessionStatus() {
    const sessionIndicator = document.querySelector('.session-status');
    if (sessionIndicator) {
        sessionIndicator.innerHTML = getSessionStatusText();
    }
}

/**
 * Handle message display and auto-hide functionality
 */
function setupMessageHandling() {
    const messagesContainer = document.querySelector('.messages');
    
    if (messagesContainer) {
        // Auto-hide messages after 5 seconds
        setTimeout(() => {
            hideMessages(messagesContainer);
        }, 5000);
        
        // Add click to dismiss functionality
        const alerts = messagesContainer.querySelectorAll('.alert');
        alerts.forEach(alert => {
            alert.style.cursor = 'pointer';
            alert.addEventListener('click', () => {
                hideMessage(alert);
            });
        });
    }
}

/**
 * Hide messages with smooth transition
 * @param {HTMLElement} messagesContainer - Container with messages
 */
function hideMessages(messagesContainer) {
    messagesContainer.style.transition = 'opacity 0.5s ease';
    messagesContainer.style.opacity = '0';
    
    setTimeout(() => {
        if (messagesContainer.parentNode) {
            messagesContainer.parentNode.removeChild(messagesContainer);
        }
    }, 500);
}

/**
 * Hide individual message
 * @param {HTMLElement} messageElement - Individual message element
 */
function hideMessage(messageElement) {
    messageElement.style.transition = 'opacity 0.3s ease';
    messageElement.style.opacity = '0';
    
    setTimeout(() => {
        if (messageElement.parentNode) {
            messageElement.parentNode.removeChild(messageElement);
        }
    }, 300);
}

/**
 * Set button to loading state
 * @param {HTMLElement} button - Submit button element
 * @param {string} originalText - Original button text
 */
function setButtonLoadingState(button, originalText) {
    button.disabled = true;
    button.innerHTML = '⏳ Uploading...';
    button.classList.add('loading');
}

/**
 * Reset button to original state
 * @param {HTMLElement} button - Submit button element
 * @param {string} originalText - Original button text
 */
function resetButtonState(button, originalText) {
    button.disabled = false;
    button.innerHTML = originalText;
    button.classList.remove('loading');
}

/**
 * Basic form validation
 * @param {HTMLFormElement} form - Form to validate
 * @returns {boolean} - Whether form is valid
 */
function validateForm(form) {
    const requiredFields = form.querySelectorAll('[required]');
    let isValid = true;
    
    requiredFields.forEach(field => {
        if (!field.value.trim()) {
            showFieldError(field, 'This field is required');
            isValid = false;
        } else {
            clearFieldError(field);
        }
    });
    
    // Validate file inputs
    const fileInputs = form.querySelectorAll('input[type="file"]');
    fileInputs.forEach(input => {
        if (input.hasAttribute('required') && !input.files.length) {
            showFieldError(input, 'Please select a file');
            isValid = false;
        }
    });
    
    return isValid;
}

/**
 * Show field error message
 * @param {HTMLElement} field - Form field with error
 * @param {string} message - Error message
 */
function showFieldError(field, message) {
    clearFieldError(field);
    
    const errorDiv = document.createElement('div');
    errorDiv.className = 'field-error';
    errorDiv.style.color = '#dc3545';
    errorDiv.style.fontSize = '12px';
    errorDiv.style.marginTop = '4px';
    errorDiv.textContent = message;
    
    field.parentNode.appendChild(errorDiv);
    field.style.borderColor = '#dc3545';
}

/**
 * Clear field error message
 * @param {HTMLElement} field - Form field to clear error from
 */
function clearFieldError(field) {
    const existingError = field.parentNode.querySelector('.field-error');
    if (existingError) {
        existingError.remove();
    }
    field.style.borderColor = '';
}

/**
 * Utility function to show toast notifications
 * @param {string} message - Message to display
 * @param {string} type - Type of notification (success, error, info)
 */
function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        background: ${getToastColor(type)};
        color: white;
        padding: 12px 20px;
        border-radius: 6px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.2);
        z-index: 1000;
        opacity: 0;
        transition: opacity 0.3s ease;
    `;
    toast.textContent = message;
    
    document.body.appendChild(toast);
    
    // Fade in
    setTimeout(() => {
        toast.style.opacity = '1';
    }, 100);
    
    // Auto remove after 4 seconds
    setTimeout(() => {
        toast.style.opacity = '0';
        setTimeout(() => {
            if (toast.parentNode) {
                toast.parentNode.removeChild(toast);
            }
        }, 300);
    }, 4000);
}

/**
 * Get toast background color based on type
 * @param {string} type - Toast type
 * @returns {string} - CSS color value
 */
function getToastColor(type) {
    const colors = {
        success: '#28a745',
        error: '#dc3545',
        warning: '#ffc107',
        info: '#17a2b8'
    };
    return colors[type] || colors.info;
}

/**
 * Handle file input changes to show selected file name
 */
document.addEventListener('change', function(event) {
    if (event.target.type === 'file') {
        handleFileInputChange(event.target);
    }
});

/**
 * Handle file input change to display selected file info
 * @param {HTMLInputElement} input - File input element
 */
function handleFileInputChange(input) {
    const fileName = input.files[0] ? input.files[0].name : 'No file selected';
    const fileSize = input.files[0] ? formatFileSize(input.files[0].size) : '';
    
    let fileInfo = input.parentNode.querySelector('.file-info');
    
    if (!fileInfo) {
        fileInfo = document.createElement('div');
        fileInfo.className = 'file-info';
        fileInfo.style.cssText = `
            font-size: 12px;
            color: #6c757d;
            margin-top: 4px;
        `;
        input.parentNode.appendChild(fileInfo);
    }
    
    fileInfo.textContent = fileName + (fileSize ? ` (${fileSize})` : '');
}

/**
 * Format file size for display
 * @param {number} bytes - File size in bytes
 * @returns {string} - Formatted file size
 */
function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

/**
 * Initialize drag and drop functionality for file inputs
 */
function initializeDragAndDrop() {
    const fileInputs = document.querySelectorAll('input[type="file"]');
    
    fileInputs.forEach(input => {
        const parentGroup = input.parentNode;
        
        // Add drag and drop styling
        parentGroup.addEventListener('dragover', function(e) {
            e.preventDefault();
            this.style.backgroundColor = '#f0f8ff';
            this.style.borderColor = '#27ae60';
        });
        
        parentGroup.addEventListener('dragleave', function(e) {
            e.preventDefault();
            this.style.backgroundColor = '';
            this.style.borderColor = '';
        });
        
        parentGroup.addEventListener('drop', function(e) {
            e.preventDefault();
            this.style.backgroundColor = '';
            this.style.borderColor = '';
            
            const files = e.dataTransfer.files;
            if (files.length > 0) {
                input.files = files;
                handleFileInputChange(input);
            }
        });
    });
}

/**
 * Add keyboard navigation support
 */
function initializeKeyboardNavigation() {
    document.addEventListener('keydown', function(e) {
        // Escape key to close any open modals or reset forms
        if (e.key === 'Escape') {
            handleEscapeKey();
        }
        
        // Ctrl/Cmd + S to save/submit current form
        if ((e.ctrlKey || e.metaKey) && e.key === 's') {
            e.preventDefault();
            submitActiveForm();
        }
    });
}

/**
 * Handle escape key press
 */
function handleEscapeKey() {
    // Clear any field errors
    const errorMessages = document.querySelectorAll('.field-error');
    errorMessages.forEach(error => error.remove());
    
    // Reset form styles
    const formInputs = document.querySelectorAll('.form-group input, .form-group select, .form-group textarea');
    formInputs.forEach(input => {
        input.style.borderColor = '';
    });
}

/**
 * Submit the currently active form
 */
function submitActiveForm() {
    const activeSection = document.querySelector('.upload-section.active');
    if (activeSection) {
        const form = activeSection.querySelector('.upload-form');
        if (form) {
            form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
        }
    }
}

/**
 * Add loading animation to buttons
 */
function addLoadingAnimation() {
    const style = document.createElement('style');
    style.textContent = `
        .submit-btn.loading {
            position: relative;
            overflow: hidden;
        }
        
        .submit-btn.loading::after {
            content: '';
            position: absolute;
            top: 0;
            left: -100%;
            width: 100%;
            height: 100%;
            background: linear-gradient(
                90deg,
                transparent,
                rgba(255, 255, 255, 0.2),
                transparent
            );
            animation: shimmer 2s infinite;
        }
        
        @keyframes shimmer {
            0% { left: -100%; }
            100% { left: 100%; }
        }
        
        .field-error {
            animation: slideDown 0.3s ease;
        }
        
        @keyframes slideDown {
            from {
                opacity: 0;
                transform: translateY(-10px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }
    `;
    document.head.appendChild(style);
}

// Export functions for potential testing or external use
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        initializeDashboard,
        handleSectionSwitch,
        validateForm,
        showToast,
        formatFileSize,
        updateUploadState,
        resetAllUploads,
        handleUploadSuccess,
        handleUploadFailure
    };
}