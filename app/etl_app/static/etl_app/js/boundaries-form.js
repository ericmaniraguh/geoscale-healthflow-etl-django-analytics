// static/etl_app/js/boundaries-form.js

document.addEventListener('DOMContentLoaded', function() {
    // Initialize boundaries form functionality
    initializeBoundariesForm();
});

function initializeBoundariesForm() {
    // Get form elements
    const showAvailableCheckbox = document.getElementById('show_available_checkbox');
    const filterSection = document.getElementById('filter-section');
    const boundariesBtnText = document.getElementById('boundaries-btn-text');
    const districtInput = document.getElementById('bounds_district');
    const sectorInput = document.getElementById('bounds_sector');
    const provinceSelect = document.getElementById('bounds_province');
    
    // Initialize debug mode toggle
    if (showAvailableCheckbox) {
        initializeDebugMode(showAvailableCheckbox, filterSection, boundariesBtnText, districtInput, sectorInput);
    }
    
    // Initialize province selection helper
    if (provinceSelect && districtInput) {
        initializeProvinceHelper(provinceSelect, districtInput);
    }
    
    // Initialize form validation
    initializeFormValidation();
}

function initializeDebugMode(checkbox, filterSection, btnText, districtInput, sectorInput) {
    function toggleDebugMode() {
        if (checkbox.checked) {
            // Debug mode
            filterSection.style.opacity = '0.6';
            districtInput.required = true;
            sectorInput.required = true;
            btnText.textContent = 'Debug MongoDB & Show Available Data';
            
            // Add debug styling
            filterSection.classList.add('debug-mode');
        } else {
            // Normal mode
            filterSection.style.opacity = '1';
            districtInput.required = true; // Optional since you can extract all data
            sectorInput.required = true;   // Optional since you can extract all data
            btnText.textContent = 'Extract Boundary Data';
            
            // Remove debug styling
            filterSection.classList.remove('debug-mode');
        }
    }
    
    checkbox.addEventListener('change', toggleDebugMode);
    toggleDebugMode(); // Initial setup
}

function initializeProvinceHelper(provinceSelect, districtInput) {
    // District data for each province
    const districtsByProvince = {
        'Amajyepfo': ['Gisagara', 'Huye', 'Nyamagabe', 'Nyanza', 'Ruhango'],
        'Amajyaruguru': ['Burera', 'Gakenke', 'Gicumbi', 'Musanze', 'Rulindo'],
        'Iburasirazuba': ['Karongi', 'Ngororero', 'Nyabihu', 'Rubavu', 'Rusizi', 'Rutsiro'],
        'Iburasiburihuzi': ['Bugesera', 'Gatsibo', 'Kayonza', 'Kirehe', 'Ngoma', 'Nyagatare', 'Rwamagana'],
        'Kigali': ['Gasabo', 'Kicukiro', 'Nyarugenge']
    };
    
    provinceSelect.addEventListener('change', function() {
        const selectedProvince = this.value;
        if (selectedProvince && districtsByProvince[selectedProvince]) {
            const districts = districtsByProvince[selectedProvince];
            districtInput.placeholder = `e.g., ${districts.slice(0, 3).join(', ')}`;
            
            // Optionally populate district dropdown if it's a select element
            updateDistrictOptions(selectedProvince, districts);
        } else {
            districtInput.placeholder = 'e.g., Gisagara, Bugesera, Kigali';
            clearDistrictOptions();
        }
    });
}

function updateDistrictOptions(province, districts) {
    // If district input is actually a select element, update its options
    const districtSelect = document.getElementById('bounds_district_select');
    if (districtSelect) {
        // Clear existing options except the first one
        while (districtSelect.children.length > 1) {
            districtSelect.removeChild(districtSelect.lastChild);
        }
        
        // Add district options
        districts.forEach(district => {
            const option = document.createElement('option');
            option.value = district;
            option.textContent = district;
            districtSelect.appendChild(option);
        });
    }
}

function clearDistrictOptions() {
    const districtSelect = document.getElementById('bounds_district_select');
    if (districtSelect) {
        // Clear all options except the first one
        while (districtSelect.children.length > 1) {
            districtSelect.removeChild(districtSelect.lastChild);
        }
    }
}

function initializeFormValidation() {
    const form = document.querySelector('#boundaries-form form');
    if (!form) return;
    
    form.addEventListener('submit', function(e) {
        const submitBtn = this.querySelector('.submit-btn');
        const debugMode = document.getElementById('show_available_checkbox').checked;
        
        if (submitBtn) {
            // Show loading state
            submitBtn.disabled = true;
            const originalText = submitBtn.innerHTML;
            submitBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Processing...';
            
            // Re-enable button and restore text after timeout (in case of errors)
            setTimeout(() => {
                submitBtn.disabled = false;
                submitBtn.innerHTML = originalText;
            }, 30000); // 30 seconds timeout
        }
        
        // Log form data for debugging
        const formData = new FormData(this);
        console.log('Boundaries form submission:', {
            district: formData.get('district'),
            sector: formData.get('sector'),
            province: formData.get('province'),
            show_available: formData.get('show_available'),
            update_mode: formData.get('update_mode'),
            debug_mode: debugMode
        });
    });
}

// Utility functions for form interaction
function showFormMessage(message, type = 'info') {
    const messageDiv = document.createElement('div');
    messageDiv.className = `alert alert-${type} boundaries-form-message`;
    messageDiv.innerHTML = `
        <div class="message-content">
            <i class="fas fa-${getMessageIcon(type)}"></i>
            <div class="message-text">${message}</div>
        </div>
        <button type="button" class="close" aria-label="Close">
            <span aria-hidden="true">&times;</span>
        </button>
    `;
    
    const form = document.querySelector('#boundaries-form');
    if (form) {
        // Remove any existing messages
        clearFormMessages();
        
        // Add new message at the top of the form
        form.insertBefore(messageDiv, form.firstChild);
        
        // Auto-hide after 15 seconds for success, 30 seconds for errors
        const timeout = type === 'success' ? 15000 : 30000;
        setTimeout(() => {
            if (messageDiv.parentNode) {
                messageDiv.style.transition = 'opacity 0.5s ease';
                messageDiv.style.opacity = '0';
                setTimeout(() => {
                    if (messageDiv.parentNode) {
                        messageDiv.parentNode.removeChild(messageDiv);
                    }
                }, 500);
            }
        }, timeout);
        
        // Close button functionality
        const closeBtn = messageDiv.querySelector('.close');
        if (closeBtn) {
            closeBtn.addEventListener('click', () => {
                if (messageDiv.parentNode) {
                    messageDiv.style.transition = 'opacity 0.3s ease';
                    messageDiv.style.opacity = '0';
                    setTimeout(() => {
                        if (messageDiv.parentNode) {
                            messageDiv.parentNode.removeChild(messageDiv);
                        }
                    }, 300);
                }
            });
        }
    }
}

function getMessageIcon(type) {
    switch (type) {
        case 'success': return 'check-circle';
        case 'error': return 'exclamation-triangle';
        case 'warning': return 'exclamation-circle';
        default: return 'info-circle';
    }
}

function clearFormMessages() {
    const existingMessages = document.querySelectorAll('.boundaries-form-message');
    existingMessages.forEach(msg => {
        if (msg.parentNode) {
            msg.parentNode.removeChild(msg);
        }
    });
}

function scrollToMessage() {
    const message = document.querySelector('.boundaries-form-message');
    if (message) {
        message.scrollIntoView({ 
            behavior: 'smooth', 
            block: 'center' 
        });
    }
}

function resetForm() {
    const form = document.querySelector('#boundaries-form form');
    if (form) {
        form.reset();
        
        // Reset debug mode
        const debugCheckbox = document.getElementById('show_available_checkbox');
        if (debugCheckbox) {
            debugCheckbox.checked = false;
            debugCheckbox.dispatchEvent(new Event('change'));
        }
    }
}

// Export functions for external use
window.BoundariesForm = {
    showMessage: showFormMessage,
    reset: resetForm,
    initialize: initializeBoundariesForm
};