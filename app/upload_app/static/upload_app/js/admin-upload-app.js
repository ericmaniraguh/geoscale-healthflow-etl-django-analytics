// Admin Dashboard JavaScript
// Handle sidebar navigation
document.querySelectorAll('.upload-item').forEach(item => {
    item.addEventListener('click', () => {
        // Remove active class from all items and forms
        document.querySelectorAll('.upload-item').forEach(i => i.classList.remove('active'));
        document.querySelectorAll('.upload-section').forEach(s => s.classList.remove('active'));
        
        // Add active class to clicked item
        item.classList.add('active');
        const upload = item.dataset.upload;
        document.getElementById(upload + '-form').classList.add('active');
    });
});

// Auto-hide messages after 5 seconds
setTimeout(() => {
    const messages = document.querySelector('.messages');
    if (messages) {
        messages.style.transition = 'opacity 0.5s';
        messages.style.opacity = '0';
        setTimeout(() => messages.remove(), 500);
    }
}, 5000);

// Form submission loading states
document.querySelectorAll('.upload-form').forEach(form => {
    form.addEventListener('submit', function() {
        const button = this.querySelector('.submit-btn');
        button.disabled = true;
        const originalText = button.innerHTML;
        button.innerHTML = '⏳ Uploading...';
        
        // Reset after 30 seconds in case of issues
        setTimeout(() => {
            button.disabled = false;
            button.innerHTML = originalText;
        }, 30000);
    });
});

// Admin-specific features (if needed later)
// You can add admin-specific JavaScript functionality here

// Example: Admin analytics tracking (optional)
function trackAdminAction(action, formType) {
    if (typeof gtag !== 'undefined') {
        gtag('event', 'admin_action', {
            'action': action,
            'form_type': formType,
            'user_role': 'admin'
        });
    }
}

// Add analytics tracking to form submissions (optional)
document.querySelectorAll('.upload-form').forEach(form => {
    form.addEventListener('submit', function() {
        const formId = this.closest('.upload-section').id;
        trackAdminAction('form_submit', formId);
    });
});

// Add analytics tracking to navigation clicks (optional)
document.querySelectorAll('.upload-item').forEach(item => {
    item.addEventListener('click', function() {
        const uploadType = this.dataset.upload;
        trackAdminAction('navigation_click', uploadType);
    });
});