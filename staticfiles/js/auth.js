// app/accounts/static/js/auth.js

document.addEventListener('DOMContentLoaded', function() {
  const positionSelect = document.getElementById('id_position');
  const customField = document.getElementById('custom-position-field');

  if (positionSelect && customField) {
    function toggleCustomField() {
      if (positionSelect.value === "Other") {
        customField.style.display = "block";
      } else {
        customField.style.display = "none";
      }
    }

    // Run once on load
    toggleCustomField();

    // Run when dropdown changes
    positionSelect.addEventListener('change', toggleCustomField);
  }
});
