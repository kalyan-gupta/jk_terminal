/**
 * Helper to show/hide credentials dynamically.
 */
function toggleVisibility(id, value) {
    const element = document.getElementById(id);
    if (!element) return;
    const button = element.nextElementSibling;
    if (element.textContent.includes('•')) {
        element.textContent = value;
        if (button) {
            button.innerHTML = '<i class="bi bi-eye-slash"></i> Hide';
        }
    } else {
        element.textContent = '•'.repeat(value.length);
        if (button) {
            button.innerHTML = '<i class="bi bi-eye"></i> Show';
        }
    }
}
