// Shop and Cart Functionality

// Global variables
let products = [
    {
        id: 1,
        name: "Laptop Pro X",
        description: "Powerful laptop for professionals with high-performance specs",
        price: 1299.99,
        image: "https://images.unsplash.com/photo-1496181133206-80ce9b88a853?q=80&w=500"
    },
    {
        id: 2,
        name: "Smartphone Z",
        description: "Latest smartphone with cutting-edge camera and battery life",
        price: 899.99,
        image: "https://images.unsplash.com/photo-1598327105666-5b89351aff97?q=80&w=500"
    },
    {
        id: 3,
        name: "Wireless Headphones",
        description: "Premium noise-canceling wireless headphones",
        price: 249.99,
        image: "https://images.unsplash.com/photo-1505740420928-5e560c06d30e?q=80&w=500"
    },
    {
        id: 4,
        name: "Smart Watch",
        description: "Track fitness and stay connected with this smartwatch",
        price: 199.99,
        image: "https://images.unsplash.com/photo-1579586337278-3befd40fd17a?q=80&w=500"
    },
    {
        id: 5,
        name: "Tablet Ultra",
        description: "Thin and light tablet with stunning display",
        price: 499.99,
        image: "https://images.unsplash.com/photo-1544244015-0df4b3ffc6b0?q=80&w=500"
    },
    {
        id: 6,
        name: "Wireless Earbuds",
        description: "Compact earbuds with amazing sound quality",
        price: 129.99,
        image: "https://images.unsplash.com/photo-1606220588913-b3aacb4d2f37?q=80&w=500"
    }
];

let cart = [];

// Initialize when document is ready
$(document).ready(function() {
    // Make sqlEditor globally accessible for the view in Trino functionality
    window.sqlEditor = CodeMirror.fromTextArea(document.getElementById('sql-editor'), {
        mode: 'text/x-sql',
        theme: 'dracula',
        lineNumbers: true,
        indentWithTabs: false,
        indentUnit: 4,
        smartIndent: true,
        matchBrackets: true,
        autofocus: true,
        extraKeys: {
            'Ctrl-Enter': function() { $('#execute-query').click(); },
            'Cmd-Enter': function() { $('#execute-query').click(); }
        }
    });

    // Render products on page load
    renderProducts();
    
    // Initialize the cart from localStorage if available
    loadCartFromStorage();
    
    // Update the cart display
    updateCartDisplay();
    
    // Handle add to cart button clicks
    $(document).on('click', '.add-to-cart', function() {
        const productId = parseInt($(this).data('product-id'));
        addToCart(productId);
    });
    
    // Handle remove from cart button clicks
    $(document).on('click', '.remove-from-cart', function() {
        const productId = parseInt($(this).data('product-id'));
        removeFromCart(productId);
    });
    
    // Handle quantity change
    $(document).on('change', '.cart-quantity', function() {
        const productId = parseInt($(this).data('product-id'));
        const quantity = parseInt($(this).val());
        updateCartItemQuantity(productId, quantity);
    });
    
    // Handle checkout button and offcanvas checkout button clicks
    $('#checkout-btn, #offcanvas-checkout-btn').click(function() {
        if (cart.length === 0) {
            alert('Your cart is empty!');
            return;
        }
        
        // Show checkout form
        $('#cart-tab-content').hide();
        $('#checkout-tab-content').show();
        $('#checkout-btn').hide();
        $('#place-order-btn').show();
        
        // Switch to cart tab if clicked from offcanvas
        $('#cart-tab').tab('show');
        
        // Close offcanvas if open
        const offcanvas = bootstrap.Offcanvas.getInstance(document.getElementById('cartOffcanvas'));
        if (offcanvas) {
            offcanvas.hide();
        }
    });
    
    // Handle place order button click
    $('#place-order-btn').click(function(e) {
        e.preventDefault();
        
        // Validate the form
        if (!validateCheckoutForm()) {
            return;
        }
        
        // Generate order event for Kafka
        sendOrderToKafka();
    });
    
    // Handle back to cart button click
    $('#back-to-cart-btn').click(function() {
        $('#checkout-tab-content').hide();
        $('#cart-tab-content').show();
        $('#place-order-btn').hide();
        $('#checkout-btn').show();
    });

    // Update offcanvas cart when shown
    $('#cartOffcanvas').on('show.bs.offcanvas', function () {
        updateOffcanvasCart();
    });
});

// Function to render products
function renderProducts() {
    const productsContainer = $('#products-container');
    productsContainer.empty();
    
    products.forEach(product => {
        const productCard = `
            <div class="col-md-4 fade-in">
                <div class="product-card">
                    <div class="product-image" style="background-image: url('${product.image}')"></div>
                    <div class="product-body">
                        <h5 class="product-title">${product.name}</h5>
                        <p class="product-description">${product.description}</p>
                        <div class="product-price">$${product.price.toFixed(2)}</div>
                        <div class="product-actions">
                            <button class="btn btn-primary add-to-cart" data-product-id="${product.id}">
                                <i class="fas fa-shopping-cart"></i> Add to Cart
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        productsContainer.append(productCard);
    });
}

// Function to add item to cart
function addToCart(productId) {
    const product = products.find(p => p.id === productId);
    
    if (!product) {
        console.error('Product not found:', productId);
        return;
    }
    
    // Check if product is already in cart
    const existingItem = cart.find(item => item.product.id === productId);
    
    if (existingItem) {
        existingItem.quantity += 1;
    } else {
        cart.push({
            product: product,
            quantity: 1
        });
    }
    
    // Save cart to localStorage
    saveCartToStorage();
    
    // Update the cart display
    updateCartDisplay();
    updateOffcanvasCart();
    
    // Show added to cart notification
    showNotification(`${product.name} added to cart!`);
}

// Function to remove item from cart
function removeFromCart(productId) {
    cart = cart.filter(item => item.product.id !== productId);
    
    // Save cart to localStorage
    saveCartToStorage();
    
    // Update the cart display
    updateCartDisplay();
    updateOffcanvasCart();
}

// Function to update cart item quantity
function updateCartItemQuantity(productId, quantity) {
    if (quantity <= 0) {
        removeFromCart(productId);
        return;
    }
    
    const item = cart.find(item => item.product.id === productId);
    
    if (item) {
        item.quantity = quantity;
        
        // Save cart to localStorage
        saveCartToStorage();
        
        // Update the cart display
        updateCartDisplay();
        updateOffcanvasCart();
    }
}

// Function to update the cart display
function updateCartDisplay() {
    const cartItemsContainer = $('#cart-items');
    const cartBadge = $('#cart-badge');
    const cartTotal = $('#cart-total');
    const checkoutTotal = $('#checkout-total');
    const checkoutSubtotal = $('#checkout-subtotal');
    
    // Update cart badge
    const totalItems = cart.reduce((total, item) => total + item.quantity, 0);
    cartBadge.text(totalItems);
    
    // Show/hide empty cart message
    if (cart.length === 0) {
        cartItemsContainer.html('<div class="text-center py-4 text-muted">Your cart is empty</div>');
        $('#checkout-btn').prop('disabled', true);
        $('#cart-summary').addClass('d-none');
    } else {
        // Render cart items
        cartItemsContainer.empty();
        
        cart.forEach(item => {
            const itemTotal = item.product.price * item.quantity;
            
            const cartItem = `
                <div class="cart-item fade-in">
                    <div class="cart-item-details">
                        <div class="cart-item-title">${item.product.name}</div>
                        <div class="cart-item-price">${item.product.price.toFixed(2)} each</div>
                    </div>
                    <div class="cart-item-actions">
                        <button class="btn btn-sm btn-outline-danger remove-from-cart" data-product-id="${item.product.id}">
                            <i class="fas fa-trash"></i>
                        </button>
                        <input type="number" class="form-control cart-quantity" data-product-id="${item.product.id}" value="${item.quantity}" min="1">
                        <span class="ms-2">${itemTotal.toFixed(2)}</span>
                    </div>
                </div>
            `;
            
            cartItemsContainer.append(cartItem);
        });
        
        $('#checkout-btn').prop('disabled', false);
        $('#cart-summary').removeClass('d-none');
    }
    
    // Calculate and update total
    const total = calculateTotal();
    // Fix: remove $ since it's added via CSS
    cartTotal.text(total.toFixed(2));
    checkoutTotal.text(total.toFixed(2));
    checkoutSubtotal.text(total.toFixed(2));
}

// Function to update the offcanvas cart
function updateOffcanvasCart() {
    const cartItemsContainer = $('#offcanvas-cart-items');
    const cartTotal = $('#offcanvas-cart-total');
    const checkoutBtn = $('#offcanvas-checkout-btn');
    
    // Show/hide empty cart message
    if (cart.length === 0) {
        cartItemsContainer.html('<div class="text-center py-4 text-muted">Your cart is empty</div>');
        checkoutBtn.prop('disabled', true);
        $('.cart-summary').addClass('d-none');
    } else {
        // Render cart items
        cartItemsContainer.empty();
        
        cart.forEach(item => {
            const itemTotal = item.product.price * item.quantity;
            
            const cartItem = `
                <div class="cart-item fade-in">
                    <div class="cart-item-details">
                        <div class="cart-item-title">${item.product.name}</div>
                        <div class="cart-item-price">${item.product.price.toFixed(2)} × ${item.quantity}</div>
                    </div>
                    <div class="cart-item-actions">
                        <span class="fw-bold">${itemTotal.toFixed(2)}</span>
                    </div>
                </div>
            `;
            
            cartItemsContainer.append(cartItem);
        });
        
        checkoutBtn.prop('disabled', false);
        $('.cart-summary').removeClass('d-none');
    }
    
    // Calculate and update total
    const total = calculateTotal();
    // Fix: remove $ since it's added via CSS
    cartTotal.text(total.toFixed(2));
}

// Function to calculate cart total
function calculateTotal() {
    return cart.reduce((total, item) => total + (item.product.price * item.quantity), 0);
}

// Function to save cart to localStorage
function saveCartToStorage() {
    // We can't store objects directly in localStorage, so convert to JSON string
    localStorage.setItem('cart', JSON.stringify(cart));
}

// Function to load cart from localStorage
function loadCartFromStorage() {
    const savedCart = localStorage.getItem('cart');
    
    if (savedCart) {
        try {
            // Parse JSON string back to object
            const parsedCart = JSON.parse(savedCart);
            
            // Validate cart items against current products
            cart = parsedCart.filter(item => {
                const productExists = products.some(p => p.id === item.product.id);
                return productExists && item.quantity > 0;
            });
        } catch (e) {
            console.error('Error loading cart from storage:', e);
            cart = [];
        }
    }
}

// Show notification
function showNotification(message) {
    const notification = $('#notification');
    notification.text(message);
    notification.addClass('show');
    
    // Hide after 3 seconds
    setTimeout(() => {
        notification.removeClass('show');
    }, 3000);
}

// Function to validate checkout form
function validateCheckoutForm() {
    const form = document.getElementById('checkout-form');
    
    if (!form.checkValidity()) {
        form.reportValidity();
        return false;
    }
    
    return true;
}

// Function to send order to Kafka
function sendOrderToKafka() {
    // Get form data
    const formData = {
        firstName: $('#firstName').val(),
        lastName: $('#lastName').val(),
        email: $('#email').val(),
        address: $('#address').val(),
        country: $('#country').val(),
        state: $('#state').val(),
        zip: $('#zip').val()
    };
    
    // Create order event with enhanced structure for easier processing
    const orderEvent = {
        id: 'order_' + Date.now(),
        type: 'order',
        timestamp: new Date().toISOString(),
        customer: {
            firstName: formData.firstName,
            lastName: formData.lastName,
            email: formData.email
        },
        shipping: {
            address: formData.address,
            country: formData.country,
            state: formData.state,
            zip: formData.zip
        },
        items: cart.map(item => ({
            product_id: item.product.id,
            product_name: item.product.name,
            price: item.product.price,
            quantity: item.quantity,
            subtotal: item.product.price * item.quantity
        })),
        total: calculateTotal(),
        status: "PENDING",
        payment_method: $('input[name="paymentMethod"]:checked').attr('id'),
        order_date: new Date().toISOString().split('T')[0]  // Just the date portion for easier analysis
    };
    
    // Show loading state
    $('#place-order-btn').prop('disabled', true).html('<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Processing...');
    
    // Send order to Kafka
    $.ajax({
        url: '/send_event',
        type: 'POST',
        contentType: 'application/json',
        data: JSON.stringify({ 
            event_data: orderEvent,
            use_docker_method: true
        }),
        success: function(response) {
            // Show success message with more details and animation
            $('#checkout-tab-content').html(`
                <div class="alert alert-success mb-4 fade-in">
                    <h4 class="alert-heading"><i class="fas fa-check-circle me-2"></i>Order Placed Successfully!</h4>
                    <p>Your order has been received and will be processed soon.</p>
                    <p><strong>Order ID:</strong> ${orderEvent.id}</p>
                    <p><strong>Total Amount:</strong> $${orderEvent.total.toFixed(2)}</p>
                    <p><strong>Order Date:</strong> ${new Date().toLocaleString()}</p>
                </div>
                <div class="card mb-4 fade-in" style="animation-delay: 0.2s">
                    <div class="card-header">
                        <h5 class="mb-0">Order Summary</h5>
                    </div>
                    <div class="card-body">
                        <h6>Ordered Items:</h6>
                        <ul class="list-group mb-3">
                            ${orderEvent.items.map(item => `
                                <li class="list-group-item d-flex justify-content-between align-items-center">
                                    ${item.product_name} × ${item.quantity}
                                    <span class="badge bg-primary rounded-pill">$${item.subtotal.toFixed(2)}</span>
                                </li>
                            `).join('')}
                        </ul>
                        <h6>Shipping to:</h6>
                        <p class="mb-0">${formData.firstName} ${formData.lastName}</p>
                        <p class="mb-0">${formData.address}</p>
                        <p class="mb-0">${formData.state}, ${formData.zip}</p>
                        <p>${formData.country}</p>
                    </div>
                </div>
                <div class="text-center fade-in" style="animation-delay: 0.4s">
                    <button id="view-in-trino-btn" class="btn btn-primary me-2">
                        <i class="fas fa-database me-2"></i>View in Trino
                    </button>
                    <button id="continue-shopping-btn" class="btn btn-success">
                        <i class="fas fa-shopping-bag me-2"></i>Continue Shopping
                    </button>
                </div>
            `);
            
            // Clear cart
            cart = [];
            saveCartToStorage();
            updateCartDisplay();
            updateOffcanvasCart();
            
            // Handle continue shopping button
            $('#continue-shopping-btn').click(function() {
                $('#shop-tab').tab('show');
            });
            
            // Handle view in Trino button
            $('#view-in-trino-btn').click(function() {
                // Switch to SQL tab
                $('#sql-tab').tab('show');
                
                // Set query to view this order
                const query = `SELECT * FROM kafka.default.events_topic WHERE _message LIKE '%${orderEvent.id}%' LIMIT 10;`;
                
                // Execute query
                window.sqlEditor.setValue(query);
                setTimeout(() => {
                    $('#execute-query').click();
                }, 300);
            });
        },
        error: function(xhr) {
            // Parse error
            let errorMessage = 'An error occurred while processing your order.';
            try {
                const response = JSON.parse(xhr.responseText);
                if (response.error) {
                    errorMessage = response.error;
                }
            } catch (e) {
                console.error('Error parsing error response:', e);
            }
            
            // Show error message with more details
            $('#checkout-error').removeClass('d-none').html(`
                <div class="d-flex align-items-center">
                    <i class="fas fa-exclamation-circle text-danger me-3 fa-2x"></i>
                    <div>
                        <strong>Order could not be processed</strong><br>
                        ${errorMessage}
                    </div>
                </div>
            `);
            
            // Reset button
            $('#place-order-btn').prop('disabled', false).html('<i class="fas fa-check me-2"></i>Place Order');
        }
    });
}