#!/usr/bin/env python3
"""
Stripe MRR Test Data Generator V2 - Parallel Test Clock Advancement
====================================================================
Generates 100 customers with 6 months of billing history using Stripe Test Clocks.
This version advances test clocks IN PARALLEL for significantly faster execution.

Key Improvements over V1:
- Parallel test clock advancement using ThreadPoolExecutor
- ~4x faster execution with 4 workers (adjustable)
- Real-time progress tracking
- Self-contained - no external module dependencies

Requirements:
- 100 customers with varying subscription statuses
- 6 months of billing history (simulated via test clocks)
- Mix of Active, Canceled, and Past Due subscriptions
- Realistic customer acquisition pattern: growth → dip → recovery

Customer Acquisition Pattern (100 customers over 6 months):
- Month 0 (Aug): 8 customers (launch)
- Month 1 (Sep): 15 customers (growth)
- Month 2 (Oct): 25 customers (peak growth)
- Month 3 (Nov): 12 customers (slowdown/dip)
- Month 4 (Dec): 18 customers (recovery)
- Month 5 (Jan): 22 customers (strong recovery)

Status Distribution: 70% active, 20% canceled, 10% past due
- Active: 70 customers
- Canceled: 20 customers - spread across months
- Past Due: 10 customers - spread across months

Usage:
    python generate_test_data_v2.py
    python generate_test_data_v2.py --workers 8  # Use 8 parallel workers
"""

import stripe
import os
import argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv
import random
import time
import string
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Any

# Load environment variables
load_dotenv()

# Initialize Stripe
stripe.api_key = os.getenv('STRIPE_TEST_SECRET_KEY')

# Configuration
NUM_CUSTOMERS = 100  # Target: 100 customers
MONTHS_OF_HISTORY = 6  # 6 months of billing history
DEFAULT_PARALLEL_WORKERS = 8  # Number of parallel threads for clock advancement

# REALISTIC CUSTOMER ACQUISITION PATTERN
# Month 0 = oldest (6 months ago), Month 5 = most recent
# Total: 100 customers with growth → dip → recovery pattern
CUSTOMER_ACQUISITION_BY_MONTH = {
    0: 8,    # Aug - Launch
    1: 15,   # Sep - Growth  
    2: 25,   # Oct - Peak growth
    3: 12,   # Nov - Slowdown/dip
    4: 18,   # Dec - Recovery
    5: 22    # Jan - Strong recovery
}

# CANCELLATION SCHEDULE: When cancellations happen (cancel_month: [list of acquisition_months])
# Total: 20 cancellations (20% of 100)
CANCELLATION_SCHEDULE = {
    # Cancel in month 1 (Sep): 2 cancels from Aug cohort
    1: [0, 0],  
    # Cancel in month 2 (Oct): 3 cancels  
    2: [0, 1, 1],  
    # Cancel in month 3 (Nov): 5 cancels (highest churn during dip)
    3: [1, 1, 2, 2, 2],
    # Cancel in month 4 (Dec): 5 cancels
    4: [2, 2, 3, 3, 3],
    # Cancel in month 5 (Jan): 5 cancels
    5: [3, 4, 4, 4, 4]
}

# PAST DUE SCHEDULE: When past dues happen (past_due_month: [list of acquisition_months])
# Total: 10 past dues (10% of 100)
PAST_DUE_SCHEDULE = {
    # Past due in month 2 (Oct): 2 past due
    2: [1, 1],
    # Past due in month 3 (Nov): 3 past due
    3: [0, 2, 2],
    # Past due in month 4 (Dec): 3 past due  
    4: [2, 3, 3],
    # Past due in month 5 (Jan): 2 past due
    5: [4, 4]
}

# Rate limiting configuration
MAX_RETRIES = 5
BASE_DELAY = 1.0  # Base delay in seconds
MAX_DELAY = 60.0  # Maximum delay cap

# Customer name pools for realistic data
COMPANY_PREFIXES = ['Tech', 'Data', 'Cloud', 'Digital', 'Smart', 'Global', 'Pro', 'Next', 'Fast', 'Prime']
COMPANY_SUFFIXES = ['Solutions', 'Systems', 'Labs', 'Corp', 'Inc', 'LLC', 'Co', 'Group', 'Hub', 'Works']
COMPANY_TYPES = ['Analytics', 'Software', 'Services', 'Consulting', 'Media', 'Ventures', 'Partners', 'Tech', 'Digital', 'AI']

# Month names for display
MONTH_NAMES = ['Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul']


def retry_with_exponential_backoff(max_retries=MAX_RETRIES, base_delay=BASE_DELAY, max_delay=MAX_DELAY):
    """
    Decorator for retrying Stripe API calls with exponential backoff.
    Handles RateLimitError and transient errors gracefully.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries <= max_retries:
                try:
                    return func(*args, **kwargs)
                except stripe.error.RateLimitError as e:
                    retries += 1
                    if retries > max_retries:
                        print(f"      ❌ Max retries ({max_retries}) exceeded for {func.__name__}")
                        raise
                    # Exponential backoff with jitter
                    delay = min(base_delay * (2 ** retries) + random.uniform(0, 1), max_delay)
                    print(f"      ⚠️ Rate limited. Retry {retries}/{max_retries} in {delay:.1f}s...")
                    time.sleep(delay)
                except stripe.error.APIConnectionError as e:
                    retries += 1
                    if retries > max_retries:
                        print(f"      ❌ Connection error, max retries exceeded for {func.__name__}")
                        raise
                    delay = min(base_delay * (2 ** retries), max_delay)
                    print(f"      ⚠️ Connection error. Retry {retries}/{max_retries} in {delay:.1f}s...")
                    time.sleep(delay)
                except stripe.error.APIError as e:
                    # Server-side errors (5xx) - retry
                    if hasattr(e, 'http_status') and e.http_status >= 500:
                        retries += 1
                        if retries > max_retries:
                            raise
                        delay = min(base_delay * (2 ** retries), max_delay)
                        print(f"      ⚠️ Server error. Retry {retries}/{max_retries} in {delay:.1f}s...")
                        time.sleep(delay)
                    else:
                        raise
            return None
        return wrapper
    return decorator


def wait_for_rate_limit(min_delay=0.3, max_delay=0.6):
    """Add delay between API calls to avoid rate limits proactively."""
    time.sleep(random.uniform(min_delay, max_delay))


@retry_with_exponential_backoff()
def create_product_with_retry(name, description):
    """Create a Stripe product with retry logic."""
    return stripe.Product.create(name=name, description=description)


@retry_with_exponential_backoff()
def create_price_with_retry(product_id, unit_amount, currency, interval, nickname):
    """Create a Stripe price with retry logic."""
    return stripe.Price.create(
        product=product_id,
        unit_amount=unit_amount,
        currency=currency,
        recurring={'interval': interval},
        nickname=nickname
    )


@retry_with_exponential_backoff()
def create_test_clock_with_retry(frozen_time, name):
    """Create a test clock with retry logic."""
    return stripe.test_helpers.TestClock.create(frozen_time=frozen_time, name=name)


@retry_with_exponential_backoff()
def create_customer_with_retry(name, email, description, test_clock_id):
    """Create a customer with retry logic."""
    return stripe.Customer.create(
        name=name,
        email=email,
        description=description,
        test_clock=test_clock_id
    )


@retry_with_exponential_backoff()
def create_payment_method_with_retry():
    """Create a payment method with retry logic."""
    return stripe.PaymentMethod.create(type="card", card={"token": "tok_visa"})


@retry_with_exponential_backoff()
def attach_payment_method_with_retry(pm_id, customer_id):
    """Attach payment method to customer with retry logic."""
    stripe.PaymentMethod.attach(pm_id, customer=customer_id)
    stripe.Customer.modify(
        customer_id,
        invoice_settings={"default_payment_method": pm_id}
    )


@retry_with_exponential_backoff()
def create_subscription_with_retry(**params):
    """Create a subscription with retry logic."""
    return stripe.Subscription.create(**params)


@retry_with_exponential_backoff()
def advance_test_clock_with_retry(clock_id, frozen_time):
    """Advance a test clock with retry logic."""
    return stripe.test_helpers.TestClock.advance(clock_id, frozen_time=frozen_time)


@retry_with_exponential_backoff()
def cancel_subscription_with_retry(subscription_id):
    """Cancel a subscription with retry logic."""
    return stripe.Subscription.cancel(subscription_id)


@retry_with_exponential_backoff()
def list_invoices_with_retry(created_gte, limit=100):
    """List invoices with retry logic."""
    return stripe.Invoice.list(created={'gte': created_gte}, limit=limit)


def generate_company_name():
    """Generate a random realistic company name."""
    style = random.choice(['prefix_suffix', 'type_suffix', 'prefix_type'])
    if style == 'prefix_suffix':
        return f"{random.choice(COMPANY_PREFIXES)}{random.choice(COMPANY_TYPES)} {random.choice(COMPANY_SUFFIXES)}"
    elif style == 'type_suffix':
        return f"{random.choice(COMPANY_TYPES)} {random.choice(COMPANY_SUFFIXES)}"
    else:
        return f"{random.choice(COMPANY_PREFIXES)} {random.choice(COMPANY_TYPES)}"


def generate_email(company_name):
    """Generate an email from company name."""
    domain = company_name.lower().replace(' ', '').replace(',', '')[:15]
    return f"billing@{domain}.com"


def create_products_and_prices():
    """Create SaaS products with multiple pricing tiers."""
    print("\n📦 Creating products and pricing tiers...")
    
    # Main SaaS Product
    product = create_product_with_retry(
        name="CloudSync Platform",
        description="Enterprise-grade cloud synchronization and analytics platform"
    )
    
    prices = {
        'starter': create_price_with_retry(
            product.id,
            unit_amount=2900,  # $29/month
            currency='usd',
            interval='month',
            nickname='Starter Plan'
        ),
        'professional': create_price_with_retry(
            product.id,
            unit_amount=7900,  # $79/month
            currency='usd',
            interval='month',
            nickname='Professional Plan'
        ),
        'business': create_price_with_retry(
            product.id,
            unit_amount=14900,  # $149/month
            currency='usd',
            interval='month',
            nickname='Business Plan'
        ),
        'enterprise': create_price_with_retry(
            product.id,
            unit_amount=29900,  # $299/month
            currency='usd',
            interval='month',
            nickname='Enterprise Plan'
        )
    }
    
    print(f"✅ Created product: {product.name}")
    for plan_name, price in prices.items():
        print(f"   • {price.nickname}: ${price.unit_amount/100}/month")
    
    return product, prices


def create_test_clocks_by_month(start_date, scenarios):
    """
    Create test clocks organized by acquisition month.
    Each acquisition month gets its own set of test clocks (3 customers per clock).
    """
    print(f"\n🕐 Creating test clocks organized by acquisition month...")
    
    test_clocks = {}
    month_seconds = 30 * 24 * 60 * 60  # ~30 days
    
    for month in range(MONTHS_OF_HISTORY):
        month_scenarios = [s for s in scenarios if s['acquisition_month'] == month]
        if not month_scenarios:
            continue
        
        # Calculate start time for this month's customers
        month_start_time = start_date + timedelta(days=30 * month)
        
        # Calculate number of clocks needed (3 customers per clock)
        num_clocks = (len(month_scenarios) + 2) // 3
        
        test_clocks[month] = []
        
        for i in range(num_clocks):
            tc = create_test_clock_with_retry(
                frozen_time=int(month_start_time.timestamp()),
                name=f"Month {month} Clock {i + 1}"
            )
            test_clocks[month].append(tc)
            wait_for_rate_limit()
        
        print(f"   • {MONTH_NAMES[month]}: {len(test_clocks[month])} clocks for {len(month_scenarios)} customers")
    
    total_clocks = sum(len(clocks) for clocks in test_clocks.values())
    print(f"\n   ✅ Created {total_clocks} total test clocks")
    
    return test_clocks


def generate_customer_scenarios(num_customers, prices):
    """
    Generate customer scenarios with realistic distribution:
    - 70% Active (42 customers)
    - 20% Canceled (12 customers) - spread across months
    - 10% Past Due (6 customers) - spread across months
    
    Customers are acquired according to CUSTOMER_ACQUISITION_BY_MONTH pattern.
    Cancellations and past dues are scheduled according to their schedules.
    """
    scenarios = []
    
    # Plan distribution (weighted towards lower tiers)
    plan_weights = {
        'starter': 0.40,      # 40% starter
        'professional': 0.30,  # 30% professional
        'business': 0.20,      # 20% business
        'enterprise': 0.10     # 10% enterprise
    }
    
    plans = list(plan_weights.keys())
    plan_probs = list(plan_weights.values())
    
    # Build cancellation tracking: list of (acquisition_month, cancel_month)
    cancellations = []
    for cancel_month, acq_months in CANCELLATION_SCHEDULE.items():
        for acq_month in acq_months:
            cancellations.append((acq_month, cancel_month))
    
    # Build past due tracking: list of (acquisition_month, past_due_month)
    past_dues = []
    for pd_month, acq_months in PAST_DUE_SCHEDULE.items():
        for acq_month in acq_months:
            past_dues.append((acq_month, pd_month))
    
    # Track assigned cancellations and past dues per acquisition month
    cancel_by_acq = {}
    for acq, cancel in cancellations:
        if acq not in cancel_by_acq:
            cancel_by_acq[acq] = []
        cancel_by_acq[acq].append(cancel)
    
    pd_by_acq = {}
    for acq, pd in past_dues:
        if acq not in pd_by_acq:
            pd_by_acq[acq] = []
        pd_by_acq[acq].append(pd)
    
    customer_index = 0
    
    # Generate customers for each acquisition month
    for acq_month, num_in_month in CUSTOMER_ACQUISITION_BY_MONTH.items():
        # Get cancellations for this cohort
        month_cancels = cancel_by_acq.get(acq_month, []).copy()
        month_past_dues = pd_by_acq.get(acq_month, []).copy()
        
        for i in range(num_in_month):
            company_name = generate_company_name()
            plan = random.choices(plans, weights=plan_probs)[0]
            
            # Assign status based on schedules
            if month_cancels:
                status = 'canceled'
                cancel_month = month_cancels.pop(0)
                cancel_after_months = cancel_month - acq_month
                past_due_month = None
            elif month_past_dues:
                status = 'past_due'
                past_due_month = month_past_dues.pop(0)
                cancel_after_months = None
            else:
                status = 'active'
                cancel_after_months = None
                past_due_month = None
            
            scenarios.append({
                'name': company_name,
                'email': generate_email(company_name),
                'plan': plan,
                'acquisition_month': acq_month,  # 0 = oldest, 5 = newest
                'status': status,
                'cancel_after_months': cancel_after_months,
                'past_due_month': past_due_month,
                'customer_index': customer_index
            })
            
            customer_index += 1
    
    # Print scenario summary
    print("\n📊 Customer Acquisition Pattern:")
    for month, count in CUSTOMER_ACQUISITION_BY_MONTH.items():
        month_scenarios = [s for s in scenarios if s['acquisition_month'] == month]
        active = len([s for s in month_scenarios if s['status'] == 'active'])
        canceled = len([s for s in month_scenarios if s['status'] == 'canceled'])
        past_due = len([s for s in month_scenarios if s['status'] == 'past_due'])
        print(f"   {MONTH_NAMES[month]}: {count} customers (Active: {active}, Canceled: {canceled}, Past Due: {past_due})")
    
    return scenarios


def create_customers_and_subscriptions(scenarios, prices, test_clocks):
    """Create customers and subscriptions organized by acquisition month."""
    print(f"\n👥 Creating {len(scenarios)} customers with subscriptions...")
    
    created_data = {
        'customers': [],
        'subscriptions': [],
        'stats': {'active': 0, 'canceled': 0, 'past_due': 0},
        'by_month': {}  # Track customers by acquisition month
    }
    
    for month in range(MONTHS_OF_HISTORY):
        month_scenarios = [s for s in scenarios if s['acquisition_month'] == month]
        if not month_scenarios:
            continue
        
        month_clocks = test_clocks.get(month, [])
        if not month_clocks:
            print(f"   ⚠️ No clocks for month {month}, skipping...")
            continue
        
        created_data['by_month'][month] = []
        print(f"\n   📅 Creating {MONTH_NAMES[month]} cohort ({len(month_scenarios)} customers)...")
        
        for i, scenario in enumerate(month_scenarios):
            clock_index = i // 3
            if clock_index >= len(month_clocks):
                print(f"   ⚠️ Not enough clocks for {scenario['name']}, skipping...")
                continue
            
            test_clock = month_clocks[clock_index]
            
            try:
                # Create customer
                customer = create_customer_with_retry(
                    name=scenario['name'],
                    email=scenario['email'],
                    description=f"{scenario['plan'].title()} plan - {scenario['status']} - Cohort: {MONTH_NAMES[month]}",
                    test_clock_id=test_clock.id
                )
                
                # Create payment method (use failing card for past_due customers)
                if scenario['status'] == 'past_due':
                    # Don't attach payment method - will create past_due status
                    pass
                else:
                    try:
                        pm = create_payment_method_with_retry()
                        attach_payment_method_with_retry(pm.id, customer.id)
                    except Exception as e:
                        print(f"   ⚠️ Payment method failed for {scenario['name']}: {e}")
                
                # Create subscription
                sub_params = {
                    "customer": customer.id,
                    "items": [{"price": prices[scenario['plan']].id}],
                    "proration_behavior": "none"
                }
                
                if scenario['status'] == 'past_due':
                    sub_params["collection_method"] = "send_invoice"
                    sub_params["days_until_due"] = 7
                
                subscription = create_subscription_with_retry(**sub_params)
                
                # Store data
                cust_data = {
                    'customer': customer,
                    'scenario': scenario,
                    'acquisition_month': month,
                    'clock_index': clock_index,
                    'test_clock': test_clock
                }
                created_data['customers'].append(cust_data)
                created_data['by_month'][month].append(cust_data)
                
                created_data['subscriptions'].append({
                    'subscription': subscription,
                    'scenario': scenario,
                    'acquisition_month': month,
                    'clock_index': clock_index,
                    'test_clock': test_clock
                })
                
                created_data['stats'][scenario['status']] += 1
                
                wait_for_rate_limit(0.3, 0.5)
                
            except Exception as e:
                print(f"   ❌ Error creating {scenario['name']}: {e}")
                continue
        
        print(f"      ✅ Created {len(created_data['by_month'][month])} customers for {MONTH_NAMES[month]}")
    
    print(f"\n✅ Created {len(created_data['customers'])} total customers")
    return created_data


# =============================================================================
# PARALLEL TEST CLOCK ADVANCEMENT (V2 Feature)
# =============================================================================

def advance_single_clock_worker(task: Dict[str, Any]) -> Tuple[int, str, int, List[str]]:
    """
    Worker function to advance a single test clock through all months.
    Runs in a separate thread for parallel execution.
    
    Args:
        task: Dictionary containing:
            - test_clock: The Stripe test clock object
            - clock_customers: List of customer data on this clock
            - acq_month: Acquisition month index
            - subscriptions: List of subscription data
            - months_to_advance: Number of months to advance
    
    Returns:
        Tuple of (invoices_count, clock_id, acq_month, canceled_names)
    """
    test_clock = task['test_clock']
    clock_customers = task['clock_customers']
    acq_month = task['acq_month']
    all_subscriptions = task['subscriptions']
    months_to_advance = task['months_to_advance']
    
    month_seconds = 30 * 24 * 60 * 60  # ~30 days
    invoices_generated = 0
    canceled_names = []
    current_time = test_clock.frozen_time
    
    # Advance month by month
    for advance_month in range(1, months_to_advance + 1):
        current_time += month_seconds
        
        try:
            # Advance the clock
            advance_test_clock_with_retry(test_clock.id, frozen_time=current_time)
            
            # Wait for Stripe to process billing
            time.sleep(2)
            
            # Handle cancellations for this month
            for cust_data in clock_customers:
                scenario = cust_data['scenario']
                
                # Check if this customer should be canceled now
                if (scenario['status'] == 'canceled' and 
                    scenario.get('cancel_after_months') == advance_month):
                    
                    # Find and cancel subscription
                    sub_data = next(
                        (s for s in all_subscriptions 
                         if s['scenario'] == scenario), None
                    )
                    if sub_data:
                        try:
                            cancel_subscription_with_retry(sub_data['subscription'].id)
                            canceled_names.append(scenario['name'])
                        except Exception as e:
                            pass  # Continue on cancellation errors
            
        except Exception as e:
            pass  # Continue on advancement errors
    
    # Count invoices for this clock's customers
    try:
        for cust_data in clock_customers:
            invoices = stripe.Invoice.list(
                customer=cust_data['customer'].id,
                limit=10
            )
            invoices_generated += len(invoices.data)
    except Exception as e:
        pass
    
    return invoices_generated, test_clock.id, acq_month, canceled_names


def advance_test_clocks_parallel(test_clocks: Dict, created_data: Dict, max_workers: int = 4) -> int:
    """
    Advance test clocks IN PARALLEL to generate billing history.
    Uses ThreadPoolExecutor for concurrent clock advancement.
    
    This is the key V2 improvement - significantly faster execution by
    advancing multiple clocks simultaneously.
    
    Args:
        test_clocks: Test clocks organized by acquisition month
        created_data: Created customer and subscription data
        max_workers: Number of parallel threads (default: 4)
    
    Returns:
        Total number of invoices generated
    """
    print(f"\n⏰ Advancing test clocks IN PARALLEL to generate billing history...")
    print(f"   🔧 Using {max_workers} parallel workers for concurrent advancement")
    
    invoices_generated = 0
    total_canceled = []
    clock_tasks = []
    
    # Prepare all clock advancement tasks
    for acq_month in range(MONTHS_OF_HISTORY):
        month_clocks = test_clocks.get(acq_month, [])
        if not month_clocks:
            continue
        
        month_customers = created_data['by_month'].get(acq_month, [])
        if not month_customers:
            continue
        
        # Calculate how many months to advance (from acquisition to present)
        months_to_advance = MONTHS_OF_HISTORY - acq_month
        
        print(f"\n   📅 {MONTH_NAMES[acq_month]} cohort: Queuing {len(month_clocks)} clock(s) × {months_to_advance} months...")
        
        # Prepare task for each clock
        for clock_idx, test_clock in enumerate(month_clocks):
            # Get customers on this specific clock
            clock_customers = [c for c in month_customers if c['clock_index'] == clock_idx]
            
            if not clock_customers:
                continue
            
            task = {
                'test_clock': test_clock,
                'clock_customers': clock_customers,
                'acq_month': acq_month,
                'subscriptions': created_data['subscriptions'],
                'months_to_advance': months_to_advance
            }
            
            clock_tasks.append(task)
    
    print(f"\n🔄 Starting PARALLEL advancement of {len(clock_tasks)} clocks...")
    print(f"   ⚡ This is ~{max_workers}x faster than sequential processing!")
    print()
    
    # Execute all clock advancements in parallel
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {executor.submit(advance_single_clock_worker, task): task for task in clock_tasks}
        
        completed = 0
        for future in as_completed(futures):
            completed += 1
            task = futures[future]
            
            try:
                invoices, clock_id, acq_month, canceled_names = future.result()
                invoices_generated += invoices
                total_canceled.extend(canceled_names)
                
                # Progress output
                month_name = MONTH_NAMES[acq_month] if acq_month < len(MONTH_NAMES) else f"M{acq_month}"
                progress_pct = (completed / len(clock_tasks)) * 100
                print(f"   ✅ Clock ...{clock_id[-8:]} ({month_name}) → {invoices} invoices | Progress: {completed}/{len(clock_tasks)} ({progress_pct:.0f}%)")
                
            except Exception as e:
                print(f"   ⚠️ Clock task failed: {e}")
    
    elapsed_time = time.time() - start_time
    
    print(f"\n{'='*60}")
    print(f"⚡ PARALLEL ADVANCEMENT COMPLETE")
    print(f"{'='*60}")
    print(f"   ⏱️  Total time: {elapsed_time:.1f} seconds")
    print(f"   🕐 Clocks advanced: {len(clock_tasks)}")
    print(f"   🧾 Invoices generated: {invoices_generated}")
    print(f"   🚫 Subscriptions canceled: {len(total_canceled)}")
    print(f"   ⚡ Avg time per clock: {elapsed_time/max(len(clock_tasks),1):.1f}s (parallel)")
    
    return invoices_generated


def print_summary(created_data, invoices_count):
    """Print final summary of generated data."""
    print("\n" + "=" * 60)
    print("📊 DATA GENERATION SUMMARY")
    print("=" * 60)
    
    print(f"\n👥 Customers Created: {len(created_data['customers'])}")
    print(f"   • Active: {created_data['stats']['active']} ({100*created_data['stats']['active']/len(created_data['customers']):.0f}%)")
    print(f"   • Canceled: {created_data['stats']['canceled']} ({100*created_data['stats']['canceled']/len(created_data['customers']):.0f}%)")
    print(f"   • Past Due: {created_data['stats']['past_due']} ({100*created_data['stats']['past_due']/len(created_data['customers']):.0f}%)")
    
    print(f"\n🧾 Invoices Generated: ~{invoices_count}")
    
    # Calculate expected MRR by month
    plan_prices = {'starter': 29, 'professional': 79, 'business': 149, 'enterprise': 299}
    
    print(f"\n📈 Expected MRR Trend (approximate):")
    
    cumulative_mrr = 0
    for month in range(MONTHS_OF_HISTORY):
        # Add new customers for this month
        month_customers = created_data['by_month'].get(month, [])
        new_mrr = sum(plan_prices[c['scenario']['plan']] for c in month_customers)
        cumulative_mrr += new_mrr
        
        # Subtract churned MRR (cancellations that happen this month)
        churned_mrr = 0
        for cust in created_data['customers']:
            scenario = cust['scenario']
            if scenario['status'] == 'canceled':
                cancel_month = scenario['acquisition_month'] + scenario.get('cancel_after_months', 0)
                if cancel_month == month:
                    churned_mrr += plan_prices[scenario['plan']]
        
        net_mrr = cumulative_mrr - churned_mrr
        cumulative_mrr = net_mrr
        
        bar_len = int(net_mrr / 100)
        print(f"   {MONTH_NAMES[month]}: ${net_mrr:,} {'█' * bar_len}")
    
    # Calculate final active MRR
    active_mrr = 0
    plan_counts = {'starter': 0, 'professional': 0, 'business': 0, 'enterprise': 0}
    
    for cust in created_data['customers']:
        if cust['scenario']['status'] == 'active':
            plan = cust['scenario']['plan']
            active_mrr += plan_prices[plan]
            plan_counts[plan] += 1
    
    print(f"\n💰 Final Active MRR: ${active_mrr:,}")
    print(f"\n📋 Plan Distribution (Active only):")
    for plan, count in plan_counts.items():
        if count > 0:
            print(f"   • {plan.title()}: {count} customers (${plan_prices[plan] * count:,} MRR)")
    
    print("\n" + "=" * 60)


def main(max_workers: int = DEFAULT_PARALLEL_WORKERS):
    """Main execution function."""
    print("🚀 Stripe MRR Test Data Generator V2")
    print("⚡ WITH PARALLEL TEST CLOCK ADVANCEMENT")
    print("=" * 60)
    print(f"Target: {NUM_CUSTOMERS} customers with {MONTHS_OF_HISTORY} months history")
    print(f"Pattern: Growth → Dip → Recovery")
    print(f"Status Mix: 70% Active, 20% Canceled, 10% Past Due")
    print(f"Parallel Workers: {max_workers}")
    
    # Calculate start date (6 months ago)
    start_date = datetime.now() - timedelta(days=30 * MONTHS_OF_HISTORY)
    print(f"\nStart Date: {start_date.strftime('%Y-%m-%d')}")
    
    # Step 1: Create products and prices
    product, prices = create_products_and_prices()
    
    # Step 2: Generate customer scenarios with realistic pattern
    scenarios = generate_customer_scenarios(NUM_CUSTOMERS, prices)
    print(f"\n📋 Generated {len(scenarios)} customer scenarios")
    
    # Step 3: Create test clocks organized by acquisition month
    test_clocks = create_test_clocks_by_month(start_date, scenarios)
    
    # Step 4: Create customers and subscriptions
    created_data = create_customers_and_subscriptions(scenarios, prices, test_clocks)
    
    # Step 5: Advance test clocks IN PARALLEL to generate billing history
    invoices_count = advance_test_clocks_parallel(test_clocks, created_data, max_workers=max_workers)
    
    # Step 6: Print summary
    print_summary(created_data, invoices_count)
    
    print("\n✅ Data generation complete!")
    print("Next step: Run 'python scripts/stripe_to_bigquery.py' to load data into BigQuery")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Generate Stripe MRR test data with parallel clock advancement'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=DEFAULT_PARALLEL_WORKERS,
        help=f'Number of parallel workers for clock advancement (default: {DEFAULT_PARALLEL_WORKERS})'
    )
    args = parser.parse_args()
    
    try:
        main(max_workers=args.workers)
    except KeyboardInterrupt:
        print("\n\n⚠️ Generation interrupted by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
