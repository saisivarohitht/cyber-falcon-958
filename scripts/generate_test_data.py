#!/usr/bin/env python3
"""
Stripe MRR Test Data Generator v2
=================================
Generates 100 customers with 6 months of billing history using Stripe Test Clocks.

This version uses proactive rate limiting with generous delays between API calls
to avoid rate limit errors entirely - no retry logic needed.

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
"""

import stripe
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path
import random
import time

# Load environment variables from parent directory
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

# Initialize Stripe
stripe.api_key = os.getenv('STRIPE_TEST_SECRET_KEY')

# Configuration
NUM_CUSTOMERS = 100
MONTHS_OF_HISTORY = 6

# API DELAY SETTINGS - generous delays to prevent rate limits
API_DELAY = 1.0          # Standard delay between API calls (seconds)
BATCH_DELAY = 3.0        # Delay after batch operations
CLOCK_ADVANCE_DELAY = 10.0  # Delay after advancing test clocks (Stripe needs time to process)
CLOCK_READY_WAIT = 15.0  # Wait time for clock to be ready for modifications

# Customer acquisition pattern
CUSTOMER_ACQUISITION_BY_MONTH = {
    0: 8,    # Aug - Launch
    1: 15,   # Sep - Growth  
    2: 25,   # Oct - Peak growth
    3: 12,   # Nov - Slowdown/dip
    4: 18,   # Dec - Recovery
    5: 22    # Jan - Strong recovery
}

# Cancellation schedule: cancel_month -> [acquisition_months]
CANCELLATION_SCHEDULE = {
    1: [0, 0],
    2: [0, 1, 1],
    3: [1, 1, 2, 2, 2],
    4: [2, 2, 3, 3, 3],
    5: [3, 4, 4, 4, 4]
}

# Past due schedule: past_due_month -> [acquisition_months]
PAST_DUE_SCHEDULE = {
    2: [1, 1],
    3: [0, 2, 2],
    4: [2, 3, 3],
    5: [4, 4]
}

# Company name generators
COMPANY_PREFIXES = ['Tech', 'Data', 'Cloud', 'Digital', 'Smart', 'Global', 'Pro', 'Next', 'Fast', 'Prime']
COMPANY_SUFFIXES = ['Solutions', 'Systems', 'Labs', 'Corp', 'Inc', 'LLC', 'Co', 'Group', 'Hub', 'Works']
COMPANY_TYPES = ['Analytics', 'Software', 'Services', 'Consulting', 'Media', 'Ventures', 'Partners', 'Tech', 'Digital', 'AI']


def api_pause(delay=None):
    """Pause between API calls to prevent rate limiting."""
    time.sleep(delay or API_DELAY)


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
    
    product = stripe.Product.create(
        name="CloudSync Platform",
        description="Enterprise-grade cloud synchronization and analytics platform"
    )
    api_pause()
    
    prices = {}
    
    price_configs = [
        ('starter', 2900, 'Starter Plan'),
        ('professional', 7900, 'Professional Plan'),
        ('business', 14900, 'Business Plan'),
        ('enterprise', 29900, 'Enterprise Plan'),
    ]
    
    for plan_key, amount, nickname in price_configs:
        prices[plan_key] = stripe.Price.create(
            product=product.id,
            unit_amount=amount,
            currency='usd',
            recurring={'interval': 'month'},
            nickname=nickname
        )
        api_pause()
        print(f"   ✅ Created {nickname}: ${amount/100}/month")
    
    print(f"✅ Created product: {product.name}")
    return product, prices


def generate_customer_scenarios(prices):
    """Generate customer scenarios with realistic distribution."""
    scenarios = []
    
    plan_weights = {
        'starter': 0.40,
        'professional': 0.30,
        'business': 0.20,
        'enterprise': 0.10
    }
    
    plans = list(plan_weights.keys())
    plan_probs = list(plan_weights.values())
    
    # Build cancellation tracking
    cancel_by_acq = {}
    for cancel_month, acq_months in CANCELLATION_SCHEDULE.items():
        for acq_month in acq_months:
            if acq_month not in cancel_by_acq:
                cancel_by_acq[acq_month] = []
            cancel_by_acq[acq_month].append(cancel_month)
    
    # Build past due tracking
    pd_by_acq = {}
    for pd_month, acq_months in PAST_DUE_SCHEDULE.items():
        for acq_month in acq_months:
            if acq_month not in pd_by_acq:
                pd_by_acq[acq_month] = []
            pd_by_acq[acq_month].append(pd_month)
    
    customer_index = 0
    
    for acq_month, num_in_month in CUSTOMER_ACQUISITION_BY_MONTH.items():
        month_cancels = cancel_by_acq.get(acq_month, []).copy()
        month_past_dues = pd_by_acq.get(acq_month, []).copy()
        
        for i in range(num_in_month):
            company_name = generate_company_name()
            plan = random.choices(plans, weights=plan_probs)[0]
            
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
                'acquisition_month': acq_month,
                'status': status,
                'cancel_after_months': cancel_after_months,
                'past_due_month': past_due_month,
                'customer_index': customer_index
            })
            
            customer_index += 1
    
    # Print summary
    print("\n📊 Customer Acquisition Pattern:")
    month_names = ['Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan']
    for month, count in CUSTOMER_ACQUISITION_BY_MONTH.items():
        month_scenarios = [s for s in scenarios if s['acquisition_month'] == month]
        active = len([s for s in month_scenarios if s['status'] == 'active'])
        canceled = len([s for s in month_scenarios if s['status'] == 'canceled'])
        past_due = len([s for s in month_scenarios if s['status'] == 'past_due'])
        print(f"   {month_names[month]}: {count} customers (Active: {active}, Canceled: {canceled}, Past Due: {past_due})")
    
    return scenarios


def create_test_clocks(start_date, scenarios):
    """Create test clocks organized by acquisition month (3 customers per clock)."""
    print(f"\n🕐 Creating test clocks...")
    
    test_clocks = {}
    month_names = ['Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan']
    
    for month in range(MONTHS_OF_HISTORY):
        month_scenarios = [s for s in scenarios if s['acquisition_month'] == month]
        if not month_scenarios:
            continue
        
        month_start_time = start_date + timedelta(days=30 * month)
        num_clocks = (len(month_scenarios) + 2) // 3
        
        test_clocks[month] = []
        
        for i in range(num_clocks):
            print(f"   Creating clock for {month_names[month]} ({i + 1}/{num_clocks})...", end=" ", flush=True)
            
            tc = stripe.test_helpers.TestClock.create(
                frozen_time=int(month_start_time.timestamp()),
                name=f"Month {month} Clock {i + 1}"
            )
            test_clocks[month].append(tc)
            print("✅")
            api_pause()
        
        print(f"   ✅ {month_names[month]}: {len(test_clocks[month])} clocks for {len(month_scenarios)} customers")
        time.sleep(BATCH_DELAY)
    
    total_clocks = sum(len(clocks) for clocks in test_clocks.values())
    print(f"\n   ✅ Created {total_clocks} total test clocks")
    
    return test_clocks


def create_customers_and_subscriptions(scenarios, prices, test_clocks):
    """Create customers and subscriptions."""
    print(f"\n👥 Creating {len(scenarios)} customers with subscriptions...")
    
    created_data = {
        'customers': [],
        'subscriptions': [],
        'stats': {'active': 0, 'canceled': 0, 'past_due': 0},
        'by_month': {}
    }
    
    month_names = ['Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan']
    
    for month in range(MONTHS_OF_HISTORY):
        month_scenarios = [s for s in scenarios if s['acquisition_month'] == month]
        if not month_scenarios:
            continue
        
        month_clocks = test_clocks.get(month, [])
        if not month_clocks:
            print(f"   ⚠️ No clocks for month {month}, skipping...")
            continue
        
        created_data['by_month'][month] = []
        print(f"\n   📅 Creating {month_names[month]} cohort ({len(month_scenarios)} customers)...")
        
        for i, scenario in enumerate(month_scenarios):
            clock_index = i // 3
            if clock_index >= len(month_clocks):
                print(f"   ⚠️ Not enough clocks for {scenario['name']}, skipping...")
                continue
            
            test_clock = month_clocks[clock_index]
            
            print(f"      Creating {scenario['name'][:30]}...", end=" ", flush=True)
            
            # Create customer
            customer = stripe.Customer.create(
                name=scenario['name'],
                email=scenario['email'],
                description=f"{scenario['plan'].title()} plan - {scenario['status']} - Cohort: {month_names[month]}",
                test_clock=test_clock.id
            )
            api_pause()
            
            # Create and attach payment method (skip for past_due to simulate failed payments)
            if scenario['status'] != 'past_due':
                pm = stripe.PaymentMethod.create(type="card", card={"token": "tok_visa"})
                api_pause()
                
                stripe.PaymentMethod.attach(pm.id, customer=customer.id)
                api_pause()
                
                stripe.Customer.modify(
                    customer.id,
                    invoice_settings={"default_payment_method": pm.id}
                )
                api_pause()
            
            # Create subscription
            sub_params = {
                "customer": customer.id,
                "items": [{"price": prices[scenario['plan']].id}],
                "proration_behavior": "none"
            }
            
            if scenario['status'] == 'past_due':
                sub_params["collection_method"] = "send_invoice"
                sub_params["days_until_due"] = 7
            
            subscription = stripe.Subscription.create(**sub_params)
            api_pause()
            
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
            print("✅")
        
        print(f"      ✅ Created {len(created_data['by_month'][month])} customers for {month_names[month]}")
        time.sleep(BATCH_DELAY)
    
    print(f"\n✅ Created {len(created_data['customers'])} total customers")
    return created_data


def advance_test_clocks(test_clocks, created_data, start_date):
    """Advance test clocks month by month to generate billing history."""
    print(f"\n⏰ Advancing test clocks to generate billing history...")
    
    month_names = ['Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan']
    month_seconds = 30 * 24 * 60 * 60
    invoices_generated = 0
    
    for acq_month in range(MONTHS_OF_HISTORY):
        month_clocks = test_clocks.get(acq_month, [])
        if not month_clocks:
            continue
        
        month_customers = created_data['by_month'].get(acq_month, [])
        if not month_customers:
            continue
        
        months_to_advance = MONTHS_OF_HISTORY - acq_month
        
        print(f"\n📅 {month_names[acq_month]} cohort: Advancing {len(month_clocks)} clocks by {months_to_advance} months...")
        
        for clock_idx, test_clock in enumerate(month_clocks):
            clock_customers = [c for c in month_customers if c['clock_index'] == clock_idx]
            
            if not clock_customers:
                continue
            
            current_time = test_clock.frozen_time
            
            for advance_month in range(1, months_to_advance + 1):
                current_time += month_seconds
                
                print(f"      Advancing clock {clock_idx + 1} to month +{advance_month}...", end=" ", flush=True)
                
                # Advance the clock
                stripe.test_helpers.TestClock.advance(test_clock.id, frozen_time=current_time)
                
                # Wait for Stripe to process billing - test clocks are async
                time.sleep(CLOCK_ADVANCE_DELAY)
                
                # Wait for clock to be ready (poll until status is 'ready')
                clock_ready = False
                for _ in range(10):  # Max 10 attempts
                    try:
                        clock_status = stripe.test_helpers.TestClock.retrieve(test_clock.id)
                        if clock_status.status == 'ready':
                            clock_ready = True
                            break
                    except Exception:
                        pass
                    time.sleep(2)
                
                if not clock_ready:
                    print("⏳ (waiting for clock)...", end=" ", flush=True)
                    time.sleep(CLOCK_READY_WAIT)
                
                # Handle cancellations - only after clock is ready
                for cust_data in clock_customers:
                    scenario = cust_data['scenario']
                    
                    if (scenario['status'] == 'canceled' and 
                        scenario.get('cancel_after_months') == advance_month):
                        
                        sub_data = next(
                            (s for s in created_data['subscriptions'] 
                             if s['scenario'] == scenario), None
                        )
                        if sub_data:
                            # Retry cancellation with backoff
                            for attempt in range(3):
                                try:
                                    stripe.Subscription.cancel(sub_data['subscription'].id)
                                    print(f"\n      🚫 Canceled: {scenario['name']}", end=" ")
                                    api_pause()
                                    break
                                except stripe.error.InvalidRequestError as e:
                                    if 'advancement underway' in str(e).lower():
                                        print(f"\n      ⏳ Clock busy, waiting...", end=" ", flush=True)
                                        time.sleep(5 * (attempt + 1))
                                    else:
                                        raise
                
                print("✅")
            
            # Count invoices
            for cust_data in clock_customers:
                invoices = stripe.Invoice.list(customer=cust_data['customer'].id, limit=10)
                invoices_generated += len(invoices.data)
                api_pause(0.5)
        
        print(f"   ✅ {month_names[acq_month]} cohort advanced to present")
        time.sleep(BATCH_DELAY)
    
    print(f"\n✅ Generated approximately {invoices_generated} invoices")
    return invoices_generated


def print_summary(created_data, invoices_count):
    """Print final summary of generated data."""
    print("\n" + "=" * 60)
    print("📊 DATA GENERATION SUMMARY")
    print("=" * 60)
    
    total = len(created_data['customers'])
    print(f"\n👥 Customers Created: {total}")
    print(f"   • Active: {created_data['stats']['active']} ({100*created_data['stats']['active']/total:.0f}%)")
    print(f"   • Canceled: {created_data['stats']['canceled']} ({100*created_data['stats']['canceled']/total:.0f}%)")
    print(f"   • Past Due: {created_data['stats']['past_due']} ({100*created_data['stats']['past_due']/total:.0f}%)")
    
    print(f"\n🧾 Invoices Generated: ~{invoices_count}")
    
    plan_prices = {'starter': 29, 'professional': 79, 'business': 149, 'enterprise': 299}
    month_names = ['Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan']
    
    print(f"\n📈 Expected MRR Trend:")
    
    cumulative_mrr = 0
    for month in range(MONTHS_OF_HISTORY):
        month_customers = created_data['by_month'].get(month, [])
        new_mrr = sum(plan_prices[c['scenario']['plan']] for c in month_customers)
        cumulative_mrr += new_mrr
        
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
        print(f"   {month_names[month]}: ${net_mrr:,} {'█' * bar_len}")
    
    # Final active MRR
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


def main():
    """Main execution function."""
    print("🚀 Stripe MRR Test Data Generator v2")
    print("=" * 60)
    print(f"Target: {NUM_CUSTOMERS} customers with {MONTHS_OF_HISTORY} months history")
    print(f"Pattern: Growth → Dip → Recovery")
    print(f"Status Mix: 70% Active, 20% Canceled, 10% Past Due")
    print(f"\n⏱️  Using proactive rate limiting (no retries needed)")
    print(f"   • API delay: {API_DELAY}s")
    print(f"   • Batch delay: {BATCH_DELAY}s")
    print(f"   • Clock advance delay: {CLOCK_ADVANCE_DELAY}s")
    
    start_date = datetime.now() - timedelta(days=30 * MONTHS_OF_HISTORY)
    print(f"\nStart Date: {start_date.strftime('%Y-%m-%d')}")
    
    # Estimate time
    estimated_minutes = (NUM_CUSTOMERS * 5 * API_DELAY + 
                        MONTHS_OF_HISTORY * 3 * BATCH_DELAY +
                        sum(MONTHS_OF_HISTORY - m for m in range(MONTHS_OF_HISTORY)) * CLOCK_ADVANCE_DELAY) / 60
    print(f"⏳ Estimated time: ~{estimated_minutes:.0f} minutes")
    
    # Step 1: Create products and prices
    product, prices = create_products_and_prices()
    
    # Step 2: Generate customer scenarios
    scenarios = generate_customer_scenarios(prices)
    print(f"\n📋 Generated {len(scenarios)} customer scenarios")
    
    # Step 3: Create test clocks
    test_clocks = create_test_clocks(start_date, scenarios)
    
    # Step 4: Create customers and subscriptions
    created_data = create_customers_and_subscriptions(scenarios, prices, test_clocks)
    
    # Step 5: Advance test clocks
    invoices_count = advance_test_clocks(test_clocks, created_data, start_date)
    
    # Step 6: Print summary
    print_summary(created_data, invoices_count)
    
    print("\n✅ Data generation complete!")
    print("Next step: Run 'python stripe_to_bigquery.py' to load data into BigQuery")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ Generation interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
