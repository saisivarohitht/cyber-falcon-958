#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stripe Test Data Cleanup Script
================================
Deletes all test data from Stripe to start fresh.
"""

import stripe
import os
from dotenv import load_dotenv
import time

load_dotenv()
stripe.api_key = os.getenv('STRIPE_TEST_SECRET_KEY')

def cleanup_all_stripe_data():
    """Delete all test data from Stripe."""
    print("Cleaning up Stripe test data...")
    print("=" * 50)
    
    # 1. Delete all subscriptions
    print("\n📋 Canceling subscriptions...")
    subs = stripe.Subscription.list(limit=100, status='all')
    for sub in subs.auto_paging_iter():
        try:
            if sub.status not in ['canceled', 'incomplete_expired']:
                stripe.Subscription.cancel(sub.id)
                print(f"   ✅ Canceled: {sub.id}")
            time.sleep(0.2)
        except Exception as e:
            print(f"   ⚠️ Could not cancel {sub.id}: {e}")
    
    # 2. Delete all customers (this also deletes their subscriptions)
    print("\n👥 Deleting customers...")
    customers = stripe.Customer.list(limit=100)
    deleted_count = 0
    for customer in customers.auto_paging_iter():
        try:
            stripe.Customer.delete(customer.id)
            deleted_count += 1
            if deleted_count % 10 == 0:
                print(f"   ✅ Deleted {deleted_count} customers...")
            time.sleep(0.2)
        except Exception as e:
            print(f"   ⚠️ Could not delete {customer.id}: {e}")
    print(f"   ✅ Deleted {deleted_count} total customers")
    
    # 3. Delete all test clocks
    print("\n🕐 Deleting test clocks...")
    try:
        test_clocks = stripe.test_helpers.TestClock.list(limit=100)
        for tc in test_clocks.data:
            try:
                stripe.test_helpers.TestClock.delete(tc.id)
                print(f"   ✅ Deleted: {tc.id}")
                time.sleep(0.2)
            except Exception as e:
                print(f"   ⚠️ Could not delete {tc.id}: {e}")
    except Exception as e:
        print(f"   ⚠️ Could not list test clocks: {e}")
    
    # 4. Archive products (can't delete, but can archive)
    print("\n📦 Archiving products...")
    products = stripe.Product.list(limit=100, active=True)
    for product in products.auto_paging_iter():
        try:
            stripe.Product.modify(product.id, active=False)
            print(f"   ✅ Archived: {product.name}")
            time.sleep(0.2)
        except Exception as e:
            print(f"   ⚠️ Could not archive {product.id}: {e}")
    
    print("\n✅ Cleanup complete!")
    print("=" * 50)

if __name__ == "__main__":
    confirm = input("⚠️  This will DELETE ALL Stripe test data. Are you sure? (yes/no): ")
    if confirm.lower() == 'yes':
        cleanup_all_stripe_data()
    else:
        print("Cleanup cancelled.")
