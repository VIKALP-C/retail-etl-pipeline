# test_run.py
# Run this to test each phase one at a time
# Command: python test_run.py

from loguru import logger

print("Testing imports...")

# Test 1: imports
try:
    from etl.extract import extract_all
    from etl.transform import transform_all
    print("✅ Imports OK")
except Exception as e:
    print(f"❌ Import failed: {e}")
    exit()

# Test 2: extract
print("\nTesting EXTRACT...")
try:
    raw = extract_all()
    for name, df in raw.items():
        print(f"  ✅ {name}: {len(df):,} rows")
except Exception as e:
    print(f"  ❌ Extract failed: {e}")
    exit()

# Test 3: transform (this is the slow one — takes 2-5 mins)
print("\nTesting TRANSFORM (this takes 2-5 minutes, please wait)...")
try:
    from etl.transform import clean_sales, clean_features, clean_stores, merge_all_sources, engineer_features, scale_to_million

    print("  Cleaning sales...")
    sales = clean_sales(raw['sales'])
    print(f"  ✅ clean_sales: {len(sales):,} rows")

    print("  Cleaning features...")
    features = clean_features(raw['features'])
    print(f"  ✅ clean_features: {len(features):,} rows")

    print("  Cleaning stores...")
    stores = clean_stores(raw['stores'])
    print(f"  ✅ clean_stores: {len(stores):,} rows")

    print("  Merging sources...")
    merged = merge_all_sources(sales, features, stores, raw['cpi'], raw['unemployment'])
    print(f"  ✅ merge: {len(merged):,} rows, {len(merged.columns)} columns")

    print("  Engineering features...")
    featured = engineer_features(merged)
    print(f"  ✅ engineer_features: {len(featured):,} rows, {len(featured.columns)} columns")

    print("  Scaling to 1M+ rows (slowest step)...")
    final = scale_to_million(featured)
    print(f"  ✅ scale_to_million: {len(final):,} rows")

except Exception as e:
    print(f"  ❌ Transform failed at this step: {e}")
    import traceback
    traceback.print_exc()
    exit()

print("\n✅ ALL STEPS PASSED")
print(f"Final shape: {final.shape}")
print(f"Columns: {list(final.columns)}")