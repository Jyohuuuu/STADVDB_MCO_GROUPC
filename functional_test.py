# functional_test.py
import pandas as pd
import time
from sqlalchemy import create_engine, text
from reports import (
    gdp_population_correlation_report,
    cost_of_living_vs_purchasing_power_report,
    climate_quality_vs_economic_development_report,
    traffic_commute_category_report,
)

# --- Database connection configuration ---
dw_username = "root"
dw_password = "admin"  #adjust this
dw_host = "localhost"
dw_database = "country_data_warehouse"

dw_engine = create_engine(f"mysql+pymysql://{dw_username}:{dw_password}@{dw_host}/{dw_database}")

# Store test results
test_results = []

# ANSI colors for pretty output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"


def log_result(name, passed, elapsed_ms, error=None):
    test_results.append({
        "Test": name,
        "Result": "PASS" if passed else "FAIL",
        "Error": error or "",
        "Time (ms)": elapsed_ms
    })


# --- Utility: check database connection ---
def verify_connection():
    print("🔌 Testing database connection...")
    try:
        with dw_engine.connect() as conn:
            db_name = conn.execute(text("SELECT DATABASE();")).scalar()
            print(f" Connected to database: {db_name}\n")
            return True
    except Exception as e:
        print(f" Connection failed: {e}")
        return False


# --- Individual Tests ---
def timed_test(func):
    """Decorator to measure and handle each test execution."""
    def wrapper(test_name):
        print(f"=== Testing {test_name} ===")
        start = time.time()
        try:
            func()
            elapsed = (time.time() - start) * 1000
            print(f"✓ {test_name} passed in {elapsed:.2f} ms\n")
            log_result(test_name, True, elapsed)
        except AssertionError as e:
            elapsed = (time.time() - start) * 1000
            print(f"❌ {test_name} failed ({elapsed:.2f} ms): {e}\n")
            log_result(test_name, False, elapsed, str(e))
        except Exception as e:
            elapsed = (time.time() - start) * 1000
            print(f"❌ {test_name} error ({elapsed:.2f} ms): {e}\n")
            log_result(test_name, False, elapsed, str(e))
    return wrapper


@timed_test
def test_report_functions_availability():
    reports = {
        "GDP Population": gdp_population_correlation_report,
        "Cost of Living": cost_of_living_vs_purchasing_power_report,
        "Climate Development": climate_quality_vs_economic_development_report,
        "Traffic Commute": traffic_commute_category_report,
    }

    for rep_name, func in reports.items():
        df = func()
        assert not df.empty, f"{rep_name} returned an empty DataFrame"
        print(f"  ✓ {rep_name}: {len(df)} records")

    print("  ✓ Database connectivity confirmed through report functions")


@timed_test
def test_gdp_population_correlation():
    df = gdp_population_correlation_report()
    assert not df.empty, "Report returned empty DataFrame"
    assert {'country_name', 'population', 'gdp_usd'}.issubset(df.columns), "Missing columns"
    assert df['population'].min() > 0, "Population should be positive"
    assert df['gdp_usd'].min() > 0, "GDP should be positive"


@timed_test
def test_cost_of_living_vs_purchasing_power():
    df = cost_of_living_vs_purchasing_power_report()
    assert not df.empty
    assert {'avg_cost_of_living', 'avg_purchasing_power', 'avg_inflation_pressure_ratio'}.issubset(df.columns)


@timed_test
def test_climate_quality_vs_economic_development():
    df = climate_quality_vs_economic_development_report()
    assert not df.empty
    assert {'climate_quality_2025', 'total_gdp_usd', 'development_efficiency_ratio'}.issubset(df.columns)
    assert df['year_value'].between(2020, 2025).all()


@timed_test
def test_traffic_commute_category():
    df = traffic_commute_category_report()
    assert not df.empty
    assert {'traffic_commute_category', 'avg_gdp_per_capita', 'total_population'}.issubset(df.columns)


# --- Test Runner ---
def run_all_tests():
    print("🚀 Starting Functional Test Suite")
    print("=" * 60)

    if not verify_connection():
        print("❌ Cannot continue without database connection.")
        return

    # Each test runs independently, time is tracked
    test_report_functions_availability("Report Functions Availability")
    test_gdp_population_correlation("GDP vs Population Correlation")
    test_cost_of_living_vs_purchasing_power("Cost of Living vs Purchasing Power")
    test_climate_quality_vs_economic_development("Climate Quality vs Economic Development")
    test_traffic_commute_category("Traffic Commute Category Report")

    # --- Summary Table ---
    print("=" * 60)
    print(" TEST SUMMARY:")
    for res in test_results:
        color = GREEN if res["Result"] == "PASS" else RED
        print(f"{color}{res['Result']:<6}{RESET} {res['Test']:<40} {res['Time (ms)']:.2f} ms")
        if res["Error"]:
            print(f"   ↳ {YELLOW}{res['Error']}{RESET}")
    print("=" * 60)

    passed = sum(1 for r in test_results if r["Result"] == "PASS")
    failed = len(test_results) - passed
    print(f" Passed: {passed} |  Failed: {failed}")
    print(" Functional testing complete!\n")


if __name__ == "__main__":
    run_all_tests()

