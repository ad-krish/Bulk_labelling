import asyncio
import aiohttp
import csv
import hashlib
from dotenv import load_dotenv
import os

# --- CONFIG ---
load_dotenv("config.env")
HOST = os.getenv("HOST")

API_URL_TEMPLATE = f"https://{HOST}/catalog-server/api/rules/data-quality/{{policy_id}}?version=1"
HEADERS = {
    "accessKey": os.getenv("ACCESS_KEY"),
    "secretKey": os.getenv("SECRET_KEY"),
    "Accept": "application/json",
    "Content-Type": "application/json"
}

INPUT_CSV = "Policy_id_mapping.csv"
OUTPUT_CSV = "Policy_Rule_Details.csv"


def compute_hash(expression: str) -> str:
    """Compute MD5 hash of the rule expression."""
    if not expression:
        return "empty"
    return hashlib.md5(expression.encode()).hexdigest()[:8]


def get_column_name(item: dict) -> str:
    """
    Determine the Column_Name based on measurementType:
    - CUSTOM: "CUSTOM-{hash of ruleExpression}"
    - SQL_METRIC: "SQL_METRIC-{hash of ruleExpression}"
    - UDF_PREDICATE: "UDF_PREDICATE-{value.udfId}"
    - SIZE_CHECK: "SIZE_CHECK"
    - Otherwise: use columnName from the item
    """
    measurement_type = item.get("measurementType", "")
    column_name = item.get("columnName", "")
    
    if measurement_type == "CUSTOM":
        rule_expression = item.get("ruleExpression", "")
        expr_hash = compute_hash(rule_expression)
        return f"CUSTOM-{expr_hash}"
    
    elif measurement_type == "SQL_METRIC":
        rule_expression = item.get("ruleExpression", "")
        expr_hash = compute_hash(rule_expression)
        return f"SQL_METRIC-{expr_hash}"
    
    elif measurement_type == "UDF_PREDICATE":
        value = item.get("value", {})
        udf_id = value.get("udfId", "unknown") if value else "unknown"
        return f"UDF_PREDICATE-{udf_id}"
    
    elif measurement_type == "SIZE_CHECK":
        return "SIZE_CHECK"
    
    else:
        return column_name


async def fetch_policy_details(session, policy_id):
    """Fetch detailed information for a single policy."""
    url = API_URL_TEMPLATE.format(policy_id=policy_id)
    try:
        async with session.get(url, headers=HEADERS) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(f"⚠️  Failed to fetch policy {policy_id}: HTTP {response.status}")
                return None
    except Exception as e:
        print(f"❌ Error fetching policy {policy_id}: {e}")
        return None


async def process_policies(policy_ids: list) -> list:
    """Process all policies and extract rule details."""
    results = []
    
    async with aiohttp.ClientSession() as session:
        # Create tasks for all policy fetches
        tasks = [fetch_policy_details(session, pid) for pid in policy_ids]
        
        print(f"📡 Fetching details for {len(policy_ids)} policies...")
        responses = await asyncio.gather(*tasks)
        
        for data in responses:
            if data is None:
                continue
            
            rule = data.get("rule", {})
            details = data.get("details", {})
            
            policy_id = rule.get("id")
            policy_name = rule.get("name")
            
            items = details.get("items", [])
            
            if not items:
                # No items, still record the policy with empty rule details
                results.append({
                    "Policy_ID": policy_id,
                    "Policy_Name": policy_name,
                    "Rule_ID": "",
                    "Rule_Type": "",
                    "Column_Name": ""
                })
            else:
                for item in items:
                    rule_id = item.get("id")
                    rule_type = item.get("measurementType", "")
                    column_name = get_column_name(item)
                    
                    results.append({
                        "Policy_ID": policy_id,
                        "Policy_Name": policy_name,
                        "Rule_ID": rule_id,
                        "Rule_Type": rule_type,
                        "Column_Name": column_name
                    })
    
    return results


def read_policy_ids(csv_path: str) -> list:
    """Read policy IDs from the input CSV file, filtering for DATA_QUALITY policies only."""
    policy_ids = []
    total_count = 0
    try:
        with open(csv_path, mode="r", newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                total_count += 1
                policy_id = row.get("Policy_ID")
                policy_type = row.get("Policy_Type", "")
                # Only include DATA_QUALITY policies
                if policy_id and policy_type == "DATA_QUALITY":
                    policy_ids.append(policy_id)
        print(f"📂 Loaded {len(policy_ids)} DATA_QUALITY policies from {csv_path} (out of {total_count} total)")
    except FileNotFoundError:
        print(f"❌ Error: {csv_path} not found. Please run Fetch_Policy_ID.py first.")
    return policy_ids


def write_results(results: list, csv_path: str):
    """Write results to output CSV."""
    with open(csv_path, mode="w", newline="") as file:
        fieldnames = ["Policy_ID", "Policy_Name", "Rule_ID", "Rule_Type", "Column_Name"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"✅ Exported {len(results)} rule details to {csv_path}")


async def main():
    print("=" * 50)
    print("Policy Details Fetcher")
    print("=" * 50)
    
    # Read policy IDs from input CSV
    policy_ids = read_policy_ids(INPUT_CSV)
    
    if not policy_ids:
        print("No policy IDs to process. Exiting.")
        return
    
    # Fetch and process all policies
    results = await process_policies(policy_ids)
    
    # Write results to CSV
    write_results(results, OUTPUT_CSV)
    
    print("-" * 50)
    print(f"📊 Summary:")
    print(f"   Policies processed: {len(policy_ids)}")
    print(f"   Total rules extracted: {len(results)}")


# Run the script
if __name__ == "__main__":
    asyncio.run(main())

