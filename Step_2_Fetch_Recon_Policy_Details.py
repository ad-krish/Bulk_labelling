import asyncio
import aiohttp
import csv
from dotenv import load_dotenv
import os

# --- CONFIG ---
load_dotenv("config.env")
HOST = os.getenv("HOST")

API_URL_TEMPLATE = f"https://{HOST}/catalog-server/api/rules/reconciliation/{{policy_id}}?version=1"
HEADERS = {
    "accessKey": os.getenv("ACCESS_KEY"),
    "secretKey": os.getenv("SECRET_KEY"),
    "Accept": "application/json",
    "Content-Type": "application/json"
}

INPUT_CSV = "Policy_id_mapping.csv"
OUTPUT_CSV = "Recon_Policy_Rule_Details.csv"


async def fetch_policy_details(session, policy_id):
    """Fetch detailed information for a single reconciliation policy."""
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
    """Process all reconciliation policies and extract rule details."""
    results = []
    
    async with aiohttp.ClientSession() as session:
        # Create tasks for all policy fetches
        tasks = [fetch_policy_details(session, pid) for pid in policy_ids]
        
        print(f"📡 Fetching details for {len(policy_ids)} reconciliation policies...")
        responses = await asyncio.gather(*tasks)
        
        for data in responses:
            if data is None:
                continue
            
            rule = data.get("rule", {})
            details = data.get("details", {})
            
            policy_id = rule.get("id")
            policy_name = rule.get("name")
            
            # Get Recon_Type from details.items
            items = details.get("items", [])
            recon_type = items[0].get("measurementType", "") if items else ""
            
            # Get column mappings
            column_mappings = details.get("columnMappings", [])
            
            if not column_mappings:
                # No column mappings, still record the policy with empty details
                results.append({
                    "Policy_ID": policy_id,
                    "Policy_Name": policy_name,
                    "Rule_ID": "",
                    "Recon_Type": recon_type,
                    "Left_Column_Name": "",
                    "Right_Column_Name": ""
                })
            else:
                for mapping in column_mappings:
                    rule_id = mapping.get("id")
                    left_column = mapping.get("leftColumnName", "")
                    right_column = mapping.get("rightColumnName", "")
                    
                    results.append({
                        "Policy_ID": policy_id,
                        "Policy_Name": policy_name,
                        "Rule_ID": rule_id,
                        "Recon_Type": recon_type,
                        "Left_Column_Name": left_column,
                        "Right_Column_Name": right_column
                    })
    
    return results


def read_policy_ids(csv_path: str) -> list:
    """Read policy IDs from the input CSV file, filtering for EQUALITY policies only."""
    policy_ids = []
    total_count = 0
    try:
        with open(csv_path, mode="r", newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                total_count += 1
                policy_id = row.get("Policy_ID")
                policy_type = row.get("Policy_Type", "")
                # Only include EQUALITY (Reconciliation) policies
                if policy_id and policy_type == "EQUALITY":
                    policy_ids.append(policy_id)
        print(f"📂 Loaded {len(policy_ids)} EQUALITY policies from {csv_path} (out of {total_count} total)")
    except FileNotFoundError:
        print(f"❌ Error: {csv_path} not found. Please run Fetch_Policy_ID.py first.")
    return policy_ids


def write_results(results: list, csv_path: str):
    """Write results to output CSV."""
    with open(csv_path, mode="w", newline="") as file:
        fieldnames = ["Policy_ID", "Policy_Name", "Rule_ID", "Recon_Type", "Left_Column_Name", "Right_Column_Name"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"✅ Exported {len(results)} rule details to {csv_path}")


async def main():
    print("=" * 50)
    print("Reconciliation Policy Details Fetcher")
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
    print(f"   Total column mappings extracted: {len(results)}")


# Run the script
if __name__ == "__main__":
    asyncio.run(main())

