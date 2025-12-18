import asyncio
import aiohttp
import csv
from dotenv import load_dotenv
import os

# --- CONFIG ---
load_dotenv("config.env")
HOST = os.getenv("HOST")

API_URL = f"http://{HOST}/catalog-server/api/rules"
HEADERS = {
    "accessKey": os.getenv("ACCESS_KEY"),
    "secretKey": os.getenv("SECRET_KEY"),
    "Accept": "application/json",
    "Content-Type": "application/json"
}

def build_params():
    """Build query parameters from config.env, excluding empty optional fields."""
    params = {
        "page": 0,
        "size": 100,  # Adjust to larger size to fetch more rules per request
        "withLatestExecution": "true",
        "sortBy": "startedAt:DESC",
    }
    
    # Required parameters from config
    rule_status = os.getenv("RULE_STATUS")
    rule_type = os.getenv("RULE_TYPE")
    
    if rule_status:
        params["ruleStatus"] = rule_status
    if rule_type:
        params["ruleType"] = rule_type
    
    # Optional parameters - only add if provided
    tag = os.getenv("TAG")
    assembly_ids = os.getenv("ASSEMBLY_IDS")
    
    if tag and tag.strip():
        params["tag"] = tag.strip()
    if assembly_ids and assembly_ids.strip():
        params["assemblyIds"] = assembly_ids.strip()
    
    return params

# Build parameters from config
PARAMS = build_params()

async def fetch_rules(session, page):
    params = PARAMS.copy()
    params["page"] = page
    async with session.get(API_URL, params=params, headers=HEADERS) as response:
        response.raise_for_status()
        return await response.json()

async def main():
    rule_data = []
    
    # Display active parameters
    print("📋 Active Query Parameters:")
    for key, value in PARAMS.items():
        if key not in ["page", "size"]:
            print(f"   {key}: {value}")
    print("-" * 40)

    async with aiohttp.ClientSession() as session:
        page = 0
        total_pages = None
        
        while True:
            if total_pages:
                print(f"Fetching page {page + 1} of {total_pages}")
            else:
                print(f"Fetching page {page + 1}...")
            
            data = await fetch_rules(session, page)
            
            # Extract pagination info if available
            if total_pages is None:
                total_count = data.get("totalCount", 0)
                page_size = PARAMS.get("size", 100)
                total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
                print(f"📊 Total rules found: {total_count} (across {total_pages} page(s))")

            rules = data.get("rules", [])
            if not rules:
                break

            for item in rules:
                rule = item.get("rule", {})
                rule_id = rule.get("id")
                rule_name = rule.get("name")
                rule_type = rule.get("type", "")
                if rule_id and rule_name:
                    rule_data.append((rule_name, rule_id, rule_type))

            page += 1
            
            # Stop if we've fetched all pages
            if page >= total_pages:
                break

    # Write to CSV
    with open("Policy_id_mapping.csv", mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Policy_Name", "Policy_ID", "Policy_Type"])
        writer.writerows(rule_data)

    print(f"✅ Exported {len(rule_data)} rules to Policy_id_mapping.csv")

# Run the script
if __name__ == "__main__":
    asyncio.run(main())
