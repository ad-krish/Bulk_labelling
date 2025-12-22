import asyncio
import aiohttp
import csv
from dotenv import load_dotenv
import os
from collections import defaultdict

# --- CONFIG ---
load_dotenv("config.env")
HOST = os.getenv("HOST")

# API URLs
RULES_LIST_API = f"http://{HOST}/catalog-server/api/rules"
RECON_POLICY_API = f"https://{HOST}/catalog-server/api/rules/reconciliation/{{policy_id}}"
RECON_POLICY_VERSION_API = f"https://{HOST}/catalog-server/api/rules/reconciliation/{{policy_id}}?version={{version}}"

HEADERS = {
    "accessKey": os.getenv("ACCESS_KEY"),
    "secretKey": os.getenv("SECRET_KEY"),
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# File paths
RECON_POLICY_DETAILS_CSV = "Recon_Policy_Rule_Details.csv"


def build_params():
    """Build query parameters from config.env."""
    params = {
        "page": 0,
        "size": 100,
        "withLatestExecution": "true",
        "sortBy": "startedAt:DESC",
    }
    
    rule_status = os.getenv("RULE_STATUS")
    rule_type = os.getenv("RULE_TYPE")
    tag = os.getenv("TAG")
    assembly_ids = os.getenv("ASSEMBLY_IDS")
    
    if rule_status:
        params["ruleStatus"] = rule_status
    if rule_type:
        params["ruleType"] = rule_type
    if tag and tag.strip():
        params["tag"] = tag.strip()
    if assembly_ids and assembly_ids.strip():
        params["assemblyIds"] = assembly_ids.strip()
    
    return params


def get_column_key(left_col: str, right_col: str) -> str:
    """Generate the label key from left and right column names."""
    return f"{left_col}_{right_col}"


# ============================================================
# PART 1: Version Comparison - Find New Column Mappings
# ============================================================

async def fetch_rules_with_versions(session) -> dict:
    """Fetch all EQUALITY rules and return a dict of policy_id -> version info."""
    params = build_params()
    policy_versions = {}
    page = 0
    
    while True:
        params["page"] = page
        async with session.get(RULES_LIST_API, params=params, headers=HEADERS) as response:
            response.raise_for_status()
            data = await response.json()
        
        rules = data.get("rules", [])
        if not rules:
            break
        
        for item in rules:
            rule = item.get("rule", {})
            rule_id = rule.get("id")
            version = rule.get("version", 1)
            rule_name = rule.get("name", "")
            rule_type = rule.get("type", "")
            # Only include EQUALITY (Reconciliation) policies
            if rule_id and rule_type == "EQUALITY":
                policy_versions[str(rule_id)] = {
                    "version": version,
                    "name": rule_name
                }
        
        page += 1
    
    return policy_versions


async def fetch_policy_version(session, policy_id, version):
    """Fetch reconciliation policy details for a specific version."""
    url = RECON_POLICY_VERSION_API.format(policy_id=policy_id, version=version)
    try:
        async with session.get(url, headers=HEADERS) as response:
            if response.status == 200:
                return await response.json()
            return None
    except Exception:
        return None


def extract_mappings_info(data: dict) -> dict:
    """Extract column mappings info from policy details for comparison."""
    details = data.get("details", {})
    column_mappings = details.get("columnMappings", [])
    
    # Get Recon_Type from details.items
    items = details.get("items", [])
    recon_type = items[0].get("measurementType", "") if items else "EQUALITY"
    
    info = {
        "mapping_keys": set(),  # Set of "leftCol_rightCol" combinations
        "mappings": column_mappings,
        "recon_type": recon_type
    }
    
    for mapping in column_mappings:
        left_col = mapping.get("leftColumnName", "")
        right_col = mapping.get("rightColumnName", "")
        if left_col and right_col:
            key = get_column_key(left_col, right_col)
            info["mapping_keys"].add(key)
    
    return info


def find_new_mappings(v1_info: dict, latest_info: dict, policy_id, policy_name) -> list:
    """Compare version 1 with latest version and find new column mappings."""
    new_mappings = []
    latest_mappings = latest_info.get("mappings", [])
    recon_type = latest_info.get("recon_type", "EQUALITY")
    
    for mapping in latest_mappings:
        left_col = mapping.get("leftColumnName", "")
        right_col = mapping.get("rightColumnName", "")
        mapping_id = mapping.get("id")
        
        if left_col and right_col:
            key = get_column_key(left_col, right_col)
            
            # Check if this mapping exists in v1
            if key not in v1_info["mapping_keys"]:
                new_mappings.append({
                    "Policy_ID": policy_id,
                    "Policy_Name": policy_name,
                    "Rule_ID": mapping_id,
                    "Recon_Type": recon_type,
                    "Left_Column_Name": left_col,
                    "Right_Column_Name": right_col
                })
    
    return new_mappings


# ============================================================
# PART 2: Add Labels to Reconciliation Policies
# ============================================================

async def fetch_policy(session, policy_id):
    """Fetch current reconciliation policy details (latest version)."""
    url = RECON_POLICY_API.format(policy_id=policy_id)
    try:
        async with session.get(url, headers=HEADERS) as response:
            if response.status == 200:
                return await response.json()
            return None
    except Exception:
        return None


async def update_policy(session, policy_id, payload):
    """Update reconciliation policy with PUT request."""
    url = RECON_POLICY_API.format(policy_id=policy_id)
    try:
        async with session.put(url, headers=HEADERS, json=payload) as response:
            if response.status == 200:
                return True
            else:
                error_text = await response.text()
                print(f"      ⚠️  PUT failed: HTTP {response.status} - {error_text[:100]}")
                return False
    except Exception as e:
        print(f"      ❌ PUT error: {e}")
        return False


def build_update_payload(policy_data: dict, label_mappings: dict) -> tuple:
    """Build the PUT payload by adding labels to column mappings.
    
    Args:
        policy_data: The current policy data from the server
        label_mappings: Dict of {column_key -> original_rule_id} from CSV
    
    Labels are added using:
        key = Left_Column_Name_Right_Column_Name
        value = ORIGINAL Rule_ID from when the rule was first added (from CSV)
    
    Matching is done by column names (since mapping IDs change between versions).
    """
    rule = policy_data.get("rule", {})
    details = policy_data.get("details", {})
    column_mappings = details.get("columnMappings", [])
    items = details.get("items", [])
    
    labels_added = []
    labels_skipped = []
    
    # Build the mappings array with labels
    updated_mappings = []
    for mapping in column_mappings:
        left_col = mapping.get("leftColumnName", "")
        right_col = mapping.get("rightColumnName", "")
        mapping_key = get_column_key(left_col, right_col)
        
        # Get existing labels
        existing_labels = mapping.get("labels", [])
        existing_label_keys = {label.get("key") for label in existing_labels if label.get("key")}
        
        # Check if we have a label mapping for this column pair
        if mapping_key in label_mappings:
            original_rule_id = label_mappings[mapping_key]  # Use original Rule_ID from CSV
            
            if mapping_key in existing_label_keys:
                labels_skipped.append(mapping_key)
            else:
                # Add label with ORIGINAL Rule_ID from when rule was first added
                new_label = {"key": mapping_key, "value": str(original_rule_id)}
                existing_labels.append(new_label)
                labels_added.append(mapping_key)
        
        updated_mapping = {
            "id": mapping.get("id"),
            "leftColumnName": left_col,
            "operation": mapping.get("operation", "EQ"),
            "rightColumnName": right_col,
            "useForJoining": mapping.get("useForJoining", False),
            "reconciliationRuleId": mapping.get("reconciliationRuleId"),
            "isJoinColumnUsedForMeasure": mapping.get("isJoinColumnUsedForMeasure", False),
            "ignoreNullValues": mapping.get("ignoreNullValues", False),
            "weightage": mapping.get("weightage", 100),
            "ruleVersion": mapping.get("ruleVersion"),
            "businessExplanation": mapping.get("businessExplanation", ""),
            "isWarning": mapping.get("isWarning", False),
            "labels": [{"key": l.get("key"), "value": l.get("value")} for l in existing_labels],
            "isArchived": mapping.get("isArchived", False),
            "mappingType": mapping.get("mappingType", "AUTO")
        }
        
        updated_mappings.append(updated_mapping)
    
    # Build the items array
    updated_items = []
    for item in items:
        updated_items.append({
            "measurementType": item.get("measurementType", "EQUALITY"),
            "executionOrder": item.get("executionOrder", 1),
            "id": item.get("id")
        })
    
    # Build the rule object
    left_backing_asset = rule.get("leftBackingAsset") or {}
    right_backing_asset = rule.get("rightBackingAsset") or {}
    notification_channels = rule.get("notificationChannels") or {}
    spark_resource_config = rule.get("sparkResourceConfig") or {}
    
    payload = {
        "rule": {
            "subType": rule.get("subType", "ASSET"),
            "enabled": rule.get("enabled", True),
            "name": rule.get("name"),
            "description": rule.get("description"),
            "executionTimeoutInMinutes": rule.get("executionTimeoutInMinutes"),
            "totalExecutionTimeoutInMinutes": rule.get("totalExecutionTimeoutInMinutes"),
            "analyticsPipelineId": rule.get("analyticsPipelineId"),
            "scheduleType": rule.get("scheduleType", "FULL"),
            "jobSchedule": rule.get("jobSchedule"),
            "segments": rule.get("segments", []),
            "customSqlConfig": rule.get("customSqlConfig"),
            "policyScoreStrategy": rule.get("policyScoreStrategy", "WEIGHTAGE"),
            "thresholdLevel": rule.get("thresholdLevel", {"success": 100, "warning": 70}),
            "leftFilter": rule.get("leftFilter", ""),
            "leftSparkFilterSelectedColumns": rule.get("leftSparkFilterSelectedColumns", []),
            "leftSparkSQLFilterType": rule.get("leftSparkSQLFilterType", "STATIC"),
            "rightFilter": rule.get("rightFilter", ""),
            "rightSparkFilterSelectedColumns": rule.get("rightSparkFilterSelectedColumns", []),
            "rightSparkSQLFilterType": rule.get("rightSparkSQLFilterType", "STATIC"),
            "delayInMinutes": rule.get("delayInMinutes"),
            "engineType": rule.get("engineType", "SPARK"),
            "leftEngineType": rule.get("leftEngineType", "SPARK"),
            "rightEngineType": rule.get("rightEngineType", "SPARK"),
            "joinType": rule.get("joinType", "LEFT"),
            "leftSparkSQLDynamicFilterVariableMapping": rule.get("leftSparkSQLDynamicFilterVariableMapping", {
                "ruleName": rule.get("name"),
                "mapping": []
            }),
            "rightSparkSQLDynamicFilterVariableMapping": rule.get("rightSparkSQLDynamicFilterVariableMapping", {
                "ruleName": rule.get("name"),
                "mapping": []
            }),
            "id": rule.get("id"),
            "includeInQualityScore": rule.get("includeInQualityScore", True),
            "type": rule.get("type", "EQUALITY"),
            "scheduled": rule.get("scheduled", False),
            "leftBackingAsset": {
                "tableAssetId": left_backing_asset.get("tableAssetId"),
                "id": left_backing_asset.get("id")
            },
            "rightBackingAsset": {
                "tableAssetId": right_backing_asset.get("tableAssetId"),
                "marker": right_backing_asset.get("marker"),
                "id": right_backing_asset.get("id")
            },
            "timeSecondsOffset": rule.get("timeSecondsOffset", 30),
            "notificationChannels": {
                "configuredNotificationGroupIds": notification_channels.get("configuredNotificationGroupIds", []),
                "notifyOn": notification_channels.get("notifyOn", []),
                "notifyOnSuccess": notification_channels.get("notifyOnSuccess", False),
                "severity": notification_channels.get("severity", "CRITICAL"),
                "alertsEnabled": notification_channels.get("alertsEnabled", True),
                "reNotifyFactor": notification_channels.get("reNotifyFactor", 0),
                "notifyOnWarning": notification_channels.get("notifyOnWarning", False),
                "notificationEnabled": notification_channels.get("notificationEnabled", False)
            },
            "sparkResourceConfig": {
                "additionalConfiguration": spark_resource_config.get("additionalConfiguration", {}),
                "yunikorn": spark_resource_config.get("yunikorn")
            },
            "resourceStrategyType": rule.get("resourceStrategyType", "INVENTORY"),
            "selectedResourceInventory": rule.get("selectedResourceInventory", "Medium"),
            "autoRetryEnabled": rule.get("autoRetryEnabled", False),
            "policyGroups": rule.get("policyGroups", []),
            "labels": rule.get("labels", []),
            "tags": rule.get("tags", [])
        },
        "items": updated_items,
        "mappings": updated_mappings,
        "cloningDetails": None,
        "analyticsPipelineId": rule.get("analyticsPipelineId")
    }
    
    return payload, labels_added, labels_skipped


# ============================================================
# CSV Operations
# ============================================================

def read_existing_csv(csv_path: str) -> tuple:
    """Read existing CSV and return (rows, existing_mapping_keys, policy_labels).
    
    policy_labels maps: policy_id -> {column_key -> original_rule_id}
    This stores the Rule_ID from when the rule was FIRST added (version 1 or later).
    """
    rows = []
    existing_mapping_keys = set()  # Track by column combination
    policy_labels = defaultdict(dict)  # policy_id -> {column_key -> original_rule_id}
    
    try:
        with open(csv_path, mode="r", newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                rows.append(row)
                policy_id = row.get("Policy_ID")
                rule_id = row.get("Rule_ID")
                left_col = row.get("Left_Column_Name", "")
                right_col = row.get("Right_Column_Name", "")
                
                if policy_id and left_col and right_col:
                    # Key is Left_Column_Name_Right_Column_Name
                    mapping_key = get_column_key(left_col, right_col)
                    existing_mapping_keys.add(f"{policy_id}_{mapping_key}")
                    # Store the original Rule_ID from when the rule was first added
                    if rule_id:
                        policy_labels[policy_id][mapping_key] = rule_id
        
        print(f"📂 Loaded {len(rows)} existing entries from {csv_path}")
    except FileNotFoundError:
        print(f"⚠️  {csv_path} not found. Will create a new file.")
    
    return rows, existing_mapping_keys, policy_labels


def get_unique_policy_ids(rows: list) -> set:
    """Extract unique policy IDs from existing CSV rows."""
    return set(row.get("Policy_ID") for row in rows if row.get("Policy_ID"))


def write_csv(rows: list, csv_path: str):
    """Write results to CSV."""
    with open(csv_path, mode="w", newline="") as file:
        fieldnames = ["Policy_ID", "Policy_Name", "Rule_ID", "Recon_Type", "Left_Column_Name", "Right_Column_Name"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# ============================================================
# Main Execution
# ============================================================

async def main():
    print("=" * 70)
    print("Sync Recon Policy Labels - Combined Update & Label Script")
    print("=" * 70)
    
    # Read existing CSV
    existing_rows, existing_mapping_keys, policy_labels = read_existing_csv(RECON_POLICY_DETAILS_CSV)
    policy_ids = get_unique_policy_ids(existing_rows)
    
    if not policy_ids:
        print("No policy IDs found in CSV. Please run Step_2_Fetch_Recon_Policy_Details.py first.")
        return
    
    print(f"📋 Found {len(policy_ids)} unique EQUALITY policies in CSV")
    
    new_mappings_added = []
    total_labels_added = 0
    total_labels_skipped = 0
    policies_updated = 0
    
    async with aiohttp.ClientSession() as session:
        # ============================================================
        # STEP 1: Find new column mappings (version comparison)
        # ============================================================
        print("\n" + "=" * 70)
        print("STEP 1: Finding new column mappings (version comparison)")
        print("=" * 70)
        
        print("\n📡 Fetching rule versions from API...")
        policy_versions = await fetch_rules_with_versions(session)
        print(f"   Found {len(policy_versions)} EQUALITY rules with version info")
        
        # Find policies with version > 1
        policies_to_compare = []
        for pid in policy_ids:
            if pid in policy_versions:
                version_info = policy_versions[pid]
                if version_info["version"] > 1:
                    policies_to_compare.append({
                        "policy_id": pid,
                        "version": version_info["version"],
                        "name": version_info["name"]
                    })
        
        print(f"\n🔍 Found {len(policies_to_compare)} policies with version > 1")
        
        if policies_to_compare:
            for policy_info in policies_to_compare:
                pid = policy_info["policy_id"]
                latest_version = policy_info["version"]
                policy_name = policy_info["name"]
                
                print(f"\n   Comparing policy {pid} ({policy_name}): v1 vs v{latest_version}")
                
                v1_data, latest_data = await asyncio.gather(
                    fetch_policy_version(session, pid, 1),
                    fetch_policy_version(session, pid, latest_version)
                )
                
                if not v1_data or not latest_data:
                    print(f"      ⚠️  Skipping - couldn't fetch version data")
                    continue
                
                v1_info = extract_mappings_info(v1_data)
                latest_info = extract_mappings_info(latest_data)
                
                new_mappings = find_new_mappings(v1_info, latest_info, pid, policy_name)
                
                for mapping in new_mappings:
                    mapping_key = get_column_key(mapping["Left_Column_Name"], mapping["Right_Column_Name"])
                    full_key = f"{pid}_{mapping_key}"
                    
                    if full_key not in existing_mapping_keys:
                        new_mappings_added.append(mapping)
                        existing_mapping_keys.add(full_key)
                        # Add to policy_labels with the Rule_ID from when it was first added
                        policy_labels[pid][mapping_key] = mapping["Rule_ID"]
                        print(f"      ➕ New mapping: {mapping['Rule_ID']} - {mapping_key}")
        
        # Update CSV if new mappings found
        if new_mappings_added:
            updated_rows = existing_rows + new_mappings_added
            write_csv(updated_rows, RECON_POLICY_DETAILS_CSV)
            print(f"\n✅ Added {len(new_mappings_added)} new mappings to {RECON_POLICY_DETAILS_CSV}")
        else:
            print(f"\n✅ No new mappings found. CSV is up to date.")
        
        # ============================================================
        # STEP 2: Add labels to reconciliation policies
        # ============================================================
        print("\n" + "=" * 70)
        print("STEP 2: Adding labels to reconciliation policies")
        print("=" * 70)
        
        for policy_id, label_mappings in policy_labels.items():
            policy_data = await fetch_policy(session, policy_id)
            if not policy_data:
                print(f"\n⚠️  Policy {policy_id}: Failed to fetch")
                continue
            
            policy_name = policy_data.get("rule", {}).get("name", "Unknown")
            print(f"\n📋 Policy {policy_id} ({policy_name})")
            
            # Pass the dict of {column_key -> original_rule_id}
            payload, labels_added, labels_skipped = build_update_payload(policy_data, label_mappings)
            
            if labels_skipped:
                print(f"   ⏭️  Already present: {', '.join(labels_skipped)}")
                total_labels_skipped += len(labels_skipped)
            
            if not labels_added:
                print(f"   ✅ No new labels to add")
                continue
            
            print(f"   ➕ Adding: {', '.join(labels_added)}")
            
            success = await update_policy(session, policy_id, payload)
            
            if success:
                print(f"   ✅ Updated successfully")
                total_labels_added += len(labels_added)
                policies_updated += 1
            else:
                print(f"   ❌ Update failed")
    
    # ============================================================
    # Summary
    # ============================================================
    print("\n" + "=" * 70)
    print("📊 SUMMARY")
    print("=" * 70)
    print(f"   Policies in CSV: {len(policy_ids)}")
    print(f"   Policies with version > 1: {len(policies_to_compare)}")
    print(f"   New mappings found & added to CSV: {len(new_mappings_added)}")
    print(f"   Policies updated with labels: {policies_updated}")
    print(f"   Labels added: {total_labels_added}")
    print(f"   Labels skipped (already present): {total_labels_skipped}")


# Run the script
if __name__ == "__main__":
    asyncio.run(main())

