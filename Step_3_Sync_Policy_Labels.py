import asyncio
import aiohttp
import csv
import hashlib
from dotenv import load_dotenv
import os
from collections import defaultdict

# --- CONFIG ---
load_dotenv("config.env")
HOST = os.getenv("HOST")

# API URLs
RULES_LIST_API = f"http://{HOST}/catalog-server/api/rules"
POLICY_DETAILS_API = f"https://{HOST}/catalog-server/api/rules/data-quality/{{policy_id}}"
POLICY_VERSION_API = f"https://{HOST}/catalog-server/api/rules/data-quality/{{policy_id}}?version={{version}}"

HEADERS = {
    "accessKey": os.getenv("ACCESS_KEY"),
    "secretKey": os.getenv("SECRET_KEY"),
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# File paths
POLICY_DETAILS_CSV = "Policy_Rule_Details.csv"

# Label options
OVERRIDE_LABELS = os.getenv("OVERRIDE_LABELS", "false").lower() == "true"


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


def compute_hash(expression: str) -> str:
    """Compute MD5 hash of the rule expression."""
    if not expression:
        return "empty"
    return hashlib.md5(expression.encode()).hexdigest()[:8]


def get_column_name(item: dict) -> str:
    """
    Determine the Column_Name based on measurementType.
    
    Column-based rules include measurementType prefix to handle
    cases where same column has multiple rule types.
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
        # Include measurementType to ensure uniqueness when same column
        # has multiple rule types
        if column_name and measurement_type:
            return f"{measurement_type}-{column_name}"
        elif column_name:
            return column_name
        else:
            return measurement_type or "UNKNOWN"


# ============================================================
# PART 1: Version Comparison - Find New Rules
# ============================================================

async def fetch_rules_with_versions(session) -> dict:
    """Fetch all rules and return a dict of policy_id -> version info."""
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
            if rule_id and rule_type == "DATA_QUALITY":
                policy_versions[str(rule_id)] = {
                    "version": version,
                    "name": rule_name
                }
        
        page += 1
    
    return policy_versions


async def fetch_policy_version(session, policy_id, version):
    """Fetch policy details for a specific version."""
    url = POLICY_VERSION_API.format(policy_id=policy_id, version=version)
    try:
        async with session.get(url, headers=HEADERS) as response:
            if response.status == 200:
                return await response.json()
            return None
    except Exception:
        return None


def extract_items_info(data: dict) -> dict:
    """Extract relevant info from policy details for comparison."""
    details = data.get("details", {})
    items = details.get("items", [])
    
    info = {
        "column_names": set(),
        "custom_expressions": set(),
        "sql_metric_expressions": set(),
        "udf_predicate_ids": set(),
        "has_size_check": False,
        "items": items
    }
    
    for item in items:
        measurement_type = item.get("measurementType", "")
        column_name = item.get("columnName", "")
        rule_expression = item.get("ruleExpression", "")
        
        if measurement_type == "CUSTOM":
            if rule_expression:
                info["custom_expressions"].add(rule_expression)
        elif measurement_type == "SQL_METRIC":
            if rule_expression:
                info["sql_metric_expressions"].add(rule_expression)
        elif measurement_type == "UDF_PREDICATE":
            value = item.get("value", {})
            if value:
                udf_id = value.get("udfId")
                if udf_id:
                    info["udf_predicate_ids"].add(udf_id)
        elif measurement_type == "SIZE_CHECK":
            info["has_size_check"] = True
        else:
            if column_name:
                info["column_names"].add(column_name)
    
    return info


def find_new_rules(v1_info: dict, latest_info: dict, policy_id, policy_name) -> list:
    """Compare version 1 with latest version and find new rules."""
    new_rules = []
    latest_items = latest_info.get("items", [])
    
    for item in latest_items:
        measurement_type = item.get("measurementType", "")
        rule_id = item.get("id")
        column_name = item.get("columnName", "")
        rule_expression = item.get("ruleExpression", "")
        
        is_new = False
        
        if measurement_type == "CUSTOM":
            if rule_expression and rule_expression not in v1_info["custom_expressions"]:
                is_new = True
            elif not v1_info["custom_expressions"] and rule_expression:
                is_new = True
        elif measurement_type == "SQL_METRIC":
            if rule_expression and rule_expression not in v1_info["sql_metric_expressions"]:
                is_new = True
        elif measurement_type == "UDF_PREDICATE":
            value = item.get("value", {})
            if value:
                udf_id = value.get("udfId")
                if udf_id and udf_id not in v1_info["udf_predicate_ids"]:
                    is_new = True
        elif measurement_type == "SIZE_CHECK":
            if not v1_info.get("has_size_check", False):
                is_new = True
        else:
            if column_name and column_name not in v1_info["column_names"]:
                is_new = True
        
        if is_new:
            new_rules.append({
                "Policy_ID": policy_id,
                "Policy_Name": policy_name,
                "Rule_ID": rule_id,
                "Rule_Type": measurement_type,
                "Column_Name": get_column_name(item)
            })
    
    return new_rules


# ============================================================
# PART 2: Add Labels to Policies
# ============================================================

async def fetch_policy(session, policy_id):
    """Fetch current policy details (latest version)."""
    url = POLICY_DETAILS_API.format(policy_id=policy_id)
    try:
        async with session.get(url, headers=HEADERS) as response:
            if response.status == 200:
                return await response.json()
            return None
    except Exception:
        return None


async def update_policy(session, policy_id, payload):
    """Update policy with PUT request."""
    url = POLICY_DETAILS_API.format(policy_id=policy_id)
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
    """Build the PUT payload by adding labels to items.
    
    If OVERRIDE_LABELS is True, removes existing labels and re-adds from CSV.
    """
    rule = policy_data.get("rule", {})
    details = policy_data.get("details", {})
    items = details.get("items", [])
    
    labels_added = []
    labels_skipped = []
    labels_removed = []
    
    updated_items = []
    for item in items:
        item_column_key = get_column_name(item)
        existing_labels = item.get("labels", [])
        existing_label_keys = {label.get("key") for label in existing_labels if label.get("key")}
        
        if OVERRIDE_LABELS:
            # Remove all existing labels and add fresh from CSV
            if existing_labels:
                labels_removed.extend([l.get("key") for l in existing_labels if l.get("key")])
            existing_labels = []  # Clear existing labels
            
            if item_column_key in label_mappings:
                rule_id = label_mappings[item_column_key]
                new_label = {"key": item_column_key, "value": str(rule_id)}
                existing_labels.append(new_label)
                labels_added.append(item_column_key)
        else:
            # Normal mode: only add if not exists
            if item_column_key in label_mappings:
                rule_id = label_mappings[item_column_key]
                
                if item_column_key in existing_label_keys:
                    labels_skipped.append(item_column_key)
                else:
                    new_label = {"key": item_column_key, "value": str(rule_id)}
                    existing_labels.append(new_label)
                    labels_added.append(item_column_key)
        
        updated_item = {
            "measurementType": item.get("measurementType"),
            "columnName": item.get("columnName", ""),
            "executionOrder": item.get("executionOrder"),
            "weightage": item.get("weightage"),
            "businessExplanation": item.get("businessExplanation", ""),
            "labels": [{"key": l.get("key"), "value": l.get("value")} for l in existing_labels],
            "isWarning": item.get("isWarning", False),
            "associatedDQRecommendationId": item.get("associatedDQRecommendationId"),
            "bulkPolicyDqRuleId": item.get("bulkPolicyDqRuleId"),
            "thresholdConfig": item.get("thresholdConfig"),
            "value": item.get("value"),
            "id": item.get("id")
        }
        
        if item.get("ruleExpression"):
            updated_item["ruleExpression"] = item.get("ruleExpression")
        
        updated_items.append(updated_item)
    
    backing_asset = rule.get("backingAsset") or {}
    notification_channels = rule.get("notificationChannels") or {}
    
    payload = {
        "rule": {
            "subType": rule.get("subType", "ASSET"),
            "enabled": rule.get("enabled", True),
            "name": rule.get("name"),
            "description": rule.get("description"),
            "schedule": rule.get("schedule", ""),
            "executionTimeoutInMinutes": rule.get("executionTimeoutInMinutes"),
            "totalExecutionTimeoutInMinutes": rule.get("totalExecutionTimeoutInMinutes"),
            "analyticsPipelineId": None,
            "scheduleType": "RECENT",
            "jobSchedule": rule.get("jobSchedule"),
            "segments": rule.get("segments", []),
            "customSqlConfig": rule.get("customSqlConfig"),
            "policyScoreStrategy": rule.get("policyScoreStrategy", "WEIGHTAGE"),
            "thresholdLevel": rule.get("thresholdLevel", {"success": 100, "warning": 70}),
            "id": rule.get("id"),
            "includeInQualityScore": rule.get("includeInQualityScore", True),
            "type": rule.get("type", "DATA_QUALITY"),
            "scheduled": rule.get("scheduled", False),
            "backingAsset": {
                "tableAssetId": backing_asset.get("tableAssetId"),
                "id": backing_asset.get("id")
            },
            "notificationChannels": {
                "configuredNotificationGroupIds": notification_channels.get("configuredNotificationGroupIds", []),
                "notifyOn": notification_channels.get("notifyOn", []),
                "notifyOnSuccess": notification_channels.get("notifyOnSuccess", False),
                "severity": notification_channels.get("severity", "CRITICAL"),
                "alertsEnabled": notification_channels.get("alertsEnabled", True),
                "reNotifyFactor": notification_channels.get("reNotifyFactor", 0),
                "notifyOnWarning": notification_channels.get("notifyOnWarning", False),
                "notificationEnabled": True
            },
            "sparkResourceConfig": rule.get("sparkResourceConfig"),
            "policyGroups": rule.get("policyGroups", []),
            "labels": rule.get("labels", []),
            "tags": rule.get("tags", []),
            "additionalPersistedColumns": rule.get("additionalPersistedColumns", []),
            "filter": rule.get("filter"),
            "sparkSQLFilterType": rule.get("sparkSQLFilterType", "STATIC")
        },
        "items": updated_items,
        "transformUDFs": details.get("transformUDFs", []),
        "engineType": rule.get("engineType", "JDBC_SQL")
    }
    
    return payload, labels_added, labels_skipped


# ============================================================
# CSV Operations
# ============================================================

def read_existing_csv(csv_path: str) -> tuple:
    """Read existing CSV and return (rows, existing_rule_ids, policy_labels)."""
    rows = []
    existing_rule_ids = set()
    policy_labels = defaultdict(dict)
    
    try:
        with open(csv_path, mode="r", newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                rows.append(row)
                rule_id = row.get("Rule_ID")
                policy_id = row.get("Policy_ID")
                column_name = row.get("Column_Name")
                
                if rule_id:
                    existing_rule_ids.add(str(rule_id))
                if policy_id and column_name and rule_id:
                    policy_labels[policy_id][column_name] = rule_id
        
        print(f"📂 Loaded {len(rows)} existing entries from {csv_path}")
    except FileNotFoundError:
        print(f"⚠️  {csv_path} not found. Will create a new file.")
    
    return rows, existing_rule_ids, policy_labels


def get_unique_policy_ids(rows: list) -> set:
    """Extract unique policy IDs from existing CSV rows."""
    return set(row.get("Policy_ID") for row in rows if row.get("Policy_ID"))


def write_csv(rows: list, csv_path: str):
    """Write results to CSV."""
    with open(csv_path, mode="w", newline="") as file:
        fieldnames = ["Policy_ID", "Policy_Name", "Rule_ID", "Rule_Type", "Column_Name"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# ============================================================
# Main Execution
# ============================================================

async def main():
    print("=" * 70)
    print("Sync Policy Labels - Combined Update & Label Script")
    print("=" * 70)
    
    if OVERRIDE_LABELS:
        print("⚠️  OVERRIDE MODE: Existing labels will be REMOVED and re-added from CSV")
    
    # Read existing CSV
    existing_rows, existing_rule_ids, policy_labels = read_existing_csv(POLICY_DETAILS_CSV)
    policy_ids = get_unique_policy_ids(existing_rows)
    
    if not policy_ids:
        print("No policy IDs found in CSV. Please run Fetch_Policy_Details.py first.")
        return
    
    print(f"📋 Found {len(policy_ids)} unique policies in CSV")
    
    new_rules_added = []
    total_labels_added = 0
    total_labels_skipped = 0
    policies_updated = 0
    
    async with aiohttp.ClientSession() as session:
        # ============================================================
        # STEP 1: Find new rules (version comparison)
        # ============================================================
        print("\n" + "=" * 70)
        print("STEP 1: Finding new rules (version comparison)")
        print("=" * 70)
        
        print("\n📡 Fetching rule versions from API...")
        policy_versions = await fetch_rules_with_versions(session)
        print(f"   Found {len(policy_versions)} DATA_QUALITY rules with version info")
        
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
                
                v1_info = extract_items_info(v1_data)
                latest_info = extract_items_info(latest_data)
                
                new_rules = find_new_rules(v1_info, latest_info, pid, policy_name)
                
                for rule in new_rules:
                    if str(rule["Rule_ID"]) not in existing_rule_ids:
                        new_rules_added.append(rule)
                        existing_rule_ids.add(str(rule["Rule_ID"]))
                        # Also add to policy_labels for labeling
                        policy_labels[pid][rule["Column_Name"]] = rule["Rule_ID"]
                        print(f"      ➕ New rule: {rule['Rule_ID']} - {rule['Column_Name']}")
        
        # Update CSV if new rules found
        if new_rules_added:
            updated_rows = existing_rows + new_rules_added
            write_csv(updated_rows, POLICY_DETAILS_CSV)
            print(f"\n✅ Added {len(new_rules_added)} new rules to {POLICY_DETAILS_CSV}")
        else:
            print(f"\n✅ No new rules found. CSV is up to date.")
        
        # ============================================================
        # STEP 2: Add labels to policies
        # ============================================================
        print("\n" + "=" * 70)
        print("STEP 2: Adding labels to policies")
        print("=" * 70)
        
        for policy_id, label_mappings in policy_labels.items():
            policy_data = await fetch_policy(session, policy_id)
            if not policy_data:
                print(f"\n⚠️  Policy {policy_id}: Failed to fetch")
                continue
            
            policy_name = policy_data.get("rule", {}).get("name", "Unknown")
            print(f"\n📋 Policy {policy_id} ({policy_name})")
            
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
    print(f"   New rules found & added to CSV: {len(new_rules_added)}")
    print(f"   Policies updated with labels: {policies_updated}")
    print(f"   Labels added: {total_labels_added}")
    print(f"   Labels skipped (already present): {total_labels_skipped}")


# Run the script
if __name__ == "__main__":
    asyncio.run(main())

