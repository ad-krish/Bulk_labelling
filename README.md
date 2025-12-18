# Bulk Policy Labels Implementation

A set of Python scripts for bulk management of policy labels in Acceldata's Data Quality platform. This toolset automates the process of fetching policy details, comparing versions, and adding labels to rules.

## 📋 Overview

This project provides scripts to:
1. Fetch all policies (DATA_QUALITY and EQUALITY/Reconciliation types)
2. Extract rule/mapping details from policies
3. Compare policy versions to detect newly added rules
4. Automatically add labels to rules via API

## 🔧 Prerequisites

- Python 3.8+
- Required packages:
  ```bash
  pip install aiohttp python-dotenv
  ```

## ⚙️ Configuration

Create/update `config.env` with your credentials:

```env
# API Credentials
ACCESS_KEY=your_access_key
SECRET_KEY=your_secret_key
HOST=your_host.acceldata.app

# Rule Filter Parameters
RULE_STATUS=ENABLED,ACTIVE
RULE_TYPE=DATA_QUALITY,EQUALITY
# Optional parameters - leave empty or comment out if not needed
TAG=your_tag_id
#ASSEMBLY_IDS=
```

## 📁 Scripts

### Step 1: Fetch Policy IDs

```bash
python Step_1_Fetch_Policy_ID.py
```

**Purpose:** Fetches all policy IDs and types from the API.

**Output:** `Policy_id_mapping.csv`

| Column | Description |
|--------|-------------|
| Policy_Name | Name of the policy |
| Policy_ID | Unique policy identifier |
| Policy_Type | Type (DATA_QUALITY, EQUALITY, etc.) |

---

### Step 2: Fetch Policy Details

#### For DATA_QUALITY Policies:
```bash
python Step_2_Fetch_Policy_Details.py
```

**Purpose:** Fetches rule details for DATA_QUALITY policies (version 1).

**Input:** `Policy_id_mapping.csv` (filters for `Policy_Type=DATA_QUALITY`)

**Output:** `Policy_Rule_Details.csv`

| Column | Description |
|--------|-------------|
| Policy_ID | Policy identifier |
| Policy_Name | Policy name |
| Rule_ID | Rule item identifier |
| Rule_Type | Measurement type (MISSING_VALUES, CUSTOM, etc.) |
| Column_Name | Column name or derived key |

#### For EQUALITY (Reconciliation) Policies:
```bash
python Step_2_Fetch_Recon_Policy_Details.py
```

**Purpose:** Fetches column mapping details for EQUALITY policies (version 1).

**Input:** `Policy_id_mapping.csv` (filters for `Policy_Type=EQUALITY`)

**Output:** `Recon_Policy_Rule_Details.csv`

| Column | Description |
|--------|-------------|
| Policy_ID | Policy identifier |
| Policy_Name | Policy name |
| Rule_ID | Column mapping identifier |
| Recon_Type | Measurement type (EQUALITY, ROW_COUNT_EQUALITY) |
| Left_Column_Name | Left table column name |
| Right_Column_Name | Right table column name |

---

### Step 3: Sync Labels

#### For DATA_QUALITY Policies:
```bash
python Step_3_Sync_Policy_Labels.py
```

**Purpose:** 
1. Compares version 1 with latest version to find new rules
2. Updates CSV with newly found rules
3. Adds labels to policies via PUT API

**Label Format:**
```json
{
    "key": "Column_Name",
    "value": "Rule_ID"
}
```

#### For EQUALITY (Reconciliation) Policies:
```bash
python Step_3_Sync_Recon_Policy_Labels.py
```

**Purpose:**
1. Compares version 1 with latest version to find new column mappings
2. Updates CSV with newly found mappings
3. Adds labels to policies via PUT API

**Label Format:**
```json
{
    "key": "Left_Column_Name_Right_Column_Name",
    "value": "Rule_ID"
}
```

---

## 🔄 Complete Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│  Step 1: python Step_1_Fetch_Policy_ID.py                       │
│          Output: Policy_id_mapping.csv                          │
└─────────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┴───────────────────┐
          ▼                                       ▼
┌─────────────────────────┐         ┌─────────────────────────────┐
│  DATA_QUALITY Policies  │         │  EQUALITY Policies          │
└─────────────────────────┘         └─────────────────────────────┘
          │                                       │
          ▼                                       ▼
┌─────────────────────────┐         ┌─────────────────────────────┐
│  Step 2: Fetch Details  │         │  Step 2: Fetch Recon Details│
│  Policy_Rule_Details.csv│         │  Recon_Policy_Rule_Details  │
└─────────────────────────┘         └─────────────────────────────┘
          │                                       │
          ▼                                       ▼
┌─────────────────────────┐         ┌─────────────────────────────┐
│  Step 3: Sync Labels    │         │  Step 3: Sync Recon Labels  │
│  - Find new rules       │         │  - Find new mappings        │
│  - Update CSV           │         │  - Update CSV               │
│  - Add labels to server │         │  - Add labels to server     │
└─────────────────────────┘         └─────────────────────────────┘
```

---

## 📊 Special Rule Types

For DATA_QUALITY policies, Column_Name is derived based on `measurementType`:

| measurementType | Column_Name Format |
|-----------------|-------------------|
| CUSTOM | `CUSTOM-{8-char MD5 hash of ruleExpression}` |
| SQL_METRIC | `SQL_METRIC-{8-char MD5 hash of ruleExpression}` |
| UDF_PREDICATE | `UDF_PREDICATE-{udfId}` |
| SIZE_CHECK | `SIZE_CHECK` |
| Others | Actual `columnName` value |

---

## 🔑 Key Features

- **Async Operations:** Uses `aiohttp` for concurrent API calls
- **Version Comparison:** Detects rules added after version 1
- **Idempotent:** Skips labels that already exist
- **Configurable:** All parameters via `config.env`
- **Pagination Support:** Handles large datasets

---

## 📝 Notes

1. **Rule IDs:** For reconciliation policies, mapping IDs change between versions. The scripts match by column names but use the original Rule_ID from when the rule was first added.

2. **Labels:** Labels are only added if they don't already exist (checked by key).

3. **Version 1:** Initial fetch always uses version 1 to capture the original Rule_IDs.

---

## 🗂️ File Structure

```
Bulk_Rule_labels_implementation/
├── config.env                          # Configuration file
├── Step_1_Fetch_Policy_ID.py           # Fetch all policy IDs
├── Step_2_Fetch_Policy_Details.py      # Fetch DATA_QUALITY details
├── Step_2_Fetch_Recon_Policy_Details.py # Fetch EQUALITY details
├── Step_3_Sync_Policy_Labels.py        # Sync DATA_QUALITY labels
├── Step_3_Sync_Recon_Policy_Labels.py  # Sync EQUALITY labels
├── Policy_id_mapping.csv               # Output: Policy IDs
├── Policy_Rule_Details.csv             # Output: DQ rule details
├── Recon_Policy_Rule_Details.csv       # Output: Recon mapping details
└── README.md                           # This file
```

---

## 🚀 Quick Start

```bash
# 1. Install dependencies
pip install aiohttp python-dotenv

# 2. Configure credentials
cp config.env.example config.env
# Edit config.env with your credentials

# 3. Run the workflow
python Step_1_Fetch_Policy_ID.py
python Step_2_Fetch_Policy_Details.py
python Step_2_Fetch_Recon_Policy_Details.py
python Step_3_Sync_Policy_Labels.py
python Step_3_Sync_Recon_Policy_Labels.py
```

---

## 📄 License

Internal use only - Acceldata

