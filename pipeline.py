from model import EligibilityRecord
from pathlib import Path
import yaml
import pandas as pd
import re
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.yaml"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)
import re
import sys
from datetime import datetime

import pandas as pd
import yaml

STANDARD_COLUMNS = ["external_id", "first_name", "last_name", "dob", "email", "phone", "partner_code"]


def to_title_case(x: str) -> str:
    return x.strip().title() if isinstance(x, str) else ""


def to_lower(x: str) -> str:
    return x.strip().lower() if isinstance(x, str) else ""


def format_phone(x: str) -> str:
    if not isinstance(x, str):
        return ""
    digits = re.sub(r"\D", "", x)
    if len(digits) == 10:
        return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"
    return x.strip()


def parse_dob(x: str) -> str:
    if not isinstance(x, str):
        return ""
    x = x.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(x, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ingest_partner(partner_cfg: dict) -> pd.DataFrame:
    file_path = partner_cfg["file_path"]
    delimiter = partner_cfg["delimiter"]
    mapping = partner_cfg["column_mapping"]
    partner_code = partner_cfg["partner_code"]

    df = pd.read_csv(file_path, sep=delimiter, dtype=str, engine="python")
    df = df.rename(columns=mapping)

    # ensure columns exist
    for col in mapping.values():
        if col not in df.columns:
            df[col] = ""

    df["partner_code"] = partner_code

    df["external_id"] = df["external_id"].fillna("").astype(str).str.strip()
    df = df[df["external_id"] != ""]

    df["first_name"] = df["first_name"].fillna("").apply(to_title_case)
    df["last_name"] = df["last_name"].fillna("").apply(to_title_case)
    df["dob"] = df["dob"].fillna("").apply(parse_dob)
    df["email"] = df["email"].fillna("").apply(to_lower)
    df["phone"] = df["phone"].fillna("").apply(format_phone)

    for col in STANDARD_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    return df[STANDARD_COLUMNS]


def main():
    config_path = CONFIG_PATH
    out_path = BASE_DIR / "output" / "unified_output.csv"

    partners = config.get("partners", {})
    frames = []

    for _, partner_cfg in partners.items():
        frames.append(ingest_partner(partner_cfg))

    unified = pd.concat(frames, ignore_index=True)

    import os
    os.makedirs(BASE_DIR / "output", exist_ok=True)
    unified.to_csv(out_path, index=False)

    print(f"✅ Wrote: {out_path}")
    print(unified)

    # Convert to dataclass records
    records = [
        EligibilityRecord(**row)
        for row in unified.to_dict(orient="records")
    ]

    print(f"Created {len(records)} EligibilityRecord objects")


if __name__ == "__main__":
    main()
