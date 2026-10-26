#!/usr/bin/env bash
# Download REES46 "eCommerce behavior data from multi category store" into data/raw/.
# Kaggle hosts 2019-Oct and 2019-Nov (~5.3 GB + 8.4 GB uncompressed). The remaining months
# (2019-Dec .. 2020-Apr) are linked from the dataset page (REES46 open CDP) — download them
# manually into data/raw/ with the same naming: YYYY-Mon.csv.gz
#
# Requires KAGGLE_USERNAME / KAGGLE_KEY in .env (https://www.kaggle.com/settings -> API token)
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; [ -f .env ] && . ./.env; set +a

DATASET="mkechinov/ecommerce-behavior-data-from-multi-category-store"
OUT="data/raw"
mkdir -p "$OUT"

if ! command -v kaggle >/dev/null; then
  echo ">> installing kaggle CLI"; pip install --quiet kaggle
fi
: "${KAGGLE_USERNAME:?set KAGGLE_USERNAME in .env}"; : "${KAGGLE_KEY:?set KAGGLE_KEY in .env}"

for f in 2019-Oct.csv 2019-Nov.csv; do
  if [ -f "$OUT/$f.gz" ]; then echo ">> $f.gz already present"; continue; fi
  echo ">> downloading $f"
  kaggle datasets download -d "$DATASET" -f "$f" -p "$OUT"
  # kaggle wraps single files in a zip -> re-compress as gzip (Spark/pyarrow read .csv.gz natively)
  if [ -f "$OUT/$f.zip" ]; then
    unzip -p "$OUT/$f.zip" | gzip -6 > "$OUT/$f.gz" && rm "$OUT/$f.zip"
  elif [ -f "$OUT/$f" ]; then
    gzip -6 "$OUT/$f"
  fi
done
ls -lh "$OUT"
echo ">> next: python scripts/make_sample.py  (small file for dev / CI)"
