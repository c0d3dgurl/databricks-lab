"""Read a public Delta Share from any Linux box. No Databricks account needed.

    pip install delta-sharing
    python3 read_share.py
"""
import urllib.request

import delta_sharing

PROFILE_URL = (
    "https://raw.githubusercontent.com/delta-io/delta-sharing/main/examples/open-datasets.share"
)
urllib.request.urlretrieve(PROFILE_URL, "open-datasets.share")

client = delta_sharing.SharingClient("open-datasets.share")
tables = client.list_all_tables()
for t in tables:
    print(f"{t.share}.{t.schema}.{t.name}")

first = tables[0]
df = delta_sharing.load_as_pandas(
    f"open-datasets.share#{first.share}.{first.schema}.{first.name}", limit=10
)
print(df)
