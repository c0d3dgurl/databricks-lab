# Databricks lab

Hands-on lab for the Linux Accra Databricks talk. Everything runs on a free personal account.
The data is for Mart 233, a made-up online shop in Ghana. The shop, the customers and every number are invented.

## The data

| Table | What it holds |
|---|---|
| `customers` | 2,000 customer profiles: name, city, age band, joined date. Customers 1 and 2 are the demo shoppers Ama and Kofi |
| `products` | 18 products in six categories, with prices in Ghana cedis (GHS) |
| `bronze_orders` | 50,000 raw orders, with some duplicates and zero quantities left in on purpose |
| `silver_orders` | Clean orders joined to profiles and products, with revenue |
| `gold_customer_preferences` | One row per customer: favourite category, total spend, preferred payment |
| `gold_category_city` | Orders, revenue and rating per category, city and age band |
| `gold_products` | Orders, revenue and rating per product |
| `gold_daily` | Orders and revenue per day and category |
| `gold_personas` | One row per customer with their persona (Mobile first, Trend setter, and so on) and spend tier |
| `gold_persona_summary` | One row per persona: size, average spend, main age band, top city, top product |

Each customer has a hidden favourite category, mostly set by age band, and 60% of their orders come from it. The gold tables call a category someone's favourite when they bought the most items from it in the last 30 days (or ever, if they have not shopped recently), and give each customer a persona from it. Each city also leans towards one category (Kumasi to Fashion, Tamale to Groceries, and so on), which 15% of orders come from. The gold tables and the model should find both patterns.

## What you need

* **A laptop and a browser**: any OS.
* **A personal email**: for Databricks Free Edition.
* **Optional, for the terminal parts**: Python 3.10 or newer for Delta Sharing; Python 3.10 to 3.12 and Java 17 for local Spark.

## Core lab (about 30 minutes)

1. **Sign up**: go to https://www.databricks.com/learn/free-edition and create an account with a personal email.
2. **Import this repo**: in the workspace, Workspace > Create > Git folder, paste the repo URL.
3. **Ingest**: open `01_ingest.py`, connect to serverless compute, Run all. This creates the tables in `workspace.shop`. Look at the `DESCRIBE HISTORY` and time travel cells at the end.
4. **Gold**: open `02_gold.sql` and Run all. The last two cells show what each age band prefers and the top category in each city.
5. **Explore in SQL**: open the SQL editor, pick the Serverless Starter Warehouse and query the gold tables.
6. **Dashboard**: Dashboards > Create dashboard, add `gold_daily` and `gold_category_city` as datasets, and build one chart such as orders by city, coloured by category. A finished version is in `dashboards/mart233.lvdash.json`: Dashboards > Create > Import dashboard from file, or let `databricks bundle deploy` create it for you.
7. **Genie**: Genie > New, add the gold tables, and ask "Which category do shoppers in Kumasi order most?". Click "Show code" to read the SQL it wrote.

## Stretch goals

### Schedule it as a Job
Jobs & Pipelines > Create job, add `01_ingest` then `02_gold` as tasks, set an hourly trigger.
Or from your terminal with Asset Bundles:

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks auth login --host https://<your-workspace>.cloud.databricks.com
cd lab
databricks bundle validate
databricks bundle deploy
databricks bundle run shop_pipeline
```
`bundle deploy` creates the job and the Mart 233 dashboard. The dashboard is a JSON file in `dashboards/`, so it lives in git like everything else.

### Train a model
Run `03_model.py`. It predicts a customer's favourite category from their city, age band and preferred payment. Guessing at random scores about 17%; the model scores about 54%, so it found the pattern. It cannot get much higher, because within an age band shoppers split roughly half and half between two categories. Open Experiments to compare runs.

The last cell tries to register the model in Unity Catalog. On Free Edition this can be blocked, and the cell then prints a message instead of failing. The model is still saved with its run in Experiments.

### Shop app with Lakebase
`app/` is Mart 233, a small web shop built as a Databricks App: a Python (FastAPI) backend and a plain HTML page.
Sign in as one of two demo shoppers, Ama Mensah (Accra, 18-24) or Kofi Boateng (Kumasi, 45+), or as the store manager.
Profiles and products are read from the Delta gold tables. Interests, wishlist, basket and orders are written to Lakebase.
Tick "Show data sources" in the header to label every part of the page with the table it came from.

Apps can be written in Python or Node.js. The `command` in `app.yaml` says how to start yours.

1. Create a Lakebase project:
   ```bash
   databricks postgres create-project mart233 --json '{"spec":{"display_name":"mart233"}}'
   ```
2. Create the app with the SQL warehouse and the Lakebase database attached (find your warehouse id with `databricks warehouses list`):
   ```bash
   databricks apps create --json '{"name":"mart233","resources":[
     {"name":"sql-warehouse","sql_warehouse":{"id":"<warehouse-id>","permission":"CAN_USE"}},
     {"name":"postgres","postgres":{"branch":"projects/mart233/branches/production",
      "database":"projects/mart233/branches/production/databases/databricks-postgres",
      "permission":"CAN_CONNECT_AND_CREATE"}}]}'
   ```
3. Give the app's service principal read access to the gold tables. Its id is `service_principal_client_id` in `databricks apps get mart233`. In the SQL editor:
   ```sql
   GRANT USE CATALOG ON CATALOG workspace TO `<app service principal>`;
   GRANT USE SCHEMA ON SCHEMA workspace.shop TO `<app service principal>`;
   GRANT SELECT ON TABLE workspace.shop.gold_customer_preferences TO `<app service principal>`;
   GRANT SELECT ON TABLE workspace.shop.gold_products TO `<app service principal>`;
   GRANT SELECT ON TABLE workspace.shop.gold_daily TO `<app service principal>`;
   GRANT SELECT ON TABLE workspace.shop.gold_category_city TO `<app service principal>`;
   ```
4. Deploy the code (after `databricks bundle deploy` has uploaded it):
   ```bash
   databricks apps deploy mart233 --source-code-path /Workspace/Users/<you>/.bundle/shop/dev/files/app
   ```
5. Buy something as Ama, switch to Store manager to see it arrive, then read the same row from your terminal:
   ```bash
   databricks psql --project mart233 -- -c "SELECT * FROM shop.orders ORDER BY id DESC LIMIT 5;"
   ```

6. Close the loop, so purchases in the app change the gold tables. Register the Lakebase database in Unity Catalog:
   ```bash
   databricks postgres create-catalog mart233_app --json \
     '{"spec":{"branch":"projects/mart233/branches/production","postgres_database":"databricks_postgres"}}'
   ```
   `01_ingest` now reads `mart233_app.shop.order_items` and adds the app's order lines to bronze. Buy 12 Home items as Ama (three products, quantity 4), run `databricks bundle run shop_pipeline`, refresh the shop, and her persona changes from Mobile first to Home maker.
7. To start the demo again from clean data: `./reset_demo.sh`.

Free Edition apps stop 24 hours after a start or deploy. Start it again from the Apps page.

### Delta Sharing from your own terminal
```bash
python3 -m venv .venv-share && . .venv-share/bin/activate
pip install delta-sharing
python local/read_share.py
```
This reads a public share published by the Delta Sharing project: seven tables, no Databricks account involved.

On Free Edition you can create a share (Catalog > Delta Sharing > Shared by me), but sharing outside Databricks is switched off, so you cannot create a recipient with a profile file. On a paid workspace the provider side is one more step: create a recipient and send them the activation link.

### Spark and Delta on your laptop
PySpark 3.5 runs on Python 3.10 to 3.12, and needs Java 17. It does not work on Python 3.13 or newer yet.
```bash
python3.12 -m venv .venv-spark && . .venv-spark/bin/activate
pip install "pyspark==3.5.*" "delta-spark==3.2.*"
python local/local_delta.py
ls /tmp/mart233_customers/_delta_log
```
The `_delta_log` folder holds one JSON file per write. That log is what gives you history and time travel.

## Common problems

* **"No compute attached"**: click Connect at the top of the notebook and pick Serverless.
* **Model stuck at PENDING_REGISTRATION**: Free Edition blocks the model upload to Unity Catalog storage. Use the model from Experiments instead.
* **Table not found**: check you ran `01_ingest` first and that the catalog is `workspace`.
* **App cannot read the tables**: the service principal grants in step 3 of the app section are missing.
* **Shop shows only two shoppers**: on purpose. Ama and Kofi are the demo accounts; the other 1,998 customers are background data.
* **App cannot reach Lakebase**: check `PGHOST` and `PGUSER` appear in the app's environment; if not, add them in `app.yaml`.
* **`PYTHON_VERSION_MISMATCH` or `PicklingError` from local Spark**: you are on Python 3.13 or newer. Make the venv with `python3.12`.
