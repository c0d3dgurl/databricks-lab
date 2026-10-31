"""Mart 233: a shop built as a Databricks App.

The catalogue and customer profiles come from Delta tables (through the SQL warehouse).
Everything a shopper does, such as interests, wishlist, basket and orders, is saved to Lakebase (Postgres).
"""
import json
import os
import time
from contextlib import contextmanager

import psycopg
import uvicorn
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from pydantic import BaseModel

cfg = Config()  # the app's service principal, injected by Databricks Apps
ws = WorkspaceClient()
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]
DEMO_ACCOUNTS = (1, 2)  # Ama Mensah and Kofi Boateng
CATEGORIES = ["Phones", "Fashion", "Electronics", "Home", "Groceries", "Beauty"]

app = FastAPI(title="Mart 233")


# ---------- Delta: read through the SQL warehouse, cached for five minutes ----------

_cache = {}


def delta(statement: str, params: dict | None = None, ttl: int = 300) -> list[dict]:
    key = (statement, tuple(sorted((params or {}).items())))
    hit = _cache.get(key)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: cfg.authenticate,
    ) as conn, conn.cursor() as cur:
        cur.execute(statement, params)  # values are sent as bound parameters, never pasted into the SQL
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    _cache[key] = (time.time(), rows)
    return rows


def products() -> dict[int, dict]:
    rows = delta("SELECT product_id, product_name, category, price_ghs, orders, avg_rating "
                 "FROM workspace.shop.gold_products ORDER BY orders DESC")
    return {r["product_id"]: r for r in rows}


def profile(customer_id: int) -> dict:
    if customer_id not in DEMO_ACCOUNTS:
        raise HTTPException(404, "Not a demo account")
    rows = delta("SELECT customer_id, name, city, age_band, favourite_category, total_spend_ghs, "
                 "preferred_payment, persona, spend_tier FROM workspace.shop.gold_personas "
                 "WHERE customer_id = :customer_id", {"customer_id": customer_id})
    return rows[0]


# ---------- Lakebase: Postgres, with a short-lived OAuth token as the password ----------

@contextmanager
def pg():
    with psycopg.connect(
        host=os.environ["PGHOST"],
        dbname=os.environ.get("PGDATABASE", "databricks_postgres"),
        user=os.environ["PGUSER"],
        password=ws.config.oauth_token().access_token,
        port=os.environ.get("PGPORT", "5432"),
        sslmode="require",
        row_factory=dict_row,
    ) as conn:
        yield conn


with pg() as conn:
    conn.execute("CREATE SCHEMA IF NOT EXISTS shop")
    conn.execute("""CREATE TABLE IF NOT EXISTS shop.likes (
        id SERIAL PRIMARY KEY, customer_id BIGINT NOT NULL, customer_name TEXT NOT NULL,
        product_id INT NOT NULL, product_name TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT now())""")
    conn.execute("""CREATE TABLE IF NOT EXISTS shop.interests (
        customer_id BIGINT, category TEXT, PRIMARY KEY (customer_id, category))""")
    conn.execute("""CREATE TABLE IF NOT EXISTS shop.basket (
        customer_id BIGINT, product_id INT, quantity INT NOT NULL,
        updated_at TIMESTAMPTZ DEFAULT now(), PRIMARY KEY (customer_id, product_id))""")
    conn.execute("""CREATE TABLE IF NOT EXISTS shop.orders (
        id SERIAL PRIMARY KEY, customer_id BIGINT NOT NULL, customer_name TEXT NOT NULL,
        items JSONB NOT NULL, total_ghs NUMERIC(12, 2) NOT NULL, created_at TIMESTAMPTZ DEFAULT now())""")
    # One row per product bought. The pipeline reads this through Unity Catalog
    # and adds it to the order history, so app purchases change the gold tables.
    conn.execute("""CREATE TABLE IF NOT EXISTS shop.order_items (
        id SERIAL PRIMARY KEY, order_id INT NOT NULL REFERENCES shop.orders (id),
        customer_id BIGINT NOT NULL, product_id INT NOT NULL, quantity INT NOT NULL,
        price_ghs NUMERIC(12, 2) NOT NULL, payment TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT now())""")


# ---------- API ----------

class Interests(BaseModel):
    categories: list[str]


class Quantity(BaseModel):
    quantity: int


@app.get("/api/accounts")
def accounts():
    return [profile(cid) for cid in DEMO_ACCOUNTS]


@app.get("/api/products")
def list_products():
    return list(products().values())


@app.get("/api/customers/{cid}")
def customer(cid: int):
    me = profile(cid)
    with pg() as conn:
        interests = [r["category"] for r in conn.execute(
            "SELECT category FROM shop.interests WHERE customer_id = %s", (cid,))]
        liked = [r["product_id"] for r in conn.execute(
            "SELECT product_id FROM shop.likes WHERE customer_id = %s", (cid,))]
        basket = conn.execute(
            "SELECT product_id, quantity FROM shop.basket WHERE customer_id = %s ORDER BY updated_at", (cid,)
        ).fetchall()
        orders = conn.execute(
            "SELECT id, total_ghs, created_at FROM shop.orders WHERE customer_id = %s ORDER BY id DESC LIMIT 5",
            (cid,),
        ).fetchall()

    # Recommend the favourite category found in the data, then anything the shopper ticked
    wanted = [me["favourite_category"]] + [c for c in interests if c != me["favourite_category"]]
    recommended = [
        {**p, "reason": "You shop this most" if p["category"] == me["favourite_category"] else "You said you like this"}
        for c in wanted for p in products().values() if p["category"] == c
    ]
    return {"profile": me, "interests": interests, "liked": liked, "basket": basket,
            "orders": orders, "recommended": recommended[:8]}


def demo_only(cid: int):
    if cid not in DEMO_ACCOUNTS:
        raise HTTPException(403, "Only the demo accounts can make changes")


@app.put("/api/customers/{cid}/interests")
def set_interests(cid: int, body: Interests):
    demo_only(cid)
    with pg() as conn:
        conn.execute("DELETE FROM shop.interests WHERE customer_id = %s", (cid,))
        for c in set(body.categories) & set(CATEGORIES):
            conn.execute("INSERT INTO shop.interests VALUES (%s, %s)", (cid, c))
    return customer(cid)


@app.post("/api/customers/{cid}/likes/{pid}")
def toggle_like(cid: int, pid: int):
    demo_only(cid)
    p = products()[pid]
    with pg() as conn:
        if not conn.execute("DELETE FROM shop.likes WHERE customer_id = %s AND product_id = %s", (cid, pid)).rowcount:
            conn.execute(
                "INSERT INTO shop.likes (customer_id, customer_name, product_id, product_name) VALUES (%s, %s, %s, %s)",
                (cid, profile(cid)["name"], pid, p["product_name"]),
            )
    return customer(cid)


@app.put("/api/customers/{cid}/basket/{pid}")
def set_basket(cid: int, pid: int, body: Quantity):
    demo_only(cid)
    with pg() as conn:
        if body.quantity <= 0:
            conn.execute("DELETE FROM shop.basket WHERE customer_id = %s AND product_id = %s", (cid, pid))
        else:
            conn.execute(
                """INSERT INTO shop.basket (customer_id, product_id, quantity) VALUES (%s, %s, %s)
                   ON CONFLICT (customer_id, product_id)
                   DO UPDATE SET quantity = EXCLUDED.quantity, updated_at = now()""",
                (cid, pid, body.quantity),
            )
    return customer(cid)


@app.post("/api/customers/{cid}/checkout")
def checkout(cid: int):
    demo_only(cid)
    catalogue = products()
    with pg() as conn:
        basket = conn.execute("SELECT product_id, quantity FROM shop.basket WHERE customer_id = %s", (cid,)).fetchall()
        if not basket:
            raise HTTPException(400, "Basket is empty")
        items = [{"product": catalogue[b["product_id"]]["product_name"], "quantity": b["quantity"],
                  "price_ghs": catalogue[b["product_id"]]["price_ghs"]} for b in basket]
        total = sum(i["quantity"] * i["price_ghs"] for i in items)
        me = profile(cid)
        order_id = conn.execute(
            "INSERT INTO shop.orders (customer_id, customer_name, items, total_ghs) VALUES (%s, %s, %s, %s) RETURNING id",
            (cid, me["name"], json.dumps(items), total),
        ).fetchone()["id"]
        for b in basket:
            conn.execute(
                "INSERT INTO shop.order_items (order_id, customer_id, product_id, quantity, price_ghs, payment) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (order_id, cid, b["product_id"], b["quantity"], catalogue[b["product_id"]]["price_ghs"],
                 me["preferred_payment"]),
            )
        conn.execute("DELETE FROM shop.basket WHERE customer_id = %s", (cid,))
    return customer(cid)


@app.get("/api/manager")
def manager():
    totals = delta("SELECT sum(orders) AS orders, round(sum(revenue_ghs), 2) AS revenue_ghs FROM workspace.shop.gold_daily")[0]
    by_category = delta("SELECT category, sum(orders) AS orders, round(sum(revenue_ghs), 2) AS revenue_ghs "
                        "FROM workspace.shop.gold_daily GROUP BY category ORDER BY revenue_ghs DESC")
    by_city = delta("SELECT city, max_by(category, orders) AS top_category, max(orders) AS orders FROM "
                    "(SELECT city, category, sum(orders) AS orders FROM workspace.shop.gold_category_city GROUP BY ALL) "
                    "GROUP BY city ORDER BY city")
    with pg() as conn:
        live = conn.execute("""
            SELECT 'order' AS kind, customer_name, 'GHS ' || total_ghs::text AS detail, created_at FROM shop.orders
            UNION ALL
            SELECT 'like', customer_name, product_name, created_at FROM shop.likes
            ORDER BY created_at DESC LIMIT 12""").fetchall()
        counts = conn.execute("""SELECT
            (SELECT count(*) FROM shop.orders) AS orders,
            (SELECT coalesce(sum(total_ghs), 0) FROM shop.orders) AS revenue_ghs,
            (SELECT coalesce(sum(quantity), 0) FROM shop.basket) AS in_baskets""").fetchone()
    return {"history": {**totals, "by_category": by_category, "by_city": by_city}, "live": {**counts, "feed": live}}


app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def index():
    return FileResponse("static/index.html")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("DATABRICKS_APP_PORT", "8000")))
