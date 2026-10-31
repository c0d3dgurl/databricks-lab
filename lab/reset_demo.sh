#!/usr/bin/env sh
# Clear everything shoppers did in the Mart 233 app, then rebuild the tables,
# so Ama and Kofi start from their generated history again.
set -e
databricks psql --project mart233 -- -c "DELETE FROM shop.order_items; DELETE FROM shop.orders; DELETE FROM shop.basket; DELETE FROM shop.likes; DELETE FROM shop.interests;"
databricks bundle run shop_pipeline
