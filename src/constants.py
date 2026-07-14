"""Static text fragments and defaults used across the Streamlit app."""

from __future__ import annotations

DATASET_URL = "https://www.kaggle.com/datasets/vivek468/superstore-dataset-final"

DEFAULT_EXAMPLES = (
    "### Example questions\n\n"
    "- fetch total orders across years\n"
    "- fetch top 5 states with most number of sales\n"
    "- show total sales and profit for the furniture category\n"
)

DEFAULT_SCHEMA = (
    "#### Dataset: sample.super_store_data\n\n"
    "A retail dataset of orders, customers, products, and sales metrics.\n\n"
    "| column         | type   | description                          |\n"
    "|----------------|--------|--------------------------------------|\n"
    "| row_id         | int    | unique row identifier                 |\n"
    "| order_id       | string | unique order identifier               |\n"
    "| order_date     | string | order date (M/D/YYYY)                 |\n"
    "| ship_date      | string | shipment date (M/D/YYYY)              |\n"
    "| ship_mode      | string | shipment method                       |\n"
    "| customer_id    | string | unique customer identifier            |\n"
    "| customer_name  | string | customer full name                    |\n"
    "| segment        | string | customer segment                      |\n"
    "| country        | string | country                               |\n"
    "| city           | string | city                                  |\n"
    "| state          | string | state/province                        |\n"
    "| postal_code    | string | postal/zip code                       |\n"
    "| region         | string | sales region                          |\n"
    "| product_id     | string | unique product identifier             |\n"
    "| category       | string | product category                      |\n"
    "| sub_category   | string | product sub-category                  |\n"
    "| product_name   | string | product display name                  |\n"
    "| sales          | double | sales amount                          |\n"
    "| quantity       | int    | quantity sold                         |\n"
    "| discount       | double | discount applied (0-1)                |\n"
    "| profit         | double | profit amount                         |\n"
)

