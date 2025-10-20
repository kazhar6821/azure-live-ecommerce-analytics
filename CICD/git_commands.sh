mkdir realtime-ecommerce-insights-azure
cd realtime-ecommerce-insights-azure
git init
mkdir simulator eventhub databricks_notebooks storage powerbi cicd
touch README.md requirements.txt
git add .
git commit -m "Initial folder structure for Azure e-commerce insights project"


cd simulator
git add generate_orders.py
git commit -m "Add real-time e-commerce order simulator script"


cd databricks_notebooks
git add 01-streamorders-to-bronze.py
cd ..
git commit -m "Bronze layer streaming for U.S. order data"

cd databricks_notebooks
git add 02_clean_to_silver.py
cd ..
git commit -m "Cleaned and enriched Bronze data into Silver layer for U.S. cities"

cd databricks_notebooks
git add 03_aggregate_to_gold.py
cd ..
git commit -m "Aggregated Silver data to Gold layer for U.S. states (minute totals)"

cd Power\ BI
touch README.md
echo "# Power BI Dashboard for Real-Time E-Commerce Insights (U.S.)" > README.md
git add Power\ BI
git commit -m "Power BI dashboard with U.S. state-wise forecasting from Gold layer"