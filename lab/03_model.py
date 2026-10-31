# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Model: predict a shopper's favourite category
# MAGIC Trains a small scikit-learn model on the customer profiles, logs it to MLflow
# MAGIC and registers it in Unity Catalog.

# COMMAND ----------

import mlflow
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

mlflow.set_registry_uri("databricks-uc")
MODEL_NAME = "workspace.shop.favourite_category"

df = (
    spark.table("workspace.shop.gold_customer_preferences")
    .select("city", "age_band", "preferred_payment", "favourite_category")
    .toPandas()
)

X = pd.get_dummies(df.drop(columns="favourite_category"), dtype=int)
y = df["favourite_category"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# COMMAND ----------

with mlflow.start_run(run_name="rf-favourite-category"):
    params = {"n_estimators": 200, "max_depth": 6, "random_state": 42}
    model = RandomForestClassifier(**params).fit(X_train, y_train)
    accuracy = accuracy_score(y_test, model.predict(X_test))

    mlflow.log_params(params)
    mlflow.log_metric("accuracy", accuracy)
    logged = mlflow.sklearn.log_model(model, artifact_path="model", input_example=X_test.head(5))

# Six categories, so guessing at random scores about 17%
print(f"Accuracy: {accuracy:.0%}")

# COMMAND ----------

# Register the model in Unity Catalog. Some Free Edition workspaces block this step,
# so a failure here is reported rather than stopping the notebook.
try:
    version = mlflow.register_model(logged.model_uri, MODEL_NAME)
    print(f"Registered {MODEL_NAME} version {version.version}")
except Exception as e:
    print(f"Could not register the model in Unity Catalog: {type(e).__name__}. The run and model are still in Experiments.")

# COMMAND ----------

# MAGIC %md
# MAGIC Age band carries most of the signal, because that is how the data was generated.
# MAGIC Change `params`, run again, then compare the runs in the Experiments page.
