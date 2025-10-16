from sshtunnel import SSHTunnelForwarder
import psycopg2
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBRegressor
from sklearn.metrics import r2_score, root_mean_squared_error
from dotenv import load_dotenv
import os

########################################################
# Configuration
########################################################
load_dotenv()
SSH_HOST = os.getenv("SSH_HOST")
SSH_PORT = int(os.getenv("SSH_PORT"))
SSH_USER = os.getenv("SSH_USER")
SSH_PASSWORD = os.getenv("SSH_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT"))
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

########################################################
# Fetch data
########################################################
with open("query.sql", "r") as f:
    SQL = f.read()

with SSHTunnelForwarder(
    (SSH_HOST, SSH_PORT),
    ssh_username=SSH_USER,
    ssh_password=SSH_PASSWORD,
    remote_bind_address=(DB_HOST, DB_PORT),
    local_bind_address=('127.0.0.1',)
) as tunnel:
    
    with psycopg2.connect(
        host='127.0.0.1',
        port=tunnel.local_bind_port,
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_NAME
    ) as conn:
        
        with conn.cursor() as cur:
            cur.execute(SQL)
            df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

########################################################
# Model Training
########################################################

# Label encoding for categorical features
le = LabelEncoder()
df["expense_type"] = le.fit_transform(df["expense_type"])

# Define predictor and target
X = df[["discount_percentage", "frequency", "expense_type", "rating"]]
y = df["expense_amount"]

# Data partitioning
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Training
model = XGBRegressor(
    n_estimators=300,
    learning_rate=0.08,
    max_depth=6,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=1999
)
model.fit(X_train, y_train)

# Evaluation
y_pred = model.predict(X_test)
print("R²:", r2_score(y_test, y_pred))
print("RMSE:", root_mean_squared_error(y_test, y_pred))
