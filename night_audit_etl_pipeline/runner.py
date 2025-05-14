from sqlalchemy import create_engine
import pandas as pd

# Confirm engine creation
engine = create_engine("mysql+pymysql://root:Sandeep%40123@localhost:3306/dev_night_audit")
print("🔍 Engine object type:", type(engine))
print("🔍 Engine dialect:", engine.dialect)
print("🔍 Dialect name:", engine.dialect.name)

df = pd.DataFrame({"col1": [1], "col2": ["test"]})
try:
    df.to_sql("test_table", con=engine, if_exists="append", index=False)
    print("✅ Data inserted successfully")
except Exception as e:
    print("❌ Exception:", type(e).__name__, "-", str(e))
