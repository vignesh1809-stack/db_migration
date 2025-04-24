import json
import pymysql
import pandas as pd
import sys
import os

def classify_user(row):
    rights = str(row.get("Rights", "")).strip().lower()
    user_id = str(row.get("UniqueID", "")).strip().lower()

    if rights == "superadmin" and user_id == "superuser":
        user_role = "Admin"
    elif rights == "staff" and user_id == "superuser":
        user_role = "TeamLead"
    elif rights == "staff" and not user_id:
        user_role = "Staff"
    elif "lawyer" in rights:
        user_role = "Lawyer"
    elif "attorney" in rights:
        user_role = "Attorney"
    elif "provider" in rights:
        user_role = "Provider"
    else:
        user_role = "Staff"  # default fallback

    user_type = "External" if user_role in ["Lawyer", "Attorney", "Provider"] else "Internal"
    return pd.Series([user_role, user_type])

def validate_config(config):
    required_keys = ["source", "destination", "table"]
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing key in config: '{key}'")
    print("✅ Configuration validated.")

def connect_db(config, label):
    print(f"🔌 Connecting to {label} database...")
    try:
        conn = pymysql.connect(
            host=config["host"],
            user=config["user"],
            password=config["password"],
            database=config["database"]
        )
        print(f"✅ Connected to {label} DB.")
        return conn
    except Exception as e:
        raise Exception(f"❌ Failed to connect to {label} DB: {e}")

def ensure_columns_exist(conn, table_name, columns):
    cursor = conn.cursor()
    cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
    existing_columns = [row[0] for row in cursor.fetchall()]

    for col_name, col_type in columns.items():
        if col_name not in existing_columns:
            cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{col_name}` {col_type}")
            conn.commit()
            print(f"🛠️ Added missing column: {col_name}")
        else:
            print(f"✅ Column '{col_name}' already exists.")
    cursor.close()

def drop_columns_if_exist(conn, table_name, columns):
    cursor = conn.cursor()
    for col in columns:
        cursor.execute(f"SHOW COLUMNS FROM `{table_name}` LIKE '{col}'")
        if cursor.fetchone():
            cursor.execute(f"ALTER TABLE `{table_name}` DROP COLUMN `{col}`")
            conn.commit()
            print(f"🗑️ Dropped column: {col}")
        else:
            print(f"⚠️ Column '{col}' not found in the destination table.")
    cursor.close()

def migrate_users(config_path="config.json"):
    try:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"❌ Config file '{config_path}' not found.")

        print("📄 Loading config...")
        with open(config_path, "r") as file:
            config = json.load(file)

        validate_config(config)

        source_config = config["source"]
        destination_config = config["destination"]
        table_name = config["table"]

        # Connect to source and fetch data
        src_conn = connect_db(source_config, "source")
        df = pd.read_sql(f"SELECT * FROM `{table_name}`", src_conn)
        src_conn.close()
        print(f"📥 Retrieved {len(df)} rows from source.")

        # Classify users
        print("🔄 Classifying users...")
        df[["UserRole_Temp", "UserType"]] = df.apply(classify_user, axis=1)
        print("✅ Classification complete.")

        # Replace Doctor column with UserRole
        if "Doctor" in df.columns:
            df["Doctor"] = df["UserRole_Temp"]
            df.rename(columns={"Doctor": "UserRole"}, inplace=True)
            df.drop(columns=["UserRole_Temp"], inplace=True)
            print("🔁 Replaced 'Doctor' column with 'UserRole'.")
        else:
            raise Exception("❌ 'Doctor' column not found in the source table.")

        # Drop Rights, UniqueID, and Doctor columns from the DataFrame
        for col in ["Rights", "UniqueID", "Doctor"]:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)
                print(f"🗑️ Dropped column: {col}")
            else:
                print(f"⚠️ Column '{col}' not found in data.")

        # Ensure final column order
        if "UserType" in df.columns:
            cols = [col for col in df.columns if col not in ["UserRole", "UserType"]] + ["UserRole", "UserType"]
            df = df[cols]

        # Connect to destination DB
        dst_conn = connect_db(destination_config, "destination")

        # Drop columns Rights, UniqueID, and Doctor from the destination table
        drop_columns_if_exist(dst_conn, table_name, ["Rights", "UniqueID", "Doctor"])

        # Ensure required columns exist in the destination table
        ensure_columns_exist(dst_conn, table_name, {"UserRole": "VARCHAR(50)", "UserType": "VARCHAR(50)"})

        # Insert or update records
        cursor = dst_conn.cursor()
        cols = ", ".join(f"`{col}`" for col in df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_query = f"REPLACE INTO `{table_name}` ({cols}) VALUES ({placeholders})"

        print(f"📤 Inserting data into destination table '{table_name}'...")
        for _, row in df.iterrows():
            cursor.execute(insert_query, tuple(row))

        dst_conn.commit()
        cursor.close()
        dst_conn.close()
        print("🎉 Data migration and transformation completed successfully.")

    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

# 🚀 Entry point
if __name__ == "__main__":
    migrate_users("config.json")
