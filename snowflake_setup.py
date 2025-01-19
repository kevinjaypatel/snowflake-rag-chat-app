from dotenv import load_dotenv
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.session import Session
import traceback
import uuid
import os
load_dotenv()

connection_params = {
  "account":  os.getenv("SNOWFLAKE_ACCOUNT"),
  "user": os.getenv("SNOWFLAKE_USER"),
  "password": os.getenv("SNOWFLAKE_USER_PASSWORD"),
  "role": os.getenv("SNOWFLAKE_ROLE"),
  "database": os.getenv("SNOWFLAKE_DATABASE"),
  "schema": os.getenv("SNOWFLAKE_SCHEMA"),
  "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE")
}

def get_snowpark_session() -> Session:
    # Try to get existing active session first
    try:
        session = get_active_session()
        if session is not None:
            return session
    except:
        pass
        
    # Create new session only if no active session exists
    session_id = str(uuid.uuid4())[:8]
    print(f"\nCreating new Snowpark session (ID: {session_id})")
    print("Called from:")
    for line in traceback.format_stack()[:-1]:
        if "main.py" in line:
            print(line.strip())
            
    return Session.builder.configs(connection_params).create()

