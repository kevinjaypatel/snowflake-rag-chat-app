from snowflake_setup import get_snowpark_session

try: 
    session = get_snowpark_session()
    print("Connection successful!")
except Exception as e:
    print("Connection failed:", str(e))
    