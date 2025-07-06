import streamlit as st
import asyncio
from dotenv import load_dotenv

# Import our SQL agent runner
from main import run_sql_agent

# Load environment variables
load_dotenv()

def main():
    st.set_page_config(page_title="SQL Assistant", layout="centered")
    st.title("Intent-Based Text2SQL Engine")
    st.write(
        "Enter your natural language query below and let the agent translate it into a SQL query and execute it."
    )
    st.markdown("""
    Examples:
    - "Show me all products with price less than 100"
    - "What is the average stock of products?"
    - "Give me the price of the most expensive product"
    """)

    # Text input for user query
    user_input = st.text_area("Your Query", height=150)

    # Button to submit
    if st.button("Run Query"):
        if not user_input.strip():
            st.warning("Please enter a query to proceed.")
        else:
            # Run the SQL agent asynchronously
            with st.spinner("Processing your query..."):
                try:
                    result = asyncio.run(run_sql_agent(user_input))
                except Exception as e:
                    st.error(f"Error running agent: {e}")
                    return

            # Display result
            if "Failed security check" in result or "Security breach" in result:
                st.error(result)
            elif "Please ask question relevant to a Db query" in result:
                st.info(result)
            else:
                st.success("Query executed successfully:")
                st.code(result)

if __name__ == "__main__":
    main()
