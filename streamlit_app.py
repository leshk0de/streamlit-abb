import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery

# Streamlit secrets configuration for GCP credentials
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)

# Initialize the BigQuery client with the credentials from Streamlit secrets
client = bigquery.Client(credentials=credentials)

# Constants for pagination
RESULTS_PER_PAGE = 10

# Function to log and execute the BigQuery query
def run_query(query, parameters=None):
    # Log the query and parameters
    print("Executing query:")
    print(query)
    if parameters:
        print("With parameters:")
        for param in parameters:
            print(f"{param.name}: {param.value}")

    # Run the query
    query_job = client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=parameters
    ))
    result = query_job.result()
    return result

# Streamlit UI layout
st.title("Audiobook Search App")

# User input for keyword search
search_term = st.text_input("Enter search term (e.g., title, author, category, etc.)")

# Categories input - multi-select for categories (customize categories list)
categories_list = ["Fiction", "Non-Fiction", "Mystery", "Romance", "Sci-Fi"]  # Example categories
selected_categories = st.multiselect("Select Categories", categories_list)

# Page selection for pagination
page_number = st.number_input("Page Number", min_value=1, step=1)

# Construct the query dynamically based on the input
def generate_query(search_term, selected_categories, page_number):
    offset = (page_number - 1) * RESULTS_PER_PAGE
    
    # Base query
    query = """
    SELECT
      title AS `Book Title`,
      ARRAY_TO_STRING(authors, ', ') AS `Author`,
      ARRAY_TO_STRING(categories, ', ') AS `Category`,
      TRIM(
        REGEXP_REPLACE(
          summary,
          r'(?i)^Shared by:[^\n]*\s*\n+',
          ''
        )
      ) AS `Summary`,
      link AS `Link`,
      update_time AS `Record Updated`,
      publication_time AS `Published`,
      user AS `Uploaded By`
    FROM
      `mybots-397304.audiobookbay.atom_feed`
    WHERE
      title IS NOT NULL
    """
    
    # Parameters for the query
    parameters = []
    
    # Handle category selection
    if selected_categories:
        categories_pattern = "|".join(selected_categories)
        query += " AND REGEXP_CONTAINS(ARRAY_TO_STRING(categories, ', '), @categories)"
        parameters.append(bigquery.ScalarQueryParameter("categories", "STRING", categories_pattern))
    
    # Handle search term
    if search_term:
        query += """
        AND (
          LOWER(title) LIKE @search_term
          OR LOWER(ARRAY_TO_STRING(authors, ', ')) LIKE @search_term
          OR LOWER(ARRAY_TO_STRING(categories, ', ')) LIKE @search_term
          OR LOWER(user) LIKE @search_term
        )
        """
        search_term_pattern = f"%{search_term.lower()}%"
        parameters.append(bigquery.ScalarQueryParameter("search_term", "STRING", search_term_pattern))
    
    # Add ordering, limit, and offset for pagination
    query += f" ORDER BY update_time DESC LIMIT {RESULTS_PER_PAGE} OFFSET {offset}"
    
    return query, parameters

# Generate the SQL query based on the user inputs
query, parameters = generate_query(search_term, selected_categories, page_number)

# Default view action (load latest data)
st.write(f"Displaying page {page_number} (10 results per page):")
try:
    # Print out the query and parameters before running it
    run_query(query, parameters)
    
    # Execute the query and display the results
    results = run_query(query, parameters)
    
    # Display the results in a table
    if results.total_rows > 0:
        for row in results:
            st.write(f"**Title**: {row['Book Title']}")
            st.write(f"**Author**: {row['Author']}")
            st.write(f"**Category**: {row['Category']}")
            st.write(f"**Summary**: {row['Summary']}")
            st.write(f"[Link to Book]({row['Link']})")
            st.write("---")
    else:
        st.write("No results found")
except Exception as e:
    st.error(f"An error occurred: {e}")

# Pagination controls
if st.button("Next Page"):
    st.session_state.page_number = page_number + 1

if st.button("Previous Page") and page_number > 1:
    st.session_state.page_number = page_number - 1
