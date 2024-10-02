import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery

# Streamlit secrets configuration for GCP credentials
# Make sure you set up the `gcp_service_account` in the Streamlit secrets
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)

# Initialize the BigQuery client with the credentials from Streamlit secrets
client = bigquery.Client(credentials=credentials)

# Function to execute the BigQuery query
def run_query(query):
    query_job = client.query(query)
    result = query_job.result()
    return result

# Streamlit UI layout
st.title("Audiobook Search App")

# User input for keyword search
search_term = st.text_input("Enter search term (e.g., title, author, category, etc.)")

# Categories input - multi-select for categories (customize categories list)
categories_list = ["Fiction", "Non-Fiction", "Mystery", "Romance", "Sci-Fi"]  # Example categories
selected_categories = st.multiselect("Select Categories", categories_list)

# Construct the query dynamically based on the input
def generate_query(search_term, selected_categories):
    search_term = search_term.strip().lower()  # Clean up the search term
    
    # Start the base query
    query = f"""
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
      `mybots-397304.audiobookbay.atom_feed` AS atom_feed
    WHERE
      title IS NOT NULL
    """
    
    # Handle category selection
    if selected_categories:
        categories_regex = "|".join([f"\\b{category}\\b" for category in selected_categories])
        query += f" AND REGEXP_CONTAINS(ARRAY_TO_STRING(categories, ', '), r'(?i)({categories_regex})')"

    # Handle search term
    if search_term:
        query += f"""
        AND (
          LOWER(title) LIKE '%{search_term}%'
          OR LOWER(ARRAY_TO_STRING(authors, ', ')) LIKE '%{search_term}%'
          OR LOWER(ARRAY_TO_STRING(categories, ', ')) LIKE '%{search_term}%'
          OR LOWER(user) LIKE '%{search_term}%'
        )
        """
    
    # Add ordering and limit
    query += " ORDER BY update_time DESC LIMIT 20"
    
    return query

# Generate the SQL query based on the user inputs
query = generate_query(search_term, selected_categories)

# Search action
if st.button("Search"):
    results = run_query(query)
    
    # Display the results in a table
    if results.total_rows > 0:
        st.write(f"Showing results for: '{search_term}' and categories: {', '.join(selected_categories)}")
        for row in results:
            st.write(f"**Title**: {row['Book Title']}")
            st.write(f"**Author**: {row['Author']}")
            st.write(f"**Category**: {row['Category']}")
            st.write(f"**Summary**: {row['Summary']}")
            st.write(f"[Link to Book]({row['Link']})")
            st.write("---")
    else:
        st.write("No results found")
