import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

# Set the page configuration to wide mode
st.set_page_config(layout="wide")

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
    try:
        query_job = client.query(query, job_config=bigquery.QueryJobConfig(
            query_parameters=parameters
        ))
        return query_job.result()
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return None

# Streamlit UI layout
st.title("Audiobook Search App")

# Initialize page number in session state
if "page_number" not in st.session_state:
    st.session_state.page_number = 1

# Initialize selected row in session state
if "selected_row" not in st.session_state:
    st.session_state.selected_row = None

# Capture previous search term and categories to check for changes
previous_search_term = st.session_state.get('previous_search_term', "")
previous_categories = st.session_state.get('previous_categories', [])

# Layout: search and categories in the same row
col1, col2 = st.columns([2, 2])

# User input for keyword search
with col1:
    search_term = st.text_input("Enter search term (e.g., title, author, category, etc.)", value=previous_search_term)

# Categories input - multi-select for categories (customize categories list)
categories_list = ["Fiction", "Non-Fiction", "Mystery", "Romance", "Sci-Fi"]  # Example categories
with col2:
    selected_categories = st.multiselect("Select Categories", categories_list, default=previous_categories)

# Reset page number if search term or selected categories have changed
if search_term != previous_search_term or selected_categories != previous_categories:
    st.session_state.page_number = 1

# Store the current search term and categories for future comparison
st.session_state['previous_search_term'] = search_term
st.session_state['previous_categories'] = selected_categories

# Layout for page selection and pagination buttons aligned to the right
col_prev, col_page_info, col_next = st.columns([1, 3, 1])

with col_prev:
    if st.button("Previous Page") and st.session_state.page_number > 1:
        st.session_state.page_number -= 1
        # Reset the selected row when page changes
        st.session_state.selected_row = None

# Show current page number
page_number = st.session_state.page_number

with col_next:
    if st.button("Next Page"):
        st.session_state.page_number += 1
        # Reset the selected row when page changes
        st.session_state.selected_row = None

# Construct the COUNT query dynamically based on the input
def generate_count_query(search_term, selected_categories):
    # Base query for counting total results
    query = """
    SELECT COUNT(*) AS total_results
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
        )
        """
        search_term_pattern = f"%{search_term.lower()}%"
        parameters.append(bigquery.ScalarQueryParameter("search_term", "STRING", search_term_pattern))
    
    return query, parameters

# Construct the data query dynamically based on the input
def generate_data_query(search_term, selected_categories, page_number):
    offset = (page_number - 1) * RESULTS_PER_PAGE
    
    # Base query with YYYY-MM-DD format for `Published` and `Updated`
    query = """
    SELECT
      title AS `Book Title`,
      ARRAY_TO_STRING(authors, ', ') AS `Author`,
      ARRAY_TO_STRING(categories, ', ') AS `Category`,
      TRIM(
        REGEXP_REPLACE(summary, r'(?i)^Shared by:.*?\s*\\n*', '' )
      ) AS `Summary`,
      link AS `Link`,
      FORMAT_TIMESTAMP('%Y-%m-%d', update_time) AS `Updated`,
      FORMAT_TIMESTAMP('%Y-%m-%d', publication_time) AS `Published`
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
        )
        """
        search_term_pattern = f"%{search_term.lower()}%"
        parameters.append(bigquery.ScalarQueryParameter("search_term", "STRING", search_term_pattern))
    
    # Add ordering, limit, and offset for pagination
    query += f" ORDER BY update_time DESC LIMIT {RESULTS_PER_PAGE} OFFSET {offset}"
    
    return query, parameters

# Execute the COUNT query to get total results
count_query, count_params = generate_count_query(search_term, selected_categories)
count_results = run_query(count_query, count_params)

# Safely handle the count results
if count_results:
    total_results = list(count_results)[0].total_results  # Extract the total results from the query
else:
    total_results = 0

# Display total number of potential results
with col_page_info:
    st.write(f"Page {st.session_state.page_number} of {total_results // RESULTS_PER_PAGE + 1} | Total Results: {total_results}")

# Generate and execute the data query
data_query, data_params = generate_data_query(search_term, selected_categories, page_number)
results = run_query(data_query, data_params)

# Safely handle the data query results
if results:
    # Prepare data for display in a table
    rows = []
    for idx, row in enumerate(results):
        rows.append({
            'Title': row['Book Title'],
            'Author': row['Author'],
            'Category': row['Category'],
            'Published': row['Published'],
            'Updated': row['Updated'],
            'Link': row['Link'],  # Keep raw link for the button
            'Summary': row['Summary'],  # Keep this for the detailed view
        })

    # Create DataFrame
    df = pd.DataFrame(rows)

    # Table headers using Streamlit's layout system for real checkboxes
    col1, col2, col3, col4, col5 = st.columns([0.2, 2, 2, 2, 2])  # Adjust column widths for checkboxes
    with col1:
        st.write("")  # No header for checkbox column
    with col2:
        st.write("Title")
    with col3:
        st.write("Author")
    with col4:
        st.write("Category")
    with col5:
        st.write("Published")

    # Display table with checkboxes and data
    for idx, row in df.iterrows():
        # Insert divider line between rows
        if idx > 0:
            st.markdown("<hr>", unsafe_allow_html=True)  # Add horizontal divider
        
        col1, col2, col3, col4, col5 = st.columns([0.2, 2, 2, 2, 2])  # Adjust column widths
        
        # Add checkbox
        with col1:
            checkbox = st.checkbox("", key=f"checkbox_{idx}", value=(st.session_state.selected_row == idx))
        
        # Display row data
        with col2:
            st.write(row['Title'])
        with col3:
            st.write(row['Author'])
        with col4:
            st.write(row['Category'])
        with col5:
            st.write(row['Published'])

        # Handle checkbox selection
        if checkbox:
            st.session_state.selected_row = idx
        elif st.session_state.selected_row == idx:
            st.session_state.selected_row = None

    # Display sidebar with book details if a checkbox is selected
    if st.session_state.selected_row is not None:
        selected_row = df.iloc[st.session_state.selected_row]
        with st.sidebar:
            st.write(f"### Detailed View for {selected_row['Title']}")
            st.write(f"**Summary**: {selected_row['Summary']}")
            st.write(f"**Category**: {selected_row['Category']}")
            st.write(f"**Published**: {selected_row['Published']}")
            st.write(f"**Updated**: {selected_row['Updated']}")
            st.write(f"[Click to visit ABB link]({selected_row['Link']})")

else:
    st.write("No results found.")

