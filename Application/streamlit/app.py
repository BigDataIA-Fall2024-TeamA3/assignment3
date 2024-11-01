import streamlit as st
import requests
import pandas as pd

st.set_page_config(layout="wide")
PROCESS_FILES_URL = "http://localhost:8000/process_files/"
PROCESS_DIR_URL = "http://localhost:8000/process_directory/"
PROCESS_QUERY_URL = "http://localhost:8000/query"
LIST_DOCUMENTS_URL = "http://localhost:8000/list_documents_info"

# @st.cache_data
def fetch_document_info():
    response = requests.get(LIST_DOCUMENTS_URL)
    if response.status_code == 200:
        return pd.DataFrame(response.json())
    else:
        st.error("Failed to fetch document information")
        return pd.DataFrame()

def main():
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.title("Collections")
        
        input_method = st.radio("Choose input method:", ("Upload Files", "Enter Directory Path", "Choose from Online List"))
        
        if input_method == "Upload Files":
            uploaded_files = st.file_uploader("Drag and drop files here", accept_multiple_files=True)
            if uploaded_files and st.button("Process Files"):
                with st.spinner("Processing files..."):
                    files = [("files", file) for file in uploaded_files]
                    response = requests.post(PROCESS_FILES_URL, files=files)
                    if response.status_code == 200:
                        st.success("Files processed and index created!")
                    else:
                        st.error("Error processing files.")
        elif input_method == "Enter Directory Path":
            directory_path = st.text_input("Enter directory path:")
            if directory_path and st.button("Process Directory"):
                with st.spinner("Processing directory..."):
                    response = requests.post(PROCESS_DIR_URL, json={"directory_path": directory_path})
                    if response.status_code == 200:
                        st.success("Directory processed and index created!")
                    else:
                        st.error("Error processing directory.")
        else:  # Choose from the sf Online List
            df = fetch_document_info()
            if not df.empty:
                st.write("Select a document:")
                for i, row in df.iterrows():
                    col1, col2 = st.columns([1, 3])
                    with col1:
                        st.image(row['document_cover_image_link'], width=100)
                    with col2:
                        st.write(f"**{row['document_name']}**")
                        st.write(row['summary'][:100] + "..." if len(row['summary']) > 100 else row['summary'])
                        if st.button(f"Select {row['document_name']}", key=f"select_{i}"):
                            with st.spinner(f"Processing {row['document_name']}..."):
                                response = requests.post(PROCESS_FILES_URL, json={
                                    "document_name": row['document_name'],
                                    "s3_pdf_link": row['s3_pdf_link']
                                })
                                if response.status_code == 200:
                                    st.success(f"{row['document_name']} processed and added to index!")
                                else:
                                    st.error(f"Error processing {row['document_name']}.")
    
    with col2:
        st.title("Chat")
        if 'history' not in st.session_state:
            st.session_state['history'] = []
        
        user_input = st.chat_input("Enter your query:")

        chat_container = st.container()
        with chat_container:
            for message in st.session_state['history']:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])

        if user_input:
            with st.chat_message("user"):
                st.markdown(user_input)
            st.session_state['history'].append({"role": "user", "content": user_input})
            
            with st.chat_message("assistant"):
                message_placeholder = st.empty()
                full_response = ""
                response = requests.post(PROCESS_QUERY_URL, json={"query": user_input})
                if response.status_code == 200:
                    full_response = response.json()["response"]
                    message_placeholder.markdown(full_response)
                else:
                    message_placeholder.markdown("Error querying the index.")
            st.session_state['history'].append({"role": "assistant", "content": full_response})

        if st.button("Clear Chat"):
            st.session_state['history'] = []
            st.rerun()

if __name__ == "__main__":
    main()