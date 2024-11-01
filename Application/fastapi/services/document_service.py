async def get_document_by_selection(selection: DocumentSelection) -> DocumentInfo:
    snowflake_client = SnowflakeClient()
    document = snowflake_client.fetch_document_by_id(selection.document_id)
    if not document:
        raise ValueError("Document not found")
    return DocumentInfo(**document)