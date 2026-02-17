import logging

logger = logging.getLogger(__name__)
table = "yt_api"

def sanitize_row(row, schema):
    """Convert null/None values and arrays to proper SQL values"""
    sanitized = {}
    
    for key, value in row.items():
        # Handle arrays with null values
        if isinstance(value, list):
            if value and value[0] is not None:
                sanitized[key] = value[0]
            else:
                sanitized[key] = None
        # Handle None values - keep as None for psycopg2 to convert to NULL
        elif value is None:
            sanitized[key] = None
        else:
            sanitized[key] = value
    
    return sanitized

def insert_rows(cur, conn, schema, row):

    try:
        # Sanitize row to handle null values and arrays
        row = sanitize_row(row, schema)

        if schema == 'staging':

            video_id = 'video_id'

            cur.execute(
                f"""INSERT INTO {schema}.{table}("video_ID", "Video_Title", "Upload_Date", "Duration", "Video Views", "Likes Count", "Comments_Count")
                VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s);
                """, row
            )

        else:

            video_id = 'video_id'

            cur.execute(
                f"""
                INSERT INTO {schema}.{table}("video_ID", "Video_Title", "Upload_Date", "Duration","Video_Type", "Video Views", "Likes Count", "Comments_Count")
                VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(Video_Type)s, %(viewCount)s, %(likeCount)s, %(commentCount)s);
                """, row
                
                )
        conn.commit()

        logger.info(f"Inserted row with Video_ID: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error inserting row with Video_ID: {row[video_id]}")
        raise e
    
def update_rows(cur, conn, schema, row):

    try:
        # Sanitize row to handle null values and arrays
        row = sanitize_row(row, schema)
        
        # staging
        if schema == 'staging':
            video_id = 'video_id'
            upload_date = 'publishedAt'
            video_title = 'title'
            video_views = 'viewCount'
            likes_count = 'likeCount'
            comments_count = 'commentCount'

        else:
            video_id = 'video_id'
            upload_date = 'publishedAt'
            video_title = 'title'
            video_views = 'viewCount'
            likes_count = 'likeCount'
            comments_count = 'commentCount'

        cur.execute(
            f"""
            UPDATE {schema}.{table}
            SET "Video_Title" = %(title)s,
                "Video Views" = %(viewCount)s,
                "Likes Count" = %(likeCount)s,
                "Comments_Count" = %(commentCount)s
            WHERE "video_ID" = %(video_id)s AND "Upload_Date" = %(publishedAt)s;
            """, row
        )
        conn.commit()

        logger.info(f"Updated row with Video_ID: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error updating row with Video_ID: {row[video_id]}")
        raise e
    
def delete_rows(cur, conn, schema, ids_to_delete):

    try: 
        ids_to_delete = f"""({', '.join(f"'{id}'" for id in ids_to_delete)})"""

        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "video_ID" IN {ids_to_delete};
            """
        )
        conn.commit()
        logger.info(f"Deleted rows with Video_IDs: {ids_to_delete}")

    except Exception as e:
        logger.error(f"Error deleting rows with Video_IDs: {ids_to_delete}")
        raise e
    