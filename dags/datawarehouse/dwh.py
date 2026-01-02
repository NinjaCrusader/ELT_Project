from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import transform_data

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"

@task
def staging_table():

    schema = 'staging'

    conn, cur = None, None

    try:

        conn, cur = get_conn_cursor()

        YT_data = load_data()
        if YT_data:
            logger.info(f"LOADED DATA IN STAGING PROCESS")

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)
        if table_ids:
            logger.info(f"LOADED TABLE ID'S IN STAGING PROCESS")

        for row in YT_data:

            if len(table) == 0:
                insert_rows(cur, conn, schema, row)
                logger.info(f"INSERTING ROW")
            else:
                if row['video_id'] in table_ids:
                    update_rows(cur, conn, schema, row)
                    logger.info(f"UPDATING ROW")
                else:
                   insert_rows(cur, conn, schema, row)
        
        ids_in_json = {row['video_id'] for row in YT_data}

        ids_to_delete = set(table_ids) - ids_in_json

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)
            logger.info(f"DELETED ROWS IN STAGING")

        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occured during the update of {schema} table: {e}")
        raise e
    
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)

@task
def core_table():

    schema = "core"

    conn, cur = None, None

    try:

        conn, cur = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)
        if table_ids:
            logger.info(f"GOT TABLE IDS IN CORE TABLE")

        current_video_ids = set()

        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall()

        for row in rows:

            current_video_ids.add(row["Video_ID"])

            if len(table_ids) == 0:
                transformed_row = transform_data(row)
                insert_rows(cur, conn, schema, transformed_row)
                logger.info(f"INSERTED ROWS IN CORE DATA")

            else:
                transformed_row = transform_data(row)

                if transformed_row['Video_ID'] in table_ids:
                    update_rows(cur, conn, schema, transformed_row)
                    logger.info(f"UPDATED ROWS IN CORE DATA")

                else:
                    insert_rows(cur, conn,schema,transformed_row)
                    logger.info(f"INSERTED ROWS IN CORE DATA")

        ids_to_delete = set(table_ids) - current_video_ids

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)
            logger.info(f"DELETED ROWS IN CORE DATA")
        
        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occured during the update of {schema} table: {e}")
        raise e
    
    finally:
        if conn and cur:            
            close_conn_cursor(conn, cur)