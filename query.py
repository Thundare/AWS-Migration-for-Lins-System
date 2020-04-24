import mysql.connector


def query_path():
    cnx = mysql.connector.connect(
        host='ss2-test-database.cl9pljacubb2.us-west-2.rds.amazonaws.com',
        user='ss2_application',
        password='se3cur1ty',
        database='ss2_migration_latest'
    )

    cursor = cnx.cursor()
    cursor.execute("SELECT concat('seller-document/', s.seller_id, '/', d.file_name_orig) AS s3_path "
                   "FROM ss2_migration_latest.documents d "
                   "INNER JOIN ss2_migration_latest.seller_factory_docs s ON d.document_id = s.document_id")

    query_result = cursor.fetchall()
    cnx.close()
    return query_result
    # for result in query_result:
    #     print(result[0])


query_path()
