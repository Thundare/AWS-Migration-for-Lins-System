import mysql.connector

cnx = mysql.connector.connect(
    host='ss2-test-database.cl9pljacubb2.us-west-2.rds.amazonaws.com',
    user='ss2_application',
    password='se3cur1ty',
    database='ss2_migration_latest'
)


def query_path():
    cursor = cnx.cursor()
    cursor.execute("SELECT * FROM ss2_migration_latest.documents WHERE seller_id != 0;")
    query_result = cursor.fetchall()
    for result in query_result:
        print(result[4])


cnx.close()
