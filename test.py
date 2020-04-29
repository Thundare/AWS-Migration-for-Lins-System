# import boto3
# import mysql.connector
# client = boto3.client('s3')
#
# cnx = mysql.connector.connect(
#     host='ss2-test-database.cl9pljacubb2.us-west-2.rds.amazonaws.com',
#     user='ss2_application',
#     password='se3cur1ty',
#     database='ss2_migration_latest'
# )
#
# cursor = cnx.cursor()
#
# cursor.execute("SELECT m.seller_id, concat(sku, '/',file_name_orig) AS sku1, "
#                "concat(chempax_sku, '/', file_name_orig) AS sku2 "
#                "FROM ss2_migration_latest.products p INNER JOIN master_products m "
#                "ON m.master_product_id = p.master_product_id "
#                "INNER JOIN documents d "
#                "ON d.seller_id = m.seller_id")
#
# sql_docs = cursor.fetchall()
# cnx.close()
# print('Query ran and closed out.')
#
# for docs in sql_docs:
#     jobs = client.copy_object(
#         Bucket='abacus-test-2',  # Destination bucket
#         CopySource=f'qcdocs-live/{docs[1]}',
#         Key=f'seller-document/{docs[0]}/{docs[1]}',
#     )
#     print(jobs)
