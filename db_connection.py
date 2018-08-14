import psycopg2

# Connect to postgre
def todbConn():
    return psycopg2.connect(
        dbname='people_data_prod',
        host='people-data-postgre.c18rznrwwovz.us-east-1.rds.amazonaws.com',
        port='5432',
        user='sys_admin',
        password='PeopleTeam1981'
    )

#creating schema and table if they don't exist
query_text = '''
CREATE SCHEMA IF NOT EXISTS tableau;
DROP TABLE IF EXISTS tableau.tableau_permission;
CREATE TABLE tableau.tableau_permission (usernames text, workbooks text);
'''

# Select Query
def run_query(query_text):
   cur = todbConn().cursor()
   with cur:
       cur.execute(query_text)
       f_all = cur.fetchall()
       cur.close()
   todbConn().close()
   return f_all

# Non select queries (for create table code)
def run_query_non_results(query_text):
   cur = todbConn().cursor()
   try:
       with cur:
           cur.execute(query_text)
           cur.connection.commit()
           cur.close()
   except Exception as e:
       print(e)
       todbConn().close()
   todbConn().close()

def importer():
    curT = todbConn().cursor()  # The database we are posting to
    with curT as cur:
        f = open('/var/www/data_imports/csv_tableau/tableau_permission.csv', 'r')
        try:
            #copying from CSV to table
            cur.copy_from(f, 'tableau.tableau_permission', sep=',')
        except Exception as e:
            print(e)
        f.close()
        cur.close()
    cur.connection.commit()
    todbConn().close()