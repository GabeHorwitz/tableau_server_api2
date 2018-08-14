import tableauserverclient as TSC
from config import password,username
import pandas as pd
from db_connection import importer,run_query_non_results,query_text

#sign in to tableau sever
tableau_auth = TSC.TableauAuth(username,password, site_id='PeopleAnalytics')
request_options = TSC.RequestOptions(pagesize=1000) #Set page size options
server = TSC.Server('https://tableau.we.co/')
with server.auth.sign_in(tableau_auth):

#creating table of usernames and workbook permission

    #creates a list of all workbook on the server
    list_workbooks = []

    #creates a list of all users on server
    list_users = []

    #getting all users
    all_users, pagination_item = server.users.get()
    x = 0
    #iterating through users and populating workbooks for each user
    for x in range(len(all_users)):
        page_n = server.users.populate_workbooks(all_users[x])
        for workbook in all_users[x].workbooks:
             list_users.append(all_users[x].name)
             list_workbooks.append(workbook.name)
             x+1
    #creating a dataframe for all users and workbooks
    data1 = pd.DataFrame({'usernames' : list_users,
             'workbooks' : list_workbooks})
    data1.drop_duplicates()
#exporting to CSV
data1.to_csv('/var/www/data_imports/csv_tableau/tableau_permission.csv',index = False, header=None)
#run query to create schema and table
run_query_non_results(query_text)
#import data from CSV into Table
importer()
