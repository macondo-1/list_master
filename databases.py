import constants as const
import sqlite3
import pandas as pd
from datetime import date

class SM_Database():
    """
    This class handles the SM Database
    """

    def __init__(self):
        
        self.path = const.SM_DB_PATH
        conn = sqlite3.connect(self.path)

        self.database_dataframe = pd.read_sql('SELECT * FROM contacts ', conn)
        conn.close()
        
        db_columns = self.database_dataframe.columns.to_list()
        self.database_columns = db_columns

    def filter_database(self,column_to_filter,keywords_list, include):

        uniques_column = pd.DataFrame(self.database_dataframe[column_to_filter].unique())

        if not include:
            filtered_keywords = uniques_column[uniques_column[0].str.contains('|'.join(keywords_list), case=False, na=False)]
        else:
            filtered_keywords = uniques_column[~uniques_column[0].str.contains('|'.join(keywords_list), case=False, na=False)]
        
        filtered_keywords = filtered_keywords[0].unique()
        filtered_keywords = list(filtered_keywords)
        filtered_records = self.database_dataframe[self.database_dataframe[column_to_filter].str.contains('|'.join(filtered_keywords), case=False, na=False)]

        return filtered_records

    def extract_list(self):
        """
        Extracting records from the database according to user keywords
        """
        filtered_database = self.database_dataframe
        choice = ''
        while choice != 'q':
            print('Available columns:\n {0}\n'.format(self.database_columns))
            choice = input('include[0] or exclude[1] or extract[2]?')

            if (choice == '0' or choice == '1'):
                keywords = input('Provide keywords separated by commas: ')
                keywords_list = keywords.split(',')

                column_to_filter = input('Provide columns separated by commas: ')

                filtered_database = self.filter_database(column_to_filter,keywords_list,int(choice))
                matches = filtered_database[column_to_filter].unique()
                print(matches)


            elif choice == '2':
                filename_out = const.SM_DB_DIR_PATH.joinpath('extracted_list.csv')
                filtered_database.to_csv(filename_out, index=False)
                return filtered_database

            
class ProjectDatabase():
    """
    This class handles project's databases
    """


    def __init__(self):
        pass


    def create_project_database(self, file_name):
        """
        Creates a database for the project containing one table for list makers, one for survey monkey database and
        the last one for qualtrics database
        """
        project_id = file_name.split('_')[0]
        database_file_name = '{0}.db'.format(project_id)
        conn = sqlite3.connect(database_file_name)

        df = pd.DataFrame(columns=const.DB_COLUMNS)
        df.to_sql('{0}_list_makers'.format(project_id), conn)
        df.to_sql('{0}_sm_db'.format(project_id), conn)
        df.to_sql('{0}_qualtrics_db'.format(project_id), conn)


    def add_new_list_to_db(self, file_name):
        """
        connects to the projects database, gets all the emails in there to dedupe the new list and then appends it to the database
        """
        project_id = file_name.split('_')[0]
        database_file_name = '{0}.db'.format(project_id)
        conn = sqlite3.connect(database_file_name)

        database_emails_df = pd.read_sql('SELECT email FROM "{0}_list_makers"'.format(project_id), conn)

        new_list_df = pd.read_csv(file_name)
        new_list_df = new_list_df[~new_list_df['email'].isin(database_emails_df['email'])]
        new_list_df.to_sql('{0}_list_makers'.format(project_id), conn, if_exists='append')


    def extract_list_from_db(self, project_id, list_size):
        """
        excracts a list and marks its last_contact_date column with a timestamp
        """

        today = date.today()
        today = today.strftime("%Y%m%d")

        database_file_name = '{0}.db'.format(project_id)
        conn = sqlite3.connect(database_file_name)
        database_df =  pd.read_sql('SELECT * FROM "{0}_list_makers"'.format(project_id), conn)
        exctract_list = database_df[:list_size]
        exctract_list.to_csv('{0}_list.csv'.format(project_id), index=False)

        condition = database_df['email'].isin(exctract_list['email'])
        database_df.loc[condition, 'last_contact_date'] = today
        database_df.to_sql('{0}_list_makers'.format(project_id), conn, if_exists='replace')


    def get_projects_db_stats(self, project_id):
        database_file_name = '{0}.db'.format(project_id)
        conn = sqlite3.connect(database_file_name)
        list_makers_df =  pd.read_sql('SELECT email FROM "{0}_list_makers" WHERE last_contact_date IS NULL'.format(project_id), conn)
        survey_monkey_db =  pd.read_sql('SELECT email FROM "{0}_sm_db" WHERE last_contact_date IS NULL'.format(project_id), conn)
        qualtrics_db =  pd.read_sql('SELECT email FROM "{0}_qualtrics_db" WHERE last_contact_date IS NULL'.format(project_id), conn)

        print('{0} available records from list_makers'.format(len(list_makers_df)))
        print('{0} available records from survey monkey'.format(len(survey_monkey_db)))
        print('{0} available records from qualtrics'.format(len(qualtrics_db)))


    def update_validations_status(self, file_name):
        project_id = file_name.split('_')[0]
        database_file_name = '{0}.db'.format(project_id)
        conn = sqlite3.connect(database_file_name)
        database_df = pd.read_sql('SELECT * FROM "{0}_list_makers" '.format(project_id), conn)

        valid_file_name = 'valid-{0}'.format(file_name)
        valid_df = pd.read_csv(valid_file_name)

        condition = database_df['email'].isin(valid_df['email'])
        database_df.loc[condition, 'email_validation'] = True

        database_df.to_sql('{0}_list_makers'.format(project_id), conn, if_exists='replace')

        valid_db_df = database_df[database_df['email_validation'] == True]

        print(len(valid_db_df))

class LogDB():

    def __init__(self):
        
        conn = sqlite3.connect(const.BCC_LOG_DB_PATH)
        self.conn = conn

        self.database_dataframe = pd.read_sql('SELECT * FROM log ', conn)
        
        db_columns = self.database_dataframe.columns.to_list()
        self.database_columns = db_columns

class ValidEmailsDB():

    def __init__(self):
        
        self.conn = sqlite3.connect(const.VALID_EMAILS_DB_PATH)

        self.database_dataframe = pd.read_sql('SELECT * FROM valid_emails', self.conn)

    def add_new_list_to_db(self, file_name):
        """
        connects to the valid_emails database, gets all the emails in there to dedupe the new list and then appends it to the database
        """

        new_list_df = pd.read_csv(file_name)
        new_list_df = new_list_df[['email','first_name']]
        new_list_df = new_list_df[~new_list_df['email'].isin(self.database_dataframe['email'])]
        new_list_df.to_sql('valid_emails', self.conn, if_exists='append')

        
if __name__ == '__main__':
    handler = SM_Database()
    handler.extract_list()

