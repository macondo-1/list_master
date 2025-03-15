import pandas as pd
import datetime
from lists import list_
import time
from time import sleep
import sqlite3
import csv
import numpy as np
import constants as const

class Database:
    """
    This class handles everything related to databases
    """
    def __init__(self):
        pass

    def ConnectToValidEmailsDB(self):
        try:
            conn = sqlite3.connect(const.VALID_EMAILS_DB_PATH)
            sql_query = 'SELECT * FROM valid_emails'
            valid_emails_df = pd.read_sql(sql_query, con=conn) #, index_col='ID'
            conn.close()
            self.initial_db_lenght = len(valid_emails_df)
            self.db_columns = valid_emails_df.columns.to_list()
        except:
            print('failed connecting to database')
        return valid_emails_df
    
    def UpdateValidEmails(self, new_list_df, valid_emails_df):
        """
        Gets 2 dataframes, concatenates them and save them as a database
        """

        new_list_df = new_list_df[self.db_columns]
        df_to_db = pd.concat([valid_emails_df,new_list_df], ignore_index=True)
        #df_to_db = df_to_db.drop_duplicates(subset='email')    # this is avoiding me to add emails for new projects if they are assigned to other projects previously
        self.final_db_lenght = len(df_to_db)
        mm_log_df = Log().df


        mm_log_df = mm_log_df.rename(columns={'Email':'email'})
        merged_df = df_to_db.merge(mm_log_df, on='email', how='left')
        df_to_db['last_sent'] = merged_df['timestamp']


        conn = sqlite3.connect(const.VALID_EMAILS_DB_PATH)
        df_to_db.to_sql('valid_emails', con=conn, if_exists='replace',index=False)
        conn.close()
        
        print('initial db lenght: {0}'.format(self.initial_db_lenght))
        print('final db lenght: {0}'.format(self.final_db_lenght))
        print("")
        return df_to_db
    
    def update_valid_emails_against_mm_log(self, valid_emails_df):    
        
        mm_log_df = Log().df
        mm_log_df = mm_log_df.rename(columns={'Email':'email','project_number':'project','timestamp':'last_sent'})

        merged_df = valid_emails_df.merge(mm_log_df, on='email', how='left')

        merged_df = merged_df.drop_duplicates(subset='email', keep='last')

        merged_df = merged_df[['email','first_name','project_x','last_sent_y','status_y']]
        merged_df = merged_df.rename(columns={'project_x':'project','last_sent_y':'last_sent','status_y':'status'})

        conn = sqlite3.connect(const.VALID_EMAILS_DB_PATH)
        merged_df.to_sql('valid_emails', con=conn, if_exists='replace',index=False)
        conn.close()

        print('success!')
    
    def ExtractListValidEmails(self, valid_emails_df, exctract_dict):
        """
        This function gets the valid emails df and a dictionary with keys as project ids
        and values as amount of records to extract
        """
        available_valid_emails_df = valid_emails_df[valid_emails_df['last_sent'].isna()]
        available_valid_emails_df = available_valid_emails_df[available_valid_emails_df['status'] != 'sent']
        

        for project_id in exctract_dict.keys():
            #valid_emails_to_mm = pd.DataFrame()
            valid_per_project = available_valid_emails_df[available_valid_emails_df['project'] == project_id]
            valid_per_project = valid_per_project[:exctract_dict[project_id]]
            #valid_emails_to_mm = pd.concat([valid_emails_to_mm,valid_per_project])


            project_list = valid_per_project.rename(columns={'project':'project_number'})
            project_list.to_csv('/Users/albertoruizcajiga/Documents/Documents - Alberto’s MacBook Air/final_final/to_process/{0}.csv'.format(project_id), index=False)

        #valid_emails_to_mm = valid_emails_to_mm.rename(columns={'project':'project_number'})
        #valid_emails_to_mm.to_csv('/Users/albertoruizcajiga/Documents/Documents - Alberto’s MacBook Air/final_final/to_process/mm_list.csv', index=False)
        #return valid_emails_to_mm

    def ExtractListValidEmails_to_blast(self, valid_emails_df, exctract_dict):
        """
        This function gets the valid emails df and a dictionary with keys as project ids
        and values as amount of records to extract
        """
        available_valid_emails_df = valid_emails_df

        for project_id in exctract_dict.keys():
            valid_per_project = available_valid_emails_df[available_valid_emails_df['project'] == project_id]
            valid_per_project = valid_per_project[:exctract_dict[project_id]]

            project_list = valid_per_project.rename(columns={'project':'project_number'})
            project_list.to_csv('/Users/albertoruizcajiga/Documents/Documents - Alberto’s MacBook Air/final_final/to_process/{0}.csv'.format(project_id), index=False)
    
    def UpdateTableAfterExtractionValidEmails(self, project_db_df, out_df, conn):
        """
        This function updates the project_db_df last_sent column if the records are in the out_df.
        It updates the last_sent column with the timestamp of when the record was extracted
        """
        timestamp = time()
        emails_list = list(out_df['email'])
        condition = project_db_df['email'].isin(emails_list)          
        project_db_df.loc[condition, 'last_sent'] = timestamp
        project_db_df.to_sql('valid_emails', con=conn, if_exists='replace',index=False)

    def GetAvailableEmailsFromValidDB(valid_emails_df):
        """
        Retrieves the valid emails database and returns the available emails per project
        """






    def ConnectToProjectsDB():
        db_path = '{0}projects_database.db'.format(const.PROJECTS_PATH)
        conn = sqlite3.connect(db_path)
        return conn
    
    def CreateProjectTable(file_path,conn):
        """
        Creates the table on the database for the given project
        """
        project_id = file_path.split('_',1)[0]
        new_db_df = pd.DataFrame(columns=const.DB_COLUMNS)
        new_db_df = new_db_df.rename_axis('ID')
        new_db_df.to_sql(project_id, con=conn, if_exists='replace')

    def ConnectToProjectsTable(file_path, conn):
        """
        Connects to a project table and return it as a pandas dataframe
        """
        project_id = file_path.split('_',1)[0]
        sql_query = "SELECT * FROM '{0}'".format(project_id)
        project_db_df = pd.read_sql(sql_query, con=conn, index_col='ID') #, index_col='ID'
        return project_db_df
    
    def UpdateProjectTable(new_list_df, project_db_df, file_path, conn):
        """
        Gets 2 dataframes, concatenates them and save them as a database
        """
        df_to_db = pd.concat([project_db_df,new_list_df])
        df_to_db.drop_duplicates(subset='email')
        project_id = file_path.split('_',1)[0]
        df_to_db.to_sql(project_id, con=conn, if_exists='replace')
        return df_to_db

    def ExtractList(project_db_df, file_path, max_size_list):
        """
        Get a given amount of records from the projects database
        """
        project_id = file_path.split('_',1)[0]
        out_df = project_db_df[(project_db_df['project' == project_id] & project_db_df['last_contact_date'].isna())]
        out_df = project_db_df[:max_size_list]
        return out_df
    
    def UpdateTableAfterExtraction(project_db_df, out_df, conn, project_id):
        """
        This function updates the project_db_df last_sent column if the records are in the out_df.
        It updates the last_sent column with the timestamp of when the record was extracted
        """
        timestamp = time()
        emails_list = list(out_df['email'])
        condition = project_db_df['email'].isin(emails_list)          
        project_db_df.loc[condition, 'last_sent'] = timestamp
        project_db_df.to_sql(project_id, con=conn, if_exists='replace')


    def UpdateBlockedEmails(self):

        blocked_rows = pd.read_csv(const.BLACKLIST_PATH , on_bad_lines='skip', header=None, quoting=csv.QUOTE_NONE)
        blocked_rows = blocked_rows.rename(columns={0:'email'})
        blocked_rows['email'] = blocked_rows['email'].str.lower()

        emails_to_blocklist = input('Type the emails to block separated by commas:\n')
        emails_to_blocklist = emails_to_blocklist.split(',')
        emails_to_blocklist = [x.lower() for x in emails_to_blocklist]
        emails_to_blocklist = {'email':emails_to_blocklist}

        ## updating from file
        # emails_to_blocklist = pd.read_csv('/Users/albertoruizcajiga/Library/CloudStorage/GoogleDrive-beautifulday874@gmail.com/My Drive/Information_Technology/blacklist_emails.txt', header=None)
        # emails_to_blocklist = {'email':emails_to_blocklist[0].to_list()}
        
        emails_to_blocklist = pd.DataFrame(emails_to_blocklist)

        new_block_list = pd.concat([blocked_rows,emails_to_blocklist])
        new_block_list = new_block_list.drop_duplicates(subset='email')

        new_block_list.to_csv(const.BLACKLIST_PATH, header=False, index=False, sep='\n')

        new_records_df = new_block_list[~new_block_list['email'].isin(blocked_rows['email'])]
        new_records_len = len(new_records_df)
        if new_records_len > 0:
            print('Added {0} new records'.format(new_records_len))
        else:
            print('No new records were added')


    def get_one_email_per_domain(self, df):
        unique_emails = df.groupby(df['email'].str.split('@').str[1]).first().reset_index(drop=True)
        return unique_emails
    
    def print_mm_names(self, mm_list):
        today = datetime.datetime.combine(datetime.date.today(), datetime.time.min)

        df = pd.read_excel(const.BLAST_MASTER_PATH)
        df.set_index('Unnamed: 1', inplace = True)
    
        for project in mm_list:
            try:
                project_info = df.loc[float(project)]
                mm_name = project_info['template_name'] + '_mm'
                already_mmed = list(df[today].unique())
                if mm_name not in already_mmed:
                    print(mm_name)
            except:
                print('failed {0}'.format(project)) 

    def extract_for_project(self, mm_len, database = True, last_sent = True):
        projects_dict = list_.GetMMDict(mm_len)

        database_df = pd.read_csv(const.TEST_DB_PATH,dtype=object,low_memory=False)

        non_found = []
        found = []
        
        for project in projects_dict:
            max_size_list = projects_dict[project]
        
            project_db = database_df[database_df['project'] == project]
            
            if database:
                project_db = project_db[(project_db['list'].str.contains('_db'))]
                
            if last_sent:
                project_db = project_db[project_db['last_sent'].isna()]
            
            if len(project_db) > 0:
            
                cols = project_db.columns
                
                outgoing_list = pd.DataFrame(columns=cols)

                if len(project_db) > max_size_list: 
                    while len(outgoing_list) < max_size_list:
                        outgoing_list = pd.concat([outgoing_list, self.get_one_email_per_domain(project_db)], ignore_index = True)
                        emails_list = list(outgoing_list['email'])
                        condition = project_db['email'].isin(emails_list)
                        project_db.loc[condition, 'last_sent'] = 'already used'
                        project_db = project_db[(project_db['project'] == project) & (project_db['last_sent'].isna())]
                else:
                    outgoing_list = project_db

                outgoing_list = outgoing_list[:max_size_list]
                outgoing_list.loc[outgoing_list['first_name'].isna(), 'first_name'] = 'Colleague'
                out_path = '/Users/albertoruizcajiga/Documents/Documents - Alberto’s MacBook Air/final_final/mailing_bot/'
                outgoing_list.to_csv(f'{out_path}{project}_cleaned.csv', index = False, columns = ['first_name','email'])
                emails_list = list(outgoing_list['email'])

                condition = database_df['email'].isin(emails_list)          
                timestamp = time.time()
                database_df.loc[condition, 'last_sent'] = timestamp
   
                found.append(project)
                
            else:
                non_found.append(project)
        
        database_df.to_csv(const.TEST_DB_PATH, index = False)
   
        print('found in db: {0}'.format(found))
        print('not found in db: {0}'.format(non_found))
        self.print_mm_names(found)

    def create_blast_report(self, row):
        today = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
        tomorrow = today + datetime.timedelta(1)   

        if row[tomorrow]:
            return f"{row['Project Manager']} - {row['project_name']} - {row[tomorrow]}"
        else:
            return f"{row['Project Manager']} - {row['project_name']}"

    def GetDailyBlastNeeds(self):

        today = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
        tomorrow = today + datetime.timedelta(1)
        
        df = pd.read_excel(const.BLAST_MASTER_PATH)
        blast_needs = df[['Project Manager','project_name',today,tomorrow]].dropna(subset = today)

        blast_needs = blast_needs.replace(np.nan,'')

        # Apply the function to each row
        blast_needs['blast_report'] = blast_needs.apply(self.create_blast_report, axis=1)

        blast_needs['blast_report'].to_clipboard(header=False, index=False)
        print(blast_needs['blast_report'])



class Log:
    """
    This class handles everything related to the mailmerge logs
    """

    def __init__(self):
        self.today = datetime.date.today()
        self.today = pd.to_datetime(self.today)
        self.path = const.MM_LOG_PATH
        self.df = pd.read_csv(self.path)
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'], dayfirst=True)

    def mailmerge_summary(self, input_date):
        """
        prints summary of day's mailmerges
        """

        if input_date == '':
            input_date = self.today
        else:
            input_date = pd.to_datetime(input_date, dayfirst=True)

        df1 = pd.DataFrame(self.df[self.df['timestamp']==input_date].value_counts(subset='project_number'))
        df1 = df1.rename(columns={0:'count'})

        total_count = df1['count'].sum()

        print(df1)
        print('\ntotal count: {0}'.format(total_count))
