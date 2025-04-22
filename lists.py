import pandas as pd
import constants as const
import re
import csv
import os
import sqlite3
import datetime
from datetime import time
from time import sleep
import glob
import numpy as np

class list_:
    """
    This class handles everything related to lists
    """
    def __init__(self):
        pass

    def ReadList(file_path, header_row = 0): 
        """
        Reads a list file if it's extension is either:
        ['csv','xlsx','xls']
        Returns a pandas dataframe of the read file
        """
        try:
            file_name = file_path.split('.')[0]
            file_extension = file_path.split('.')[1]
            print(file_name)
            print(file_extension)

            if file_extension == 'csv':
                df = pd.read_csv(file_path, on_bad_lines='skip',low_memory=False,encoding='latin-1',dtype=object, header=header_row)
            elif file_extension in ['xls', 'xlsx']:
                df = pd.read_excel(file_path,dtype=object, header=header_row)
            else:
                df = None
                print("file extension not compatible: {0}".format(file_extension))

            return df

        except:
            print('failed reading file\n')
    
    def FixRecords(df):
        """
        Replaces None for Colleague on the first_name column and
        deletes None records on the email column and 
        drops duplicates on the email column
        """
        try:
            df['first_name'] = df['first_name'].replace({None:'Colleague'})
            df['first_name'] = df['first_name'].str.split(' ').str[0]
            df['first_name'] = df['first_name'].str.strip()
            df['first_name'] = df['first_name'].str.capitalize()
            df['first_name'] = df['first_name'].apply(lambda x: x if x.isascii() else 'Colleague')

            df = df.dropna(subset = 'email')
            df.loc[:, 'email'] = df['email'].str.strip()
            df.loc[:, 'email'] = df['email'].str.lower()
            df = df.drop_duplicates(subset = 'email')

            # Define the regex pattern to match valid email addresses
            pattern = r'^[\w\.-]+@([\w-]+\.)+[\w-]{2,4}$'


            # Use .str.extract() to extract valid email addresses
            df = df[df['email'].apply(lambda x: isinstance(x, str) and pd.notna(x) and pd.Series(x).str.extract(pattern, expand=False).notnull().any())]

            return df

        except:
            print('failed fixing records')

    
    def FixUnknownColumns(df,special_columns = None):
        """
        Receives a raw dataframe from any source and returns a dataframe with
        columns ['first_name','email']
        """

        df_cols = pd.Series(df.columns)
        
        # This section gets the first_name column fixed
        
        re_pattern = '(?=.*first)(?=.*name)|(?=.*full)(?=.*name)|(?=.*middle)(?=.*name)|(?=.*last)(?=.*name)|(?=.*f)(?=.*name)'

        name_col = list(df_cols[df_cols.str.contains(re_pattern, case=False, na=False, regex=True )])
        name_col = name_col[0]
        
        if not name_col:
            df['first_name'] = 'Colleague'
        else:
            df = df.rename(columns = {name_col:'first_name'})

        # This section gets the email column fixed

        email_columns = [x for x in df_cols if 'email' in x.lower()]
        df[email_columns] = df[email_columns].applymap(str)
        df['email'] = df[email_columns].agg(','.join, axis=1)
        df['email'] = df['email'].apply(lambda x: x.split(','))
        df['email'] = df['email'].apply(lambda email_list: next((email for email in email_list if '@' in email), None)) #consider changing this "@" check for the actual regex of an email format

        df = df[['first_name','email']]

        return df
    
    def FixColumns(df):
        """
        Receives a raw dataframe and changes the names of the columns to match those in the database
        """
        try:
            columns = pd.Series(df.columns)
            ru_columns = pd.Series(const.RU_MAPPER.keys())
            apollo_columns = pd.Series(const.APOLLO_MAPPER.keys())
            apollo_columns_1 = pd.Series(const.APOLLO_MAPPER_1.keys())
            hunter_columns = pd.Series(const.HUNT_MAPPER.keys())
            hunter_columns_1 = pd.Series(const.HUNT_MAPPER_1.keys())
            snov_columns = pd.Series(const.SNOV_MAPPER.keys())

            if ru_columns.isin(columns).all():
                df = df.rename(columns = const.RU_MAPPER)
                df = df[const.RU_MAPPER.values()]

            elif apollo_columns.isin(columns).all():
                df = df.rename(columns = const.APOLLO_MAPPER)
                df = df[const.APOLLO_MAPPER.values()]

            elif apollo_columns_1.isin(columns).all():
                df = df.rename(columns = const.APOLLO_MAPPER_1)
                df = df[const.APOLLO_MAPPER_1.values()]

            elif hunter_columns.isin(columns).all():
                df = df.rename(columns = const.HUNT_MAPPER)
                df = df[const.HUNT_MAPPER.values()]

            elif hunter_columns_1.isin(columns).all():
                df = df.rename(columns = const.HUNT_MAPPER_1)
                df = df[const.HUNT_MAPPER_1.values()]

            elif snov_columns.isin(columns).all():
                df = df.rename(columns = const.SNOV_MAPPER)
                df = df[const.SNOV_MAPPER.values()]

            else:
                df = list_.clean_list_manually(df)

            return df
        
        except:
            print('failed fixing columns')
    
    def CleanBlacklisted(df):
        filename_in = const.BLACKLIST_PATH
        blocked_rows = pd.read_csv(filename_in, on_bad_lines='skip', header=None, quoting=csv.QUOTE_NONE)
        blocked_rows = blocked_rows.rename(columns={0:'email'})
        blocked_rows['email'] = blocked_rows['email'].str.lower()
        df = df[~df['email'].isin(blocked_rows['email'])] #this is not ok, this tests for exact matches but there are instances were complete domains are blocked, we need to check against substrings
        df = df[~df['email'].apply(lambda x: any(blocked_substring in x for blocked_substring in blocked_rows['email']))]
        return df
    
    def clean_list_manually(df):

        print('Available columns: \n\n{0}\n'.format(df.columns.to_list()))
        first_name_col = str(input('first name column: '))
        email_col = input('email column: ')
        additional_columns = input('select additional columns (separated by commas):')

        if not first_name_col and not email_col and not additional_columns:
            first_name_col = 'first_name'
            email_col = 'email'

        elif not first_name_col:
            df['first_name'] = 'Colleague'
            first_name_col = 'first_name'

        if not additional_columns:
            df = df[[first_name_col,email_col]]

        else:
            additional_columns = additional_columns.split(',')
            complete_columns = [first_name_col,email_col]
            complete_columns = complete_columns + additional_columns
            df = df[complete_columns]

        df = df.drop_duplicates(subset = email_col)
        df = df.dropna(subset = email_col)
        df = df.replace(np.nan, 'Colleague')

        # Define the regex pattern to match valid email addresses
        pattern = r'^[\w\.-]+@([\w-]+\.)+[\w-]{2,4}$'
        # Use .str.extract() to extract valid email addresses
        df = df[df[email_col].apply(lambda x: isinstance(x, str) and pd.notna(x) and pd.Series(x).str.extract(pattern, expand=False).notnull().any())]

        df = df[df[first_name_col].apply(lambda x: isinstance(x, str) and pd.notna(x))]
        df[first_name_col] = df[first_name_col].apply(lambda x: x.split(' ')[0])
        df[first_name_col] = df[first_name_col].apply(lambda x: x.capitalize())
        df[email_col] = df[email_col].apply(lambda x: x.lower())
        df= df.rename(columns={first_name_col:'first_name',
                                email_col:'email'
                                })

        return df

    def CleanListManually():
        os.chdir(const.PROCESSING_FOLDER)

        all_read_paths = [i for i in glob.glob('*.csv')]
        all_read_paths += [i for i in glob.glob('*.xlsx')]
        failed_files = []
        for file_name in all_read_paths:

            try:
                df = list_.ReadList(file_name)
                print(df.head())
                
                header_row = int(input('first[0] or second[1] row as column headers? [0/1]: '))
                
                df = list_.ReadList(file_name, header_row=header_row)
                print('Available columns: \n\n{0}\n'.format(df.columns.to_list()))
                first_name_col = input('first name column: ')
                email_col = input('email column: ')
                additional_columns = input('select additional columns (separated by commas):')

                if not additional_columns:
                    df = df[[first_name_col,email_col]]
                else:
                    additional_columns = additional_columns.split(',')
                    complete_columns = [first_name_col,email_col]
                    complete_columns = complete_columns + additional_columns
                    df = df[complete_columns]

                df = df.drop_duplicates(subset = email_col)
                df = df.dropna(subset = email_col)
                df = df.replace(np.nan, 'Colleague')

                # Define the regex pattern to match valid email addresses
                pattern = r'^[\w\.-]+@([\w-]+\.)+[\w-]{2,4}$'
                # Use .str.extract() to extract valid email addresses
                df = df[df[email_col].apply(lambda x: isinstance(x, str) and pd.notna(x) and pd.Series(x).str.extract(pattern, expand=False).notnull().any())]
                
                df = df[df[first_name_col].apply(lambda x: isinstance(x, str) and pd.notna(x))]
                df[first_name_col] = df[first_name_col].apply(lambda x: x.split(' ')[0])
                df[first_name_col] = df[first_name_col].apply(lambda x: x.capitalize())
                df[email_col] = df[email_col].apply(lambda x: x.lower())
                df = df.dropna(subset=email_col)
                df = df.replace(np.nan,'Colleague')
                df= df.rename(columns={first_name_col:'first_name',
                                    email_col:'email'
                                    })

                file_name_ext = file_name.split('.')[1]
                if file_name_ext == '.csv':
                    pass
                else:
                    os.remove(file_name)
                    file_name = file_name.split('.')[0] + '.csv'
                    

                df.to_csv(file_name, index = False)
            except Exception as e:
                 failed_files.append(file_name)
                 print('exception: ', e)
            finally:
                print('failed files:')
                print(failed_files)


    def CleanList():
        read_path = input('\nType the list file name to clean: ')
        read_path = '{0}{1}'.format(const.TO_PROCESS_PATH,read_path)
        df = list_.ReadList(read_path)
        df = list_.FixColumns(df)
        df = list_.FixRecords(df)
        df = list_.CleanBlacklisted(df)
        df.to_csv(read_path, index=False)

        return df


    def CleanLists():
        try:

            export = int(input('prepare to export? [0/1]: '))
            dedupe = input('dedupe from mm log? [y/n]: ')

            # TRYING TO FIX THE FILE NAMES

            # all_read_paths = [i for i in glob.glob('{0}{1}'.format(const.PROCESSING_FOLDER,'*.csv'))]
            # all_read_paths += [i for i in glob.glob('{0}{1}'.format(const.PROCESSING_FOLDER,'*.xlsx'))]

            # for read_path in all_read_paths:

            #     new_name = read_path.split('.')
            #     extension = new_name[-1]
            #     new_name = new_name[:-1]
            #     new_name = '_'.join(new_name)
            #     new_name = new_name.replace('.','_').replace(' ','_').replace('__','_')
            #     new_name = '{0}.{1}'.format(new_name,extension)  
            #     print(read_path)  
            #     print(new_name)
            #     os.rename(read_path,new_name)   

            all_read_paths = [i for i in glob.glob('{0}{1}'.format(const.PROCESSING_FOLDER,'*.csv'))]
            all_read_paths += [i for i in glob.glob('{0}{1}'.format(const.PROCESSING_FOLDER,'*.xlsx'))]

            for read_path in all_read_paths:
                try:
                    df = list_.ReadList(read_path)
                    df = list_.FixColumns(df)
                    df = list_.FixRecords(df)
                    df = list_.CleanBlacklisted(df)
                    if dedupe == 'y':
                        df = list_.DedupeFromLog(df)
                    if export:
                        df = df[['first_name','email']]
                    out_name = read_path.split('.')[0]
                    extension = read_path.split('.')[1]
                    #if extension == 'xlsx':
                    os.remove(read_path)

                    out_name = out_name.split('/')
                    out_name[-1] = out_name[-1].replace(' ','_')
                    out_name = '/'.join(out_name)
                    out_name += '.csv'
                    df.to_csv(out_name, index=False)
                except:
                    error_message = 'failed cleaning: {0}'.format(read_path)
                    print(error_message)
        except:
            error_message = 'failed cleaning lists'
            print(error_message)


    def get_project_info(file_name, df_blastmaster):
        try:
            df_blastmaster1 = df_blastmaster.astype({'Unnamed: 1': 'str'})
            df_blastmaster1['Unnamed: 1'] = df_blastmaster1['Unnamed: 1'].apply(lambda x: x.split('.')[0])
            df_blastmaster1 = df_blastmaster1.set_index('Unnamed: 1')
            p_number = str(file_name.split('_')[0])
            project_info = df_blastmaster1.loc[p_number]
        except:
            print('Error getting project information for {0}'.format(file_name))

        return project_info
    
    def GetMMDict(mm_len):
        today = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
        tomorrow = today + datetime.timedelta(1)

        df = pd.read_excel(const.BLAST_MASTER_PATH)
        blast_needs = df[['Unnamed: 1','project_name',today]].dropna(subset = today)

        ids_to_mm = list(blast_needs['Unnamed: 1'])
        projects_dict = {}
        lenght_per_project = int(mm_len/len(ids_to_mm))

        for id in ids_to_mm:
            id = str(id).split('.')[0]
            projects_dict[id] = lenght_per_project

        print(projects_dict)

        return projects_dict

    def CheckAgainstAlreadySentEmails():
        df = pd.read_csv(const.MM_LOG_PATH)
        sent_emails = df['Email']
    
    def CreateMMList():
        try:
            os.chdir('/Users/albertoruizcajiga/Documents/Documents - Alberto’s MacBook Air/final_final/mailing_bot')
            FROM_NAME = 'Ruth Stanat'

            #reading file names
            file_extension = '.csv'
            all_filenames = [i for i in glob.glob(f"*{file_extension}")]
            df_blast_master = pd.read_excel(const.BLAST_MASTER_PATH)
            #df_blast_master = df_blast_master.set_index('Unnamed: 1')

            #iterating over each csv
            for file_name in all_filenames:
                p_number = file_name.split('_')[0]
                project_info1 = list_.get_project_info(file_name,df_blast_master)
                MESSAGE = project_info1['Blast Message']

                df = pd.read_csv(file_name)
                df = df.rename(columns={'first_name':'First_name',
                                        'email':'Email',
                                        })

                df = df.replace({'First_name':'None'},'Colleague')
                df['message'] = df.apply(lambda row: MESSAGE.format(First_name=row['First_name'], FROM_NAME=FROM_NAME), axis=1)
                df['project_number'] = p_number
                df.to_csv(file_name, index = False)
            
            concatenated_df = pd.concat([pd.read_csv(f,low_memory=False) for f in all_filenames])
            concatenated_df = concatenated_df.sort_values(by='Email', ascending=True)
            concatenated_df = concatenated_df[['Email','First_name','message','project_number']]
            
            concatenated_df.to_csv('mm_list.csv', index = False)
            mm_list_len = len(concatenated_df)
            print("\nnew mm list length: {mm_list_len}\n".format(mm_list_len=mm_list_len))

            for file_name in all_filenames:
                os.remove(file_name)
        except:
            print('failed creating the MM list, check all file names {0}'.format(file_name))

    def DecomposeMMList():
        try:
            filename = '{0}/mm_list.csv'.format(const.MAILING_PATH)
            df = pd.read_csv(filename)

            projects_list = list(df['project_number'].unique())

            for project in projects_list:
                df_out = df[df['project_number'] == project]
                filename_out = '{0}/{1}_mm_list.csv'.format(const.MAILING_PATH,project)
                df_out.to_csv(filename_out, index=False)

            os.remove(filename)
        except:
            error_message = 'failed decomposing mm list'
            print(error_message)

    def GetURLsFromSnoToHunt():
        os.chdir(const.PROCESSING_FOLDER)

        max_chunk = 25000

        file_extension = '.csv'
        all_file_names = [i for i in glob.glob(f"*{file_extension}")]

        combined_csv_data = pd.concat([pd.read_csv(file_name,usecols=['url']) for file_name in all_file_names])
        #combined_csv_data = pd.read_excel('LIST - Data from Client - 1.30.xlsx',usecols=['url'])

        combined_csv_data = combined_csv_data.drop_duplicates().dropna()

        if len(combined_csv_data) > max_chunk:

            min_chunk = 0

            while min_chunk < len(combined_csv_data):
                urls_chunk = combined_csv_data[min_chunk:max_chunk]
                urls_chunk.to_clipboard(sep=',', header=False, index=False)
                min_chunk = max_chunk
                max_chunk += max_chunk
                print(f"you copied {len(urls_chunk)} URL's to the clipboard")
                input('Press enter to get file name...')
                print(f"you copied {all_file_names} to the clipboard")
                input('Press enter to continue...\n')


        else:
            combined_csv_data.to_clipboard(sep=',', header=False, index=False)
            print(f"you copied {len(combined_csv_data)} URL's to the clipboard")
            input('Press enter to get file name...')
            print(f"you copied {all_file_names} to the clipboard")
            input('Press enter to continue...\n')

    def concat_lists():
        all_read_paths = [i for i in glob.glob('{0}{1}'.format(const.PROCESSING_FOLDER,'*.csv'))]
        combined_csv_data = pd.concat([pd.read_csv(f,low_memory=False) for f in all_read_paths])
        combined_csv_data = combined_csv_data.drop_duplicates(subset=['email']).dropna(subset=['email'])
        combined_csv_data.to_csv('{0}combined_files.csv'.format(const.PROCESSING_FOLDER), index = False)
        for i in all_read_paths:
            os.remove(i)

    def DedupeFromLog(df):
        """
        Reads the log of MM sent emails and dedupes the new df from this log.
        Need to update to dedupe only from those actually sent emails, not considering failed ones.
        """
        read_path = const.LOG_PATH
        log_df = pd.read_csv(read_path)

        df = df[~df['email'].isin(log_df['Email'])]

        return df
    
    def Deduper():
        """
        Dedupes list A from list B
        """
        all_read_paths = [i for i in glob.glob('{0}{1}'.format(const.PROCESSING_FOLDER,'*.csv'))]
        all_read_paths += [i for i in glob.glob('{0}{1}'.format(const.PROCESSING_FOLDER,'*.xlsx'))]
        print(all_read_paths)
        
        list_A = input('provide the name of the main list: ')
        list_B = input('provide the name of the dedupe list: ')

        df_a = pd.read_csv(list_A)
        df_b = pd.read_csv(list_B)

        df_deduped = df_a[~df_a['email'].isin(df_b['email'])]
        df_deduped.to_csv(const.PROCESSING_FOLDER + '/deduped.csv',index=False)


    def UploadToGA():
        pass

    def SendListGA():
        pass

    def SendListMM():
        pass

    def SendListBCC():
        pass

    def ThrottleList():
        pass

    def DNSCheck():
        pass

class NewList:
    def __init__(self, file_path):
        try:
            self.file_path = file_path
            self.project_id = str(file_path.split('_',1)[0])
            self.file_extension = str(file_path.split('.')[-1])
            
        except:
            error_message = 'something wrong with the file name: {0}'.format(self.file_path)
            print(error_message)

    def read_list(self):
        try:
            if self.file_extension == 'csv':
                new_list_df = pd.read_csv(self.file_path, on_bad_lines='skip',low_memory=False,encoding='latin-1',dtype=object)
            elif self.file_extension in ['xls', 'xlsx']:
                new_list_df = pd.read_excel(self.file_path,dtype=object)
            else:
                new_list_df = None
                print("file extension not compatible: {0}".format(self.file_extension))

            new_list_df['project'] = self.project_id
            new_list_df['last_sent'] = None
            new_list_df= new_list_df.rename(columns={'First_name':'first_name',
                                                     'Email':'email'
                                                     })
            return new_list_df

        except:
            error_message = 'failed reading file {0}\n'.format(self.file_path)
            print(error_message)