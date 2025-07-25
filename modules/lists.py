import pandas as pd
import modules.constants as const
import re
import csv
import os
import sqlite3
import datetime
from datetime import time
from time import sleep
import glob
import numpy as np
import shutil
from pathlib import Path
import datetime


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
            file_path = Path(file_path)
            #file_name = file_path.split('.')[0]
            file_extension = file_path.suffix

            if file_extension == '.csv':
                df = pd.read_csv(file_path, on_bad_lines='skip',low_memory=False,encoding='latin-1',dtype=object, header=header_row)
            elif file_extension in ['.xls', '.xlsx']:
                df = pd.read_excel(file_path,dtype=object, header=header_row)
            else:
                df = None
                print("file extension not compatible: {0}".format(file_extension))

            return df

        except:
            print('failed reading file\n')
    
    def save_raw_file_to_project_dir(file_path):
        """
        Saves the file to the project's directory
        """
        try:
            file_path = Path(file_path)
            file_name = file_path.stem

            projects_dir_path = const.NEW_PATH_TO_PROJECST_DB

            df_blast_master = pd.read_excel(const.BLAST_MASTER_PATH)
            project_info1 = list_.get_project_info(file_name,df_blast_master)
            project_path = Path(project_info1['template_name'])
            x = datetime.datetime.now()
            x = x.strftime('%Y%m%d')
            destination_filename = projects_dir_path.joinpath(project_path.stem, 'lists', file_path.stem + '_' + x + file_path.suffix)

            directory_path = projects_dir_path.joinpath(project_path.stem, 'lists')
            directory_path.mkdir(parents=True, exist_ok=True)


            print('destination_filename',destination_filename)
            print('file_path',file_path)



            shutil.copy(file_path, destination_filename)

        except Exception as e:
            print(e)



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
        all_read_paths = [i for i in const.PROCESSING_FOLDER.glob('*.csv')]
        all_read_paths += [i for i in const.PROCESSING_FOLDER.glob('*.xlsx')]

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


    def CleanLists(save_to_project_dir):
        try:
            export = int(input('prepare to export? [0/1]: '))
            dedupe = input('dedupe from mm log? [y/n]: ')

            all_read_paths = [i for i in const.PROCESSING_FOLDER.glob('*.csv')]
            all_read_paths += [i for i in const.PROCESSING_FOLDER.glob('*.xlsx')]
            print(const.PROCESSING_FOLDER)
            print(all_read_paths)

            for read_path in all_read_paths:
                try:
                    if save_to_project_dir == 'y':
                        list_.save_raw_file_to_project_dir(read_path)
                    df = list_.ReadList(read_path)
                    columns = list(df.columns)
                    if 'quality' in columns:
                        df = df[df['quality'] == 'good']
                    df = list_.FixColumns(df)
                    df = list_.FixRecords(df)
                    df = list_.CleanBlacklisted(df)
                    if dedupe == 'y':
                        df = list_.DedupeFromLog(df)
                    if export:
                        df = df[['first_name','email']]
                    os.remove(read_path)

                    out_name = read_path.stem
                    out_name = out_name.replace(' ','_')
                    out_name = read_path.parent / '{}.csv'.format(out_name)
                    df.to_csv(out_name, index=False)

                except Exception as e:
                    error_message = 'failed cleaning: {0}'.format(read_path)
                    print(error_message)
                    print(e)
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
        df = pd.read_csv(const.LOG_PATH)
        sent_emails = df['Email']
    
    def CreateMMList():
        try:
            FROM_NAME = 'Ruth Stanat'

            #reading file names
            all_filenames = [i for i in const.MAILING_PATH.glob('*.csv')]
            df_blast_master = pd.read_excel(const.BLAST_MASTER_PATH)
            #df_blast_master = df_blast_master.set_index('Unnamed: 1')

            #iterating over each csv
            for file_name in all_filenames:
                p_number = file_name.name.split('_')[0]
                project_info1 = list_.get_project_info(file_name.name,df_blast_master)
                MESSAGE = project_info1['Blast Message']

                df = pd.read_csv(file_name)
                df = df.rename(columns={'first_name':'First_name',
                                        'email':'Email',
                                        })

                df = df.replace({'First_name':'None'},'Colleague')
                df['message'] = df.apply(lambda row: MESSAGE.format(First_name=row['First_name'], FROM_NAME=FROM_NAME), axis=1)
                df['project_number'] = p_number
                print(file_name)
                df.to_csv(file_name, index = False)
            
            concatenated_df = pd.concat([pd.read_csv(f,low_memory=False) for f in all_filenames])
            concatenated_df = concatenated_df.sort_values(by='Email', ascending=True)
            concatenated_df = concatenated_df[['Email','First_name','message','project_number']]
            
            filename_out = const.MAILING_PATH.joinpath('mm_list.csv')
            concatenated_df.to_csv(filename_out, index = False)
            mm_list_len = len(concatenated_df)
            print("\nnew mm list length: {mm_list_len}\n".format(mm_list_len=mm_list_len))

            for file_name in all_filenames:
                os.remove(file_name)
        except:
            print('failed creating the MM list, check all file names {0}'.format(file_name))

    def DecomposeMMList():
        try:
            filename = const.MAILING_PATH.joinpath('mm_list.csv')
            df = pd.read_csv(filename)

            projects_list = list(df['project_number'].unique())

            for project in projects_list:
                df_out = df[df['project_number'] == project]
                filename_out = const.MAILING_PATH.joinpath('{}_mm_list.csv'.format(project))
                df_out.to_csv(filename_out, index=False)

            os.remove(filename)
        except:
            error_message = 'failed decomposing mm list'
            print(error_message)

    def GetURLsFromSnoToHunt():
        max_chunk = 25000

        all_file_names = [i for i in const.PROCESSING_FOLDER.glob('*.csv')]


        combined_csv_data = pd.concat([pd.read_csv(file_name,usecols=['url']) for file_name in all_file_names])

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
        all_read_paths = [i for i in const.PROCESSING_FOLDER.glob('*.csv')]
        combined_csv_data = pd.concat([pd.read_csv(f,low_memory=False) for f in all_read_paths])
        combined_csv_data = combined_csv_data.drop_duplicates(subset=['email']).dropna(subset=['email'])
        filename_out = const.PROCESSING_FOLDER.joinpath('combined_files.csv')
        combined_csv_data.to_csv(filename_out, index = False)
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
        all_read_paths = [i for i in const.PROCESSING_FOLDER.glob('*.csv')]
        all_read_paths += [i for i in const.PROCESSING_FOLDER.glob('*.xlsx')]
        for x in all_read_paths:
            print(x.name)
        
        list_A = input('provide the name of the main list: ')
        list_B = input('provide the name of the dedupe list: ')

        list_A = const.PROCESSING_FOLDER / list_A
        df_a = pd.read_csv(list_A)
        list_B = const.PROCESSING_FOLDER / list_B
        df_b = pd.read_csv(list_B)

        df_deduped = df_a[~df_a['email'].isin(df_b['email'])]
        df_deduped.to_csv(const.PROCESSING_FOLDER / 'deduped.csv',index=False)

    def clean_against_email_bison_db():
        """
        Deletes records that were uploaded to email bison
        """
        all_read_paths = [i for i in const.PROCESSING_FOLDER.glob('*.csv')]
        all_read_paths += [i for i in const.PROCESSING_FOLDER.glob('*.xlsx')]
        email_bison_records_path = const.EMAIL_BISON_RECORDS_PATH
        email_bison_records_df = pd.read_csv(email_bison_records_path, low_memory=False)
        for x in all_read_paths:
            df = pd.read_csv(x)
            df[~df.email.isin(email_bison_records_df.email)].to_csv(x, index=False)

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

    def divide_list():
        """Reads a csv and divides it into the desired chunks"""
        
        chunks = int(input("Divide into how many csv's?: "))
        all_read_paths = [i for i in const.PROCESSING_FOLDER.glob('*.csv')]
        all_read_paths += [i for i in const.PROCESSING_FOLDER.glob('*.xlsx')]

        for read_path in all_read_paths:
            df = pd.read_csv(read_path)
            total_lenght = len(df)
            lenght_per_chunk = int(total_lenght/chunks)
            initial_chunk = 0
            final_chunk = lenght_per_chunk
            chunk_number = 0

            while total_lenght > 0:
                file_name = const.PROCESSING_FOLDER.joinpath('_{}.csv'.format(chunk_number))
                if int(chunk_number+1) == int(chunks):
                    df[initial_chunk:].to_csv(file_name, index=False)
                    total_lenght = 0
                else:
                    df[initial_chunk:final_chunk].to_csv(file_name, index=False)
                    total_lenght -= lenght_per_chunk
                initial_chunk += lenght_per_chunk
                final_chunk += lenght_per_chunk
                chunk_number += 1

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


