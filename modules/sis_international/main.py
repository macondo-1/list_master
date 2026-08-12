import json
from pathlib import Path
import sqlite3
import csv
from collections import defaultdict
import modules.constants as const
import pandas as pd
import matplotlib.pyplot as plt
from datetime import date
import modules.email_bison_api.main as bison
import re

def get_working_jsons():
        working_jsons = []
        base_path = const.NEW_PATH_TO_PROJECST_DB
        for x in base_path.iterdir():
            if x.is_dir():
                file_path = x.joinpath('{}.json'.format(x.name))

                if file_path.exists():

                    with open(file_path) as file:
                        project_dict = json.load(file)
                        if 'survey_monkey' in project_dict.keys() or 'qualtrics' in project_dict.keys():
                            working_jsons.append(file_path)

        return working_jsons

def get_all_projects_mailing_and_recruits_numbers(projects_list:list):
    search_string = '|'.join(projects_list)
    working_jsons = get_working_jsons()
    search_string = re.compile(search_string)
    working_jsons = [x for x in working_jsons if re.search(search_string,str(x))]
    print(working_jsons)
    # working_jsons = [x for x in working_jsons if 'intralink' in str(x)]
    for working_json in working_jsons:
        template_name = working_json.name.split('.')[0]
        projet_name = template_name.split('_',1)[1]
        project_number = template_name.split('_',1)[0]
        handler = Project(projet_name, project_number)
        handler.load_project()
        handler.get_projects_mailing_and_recruits_numbers()

def test():
    pass


class Project:
    cur_path = const.BASE_PATH
    projects_base_path = const.projects_base_path
    db_file_path = const.db_file_path

    base_filter_dict = {
        'country':[None],
        'state':[None],
        'city':[None],
        'gender':[None],
        'age':[None],
        'ethnicity':[None],
        'nationality':[None],
        'zip_code':[None],
        'job_title':[None],
        'education':[None],
        'company_name':[None],
        'projects_id':[None],
        'file_name':[None],
        'last_contact_date':[None],
    }

    def __init__(self, projet_name = '', project_number = ''):

        if projet_name == '' and project_number == '':
            self.name = input('Project name: ')
            self.number = input('Project number: ')
            self.project_manager = input("Project manager's name: ")
            self.greenarrow_server = input("Green Arrow server: ")
            self.greenarrow_template_name = '{0}_{1}'.format(self.number[:-1], self.name) # modify: this might not be necessary if it can be built out of the attributes
        else:
            self.name = projet_name
            self.number = project_number

    def save_project(self):
        """
        Saves the project attributes into a json
        """
        directory_name = '{0}_{1}'.format(self.number, self.name)
        project_path = self.projects_base_path.joinpath(directory_name)
        if not project_path.is_dir():
            project_path.mkdir()

        file_name = '{}.json'.format(directory_name)
        json_path = project_path.joinpath(file_name)
        # modify: create this dict out of iterating over the attributes
        data = {
            'name':self.name,
            'number':self.number,
            'project_manager':self.project_manager,
            'greenarrow_server':self.greenarrow_server,
            'greenarrow_template_name':self.greenarrow_template_name
        }
        with open(json_path, 'w') as file:
            json.dump(data, file, indent=4)

        print('file saved to {}'.format(json_path))

    def load_project(self) -> dict:
        """
        Reads a json with the project attributes
        returns the data as a dictionary
        """
        directory_name = '{0}_{1}'.format(self.number, self.name)
        project_path = self.projects_base_path.joinpath(directory_name)
        file_name = '{}.json'.format(directory_name)
        json_path = project_path.joinpath(file_name)
        if json_path.exists():
            with open(json_path, 'r') as file:
                project_dict = json.load(file)
        else:
            print('project file does not exists, try creating it.')
            project_dict = None

        self.project_dict = project_dict

        return project_dict

    def load_project_filter(self):
        """
        Reads a csv file with the project filters
        returns it as a dictionary with column names as keys and a list of keywords as value
        """
        directory_name = '{0}_{1}'.format(self.number, self.name)
        project_path = self.projects_base_path.joinpath(directory_name)
        file_name = '{}_filter.csv'.format(directory_name)
        csv_path = project_path.joinpath(file_name)
        print(csv_path)
        column_dict = defaultdict(set)  # Use set for deduplication

        with open(csv_path, newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                for key, value in row.items():
                    if value:  # Skip empty/null
                        cleaned_value = value.strip().lower()
                        if cleaned_value:
                            column_dict[key].add(cleaned_value)

        # Convert sets back to lists
        return {key: list(values) for key, values in column_dict.items()}

    def retrieve_records_from_db(self,full_query):
        """
        Reads the filters for the project
        parses the database
        saves matching records as a csv in project's folder
        returns the matching records
        """
        conn = sqlite3.connect(self.db_file_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(full_query)
        results = cursor.fetchall()
        conn.close()

        # Convert each row to a dictionary
        results = [dict(row) for row in results]

        # for row in results:
        #     print(row)

        return results

    def build_sqlite_query(self, filter_dict, table_name='survey_monkey'):
        base_query = "SELECT * FROM {}".format(table_name)
        conditions = []

        for field, values in filter_dict.items():
            
            if values == [None]:
                continue

            field_conditions = []
            for value in values:
                value = value.strip().lower()
                field_conditions.append(f"LOWER({field}) LIKE '%{value}%'")
            if field_conditions:
                conditions.append(f"({' OR '.join(field_conditions)})")

        if conditions:
            full_query = base_query + " WHERE " + " AND ".join(conditions)
        else:
            full_query = base_query

        return full_query

    def save_sql_results_to_csv(self, results):
        """
        receives the results from the sql query
        saves it as csv
        """
        fieldnames = results[0].keys()
        file_name = self.cur_path.joinpath('test.csv') # modify: need to select the dir path and file name
        with open(file_name, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

    def save_mail_message(self,mail_message):
        directory_name = '{0}_{1}'.format(self.number, self.name)
        project_path = self.projects_base_path.joinpath(directory_name)
        filename = project_path.joinpath('{}.txt'.format(directory_name))
        with open(filename,'w') as file:
            file.write(mail_message)

    def get_projects_mailing_and_recruits_numbers(self): # CHECK: this function is a mess, need to re-factor it
            """
            Downloads latests numbers for a given project
            plots the data
            saves plots in projects folder
            """
            try:
                # read blast master
                df_blast_master = pd.read_excel(const.BLAST_MASTER_PATH)
                template_name = '{0}_{1}'.format(self.number, self.name) # CHECK seems unnecessary if the json has a template key (but might be not well constructed, check it first)
                internal_project_number = df_blast_master[df_blast_master['template_name'] == template_name].loc[:,'Unnamed: 1']

                directory_name = '{0}_{1}'.format(self.number, self.name)
                project_path = self.projects_base_path.joinpath(directory_name)

                # input project number
                self.number

                # retrieve qualtrics or survey monkey # CHECK: might put an OR in the if to avoid the elif
                if 'qualtrics' in self.project_dict.keys():
                    for collector in self.project_dict['qualtrics']['collectors']:
                        if collector['name'] == 'mailmerge':
                            mailmerge_dict = collector
                        elif collector['name'] == 'bison':
                            bison_dict = collector
                elif 'survey_monkey' in self.project_dict.keys():
                    for collector in self.project_dict['survey_monkey']['collectors']:
                        if collector['name'] == 'mailmerge':
                            mailmerge_dict = collector
                        elif collector['name'] == 'bison':
                            bison_dict = collector


                # get bison ids
                email_bison_ids = self.project_dict['email_bison_ids']
                start_date = self.project_dict['launch_date']
                today = date.today()
                end_date = today.strftime('%Y-%m-%d')
                # retrieve bison
                campaign_id = email_bison_ids[0]
                # CHECK: some bison will have nultiple campaigns, need to retrieve them all and sum the sends
                # for loop this next line and sum right away, no need to get more into the loop ->
                response = bison.get_full_normalized_stats_by_date(start_date, end_date, campaign_id)

                for x in response['data']:
                    if x['label'] == 'Sent':
                        response = x
                bison_send_log = response['dates']
                bison_send_log = pd.DataFrame(bison_send_log, columns=['timestamp','sent emails'])

                # maybe up to here ->

                # reads mm log
                log_df = pd.read_csv(const.LOG_PATH)

                # reads qualtrics or survey monkey
                mailmerge_dict
                bison_dict

                # fixing bison log
                bison_send_log
                bison_send_log['timestamp'] = pd.to_datetime(bison_send_log['timestamp'], format='mixed')

                # getting bison results
                bison_results_dict = bison_dict['responses_counts']
                del bison_results_dict['total']
                df_bison_results = pd.DataFrame(list(bison_results_dict.items()), columns=['timestamp','registered recruits'])
                df_bison_results['timestamp'] = pd.to_datetime(df_bison_results['timestamp'])
                df_bison_results.set_index('timestamp', inplace=True)
                # merges both dfs to plot one against the other
                bison_mailing_and_results_df = pd.merge(bison_send_log,df_bison_results, on='timestamp',how='outer')
                bison_mailing_and_results_df.fillna(0,inplace=True)
                bison_mailing_and_results_df = bison_mailing_and_results_df.reset_index()#.rename(columns={"timestamp": "timestamp"})
                csv_path = project_path.joinpath('bison_results.csv')
                bison_mailing_and_results_df.to_csv(csv_path)
                # plots total mails (mm log or bison log) vs total recruits (qualtrics or survey monkey)
                df_bison = bison_mailing_and_results_df
                # Plot 'sales' on the primary y-axis
                ax = df_bison.plot(x='timestamp', y='sent emails', legend=True)
                # Plot 'leads' on the secondary y-axis
                ax2 = df_bison.plot(x='timestamp', y='registered recruits', secondary_y=True, ax=ax, legend=True, color='red')
                # Set labels for both y-axes
                ax.set_ylabel('Total Mails')
                ax2.set_ylabel('Total Recruits')
                # Add a title
                plt.title('Mails and Recruits Over Time')
                # plt.show()
                figure_path = project_path.joinpath('bison_results.jpg')
                plt.savefig(figure_path, format='jpg')
                plt.close()


                # getting mm sends
                log_df['timestamp'] = pd.to_datetime(log_df['timestamp'], format='mixed', dayfirst=True) # dayfirst needed? CHECK
                log_df = log_df[log_df.project_number == int(internal_project_number.iloc[0])] # CHECK: need to manage the +1 part, it needs to get the data maybe from the excel or add that data to the json (best to json)
                log_df = log_df[['project_number','timestamp']]
                project_mailing = pd.DataFrame(log_df.value_counts('timestamp'))
                project_mailing.rename(columns={'count':'sent emails'}, inplace=True)

                # getting mm results
                results_dict = mailmerge_dict['responses_counts']
                del results_dict['total']
                df_results = pd.DataFrame(list(results_dict.items()), columns=['timestamp','registered recruits'])
                df_results['timestamp'] = pd.to_datetime(df_results['timestamp'])
                df_results.set_index('timestamp', inplace=True)

                # merges both dfs to plot one against the other
                mailing_and_results_df = pd.merge(project_mailing,df_results, on='timestamp',how='outer')
                mailing_and_results_df.fillna(0,inplace=True)
                mailing_and_results_df = mailing_and_results_df.reset_index()#.rename(columns={"timestamp": "timestamp"})
                csv_path = project_path.joinpath('mailmerge_results.csv')
                mailing_and_results_df.to_csv(csv_path)

                # plots total mails (mm log or bison log) vs total recruits (qualtrics or survey monkey)
                df = mailing_and_results_df

                # Plot 'sales' on the primary y-axis
                ax = df.plot(x='timestamp', y='sent emails', legend=True)

                # Plot 'leads' on the secondary y-axis
                ax2 = df.plot(x='timestamp', y='registered recruits', secondary_y=True, ax=ax, legend=True, color='red')

                # Set labels for both y-axes
                ax.set_ylabel('Total Mails')
                ax2.set_ylabel('Total Recruits')

                # Add a title
                plt.title('Mails and Recruits Over Time')
                # plt.show()

                figure_path = project_path.joinpath('mailmerge_results.jpg')
                plt.savefig(figure_path, format='jpg')
                plt.close()
                print('done: ', template_name)
            except Exception as e:
                print('something failed for {0}_{1}'.format(self.number, self.name))
                print(e)

