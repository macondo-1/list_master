#!/Users/albertoruizcajiga/Documents/final_final/list_master/.myenv/bin/python3

import os
import sys
import glob
import pandas as pd
from modules.lists import list_
from modules.greenarrow_bot import import_list
from modules.greenarrow_bot import send_campaigns_testing
import modules.constants as const
from modules.bcc_bot import send_emails_selenium_concurrency
from modules.sm_api import UpdateSMReport
from modules.display import Display
from modules.database import Database
import modules.database as database
from modules.smtp_bot import SMTP
from modules.database import Log
from modules.lists import NewList
import modules.email_bison_api.main as bison
from modules.sis_international.main import Project
from modules.sis_international.main import get_working_jsons
from modules.sis_international.main import get_all_projects_mailing_and_recruits_numbers
from modules.sis_international.main import test
import threading
from concurrent.futures import ThreadPoolExecutor
from modules.greenarrow_bot import get_active_jobs_table_as_list_
import time
import modules.sis_international.main as sis
from pathlib import Path
import json
from modules.utilities import *

# concurrency set up
cli_display = Display()
task_counter = 1
lock = threading.Lock()
executor = ThreadPoolExecutor(max_workers=4)

def mailmerge(mm_len):    
    handler = Database()
    handler.extract_for_project(mm_len)
    list_.CreateMMList()
    # send_emails_selenium()

def save_new_list_to_valid_db(file_name):
    os.chdir("/Users/albertoruizcajiga/Documents/Documents - Alberto’s MacBook Air/final_final/to_process")
    new_list = NewList(file_name)
    new_list_df = new_list.read_list()

    db_handler = Database()
    db_df = db_handler.ConnectToValidEmailsDB()
    db_handler.UpdateValidEmails(new_list_df, db_df)

def save_multiple_new_list_to_valid_db():
    os.chdir("/Users/albertoruizcajiga/Documents/Documents - Alberto’s MacBook Air/final_final/to_process")
    file_extension = '.csv'
    all_filenames = [i for i in glob.glob(f"*{file_extension}")]

    for file_name in all_filenames:
        try:
            save_new_list_to_valid_db(file_name)
        except:
            print('failed saving new list {0} to valid database'.format(file_name))
        else:
            os.remove(file_name)

def clean_save_bison_ga(df, read_path, save_to_project_dir, export, dedupe,campaign_id, list_file_name, campaign_speed, not_sent_in):
    list_.clean_lists_concurrency(df, read_path, save_to_project_dir, export, dedupe)
    bison.add_list_and_start_campaign_concurrency(campaign_id, list_file_name)
    list_.clean_against_email_bison_db()
    # Modify: need something to read if there are records to update to GA
    df_ga = pd.read_csv(read_path)
    if len(df_ga) != 0:
        import_list()
        active_jobs_list = get_active_jobs_table_as_list_(read_path)
        while read_path.name in active_jobs_list:
            time.sleep(60)
            active_jobs_list = get_active_jobs_table_as_list_(read_path)
        send_campaigns_testing(campaign_speed, not_sent_in)
        print('whole process completed for file {}'.format(list_file_name))
    else:
        print('nothing to upload to GA')


def main_concurrency():
    global task_counter
    choice = ''
    os.system('clear')

    while choice != 'q':
        choice = cli_display.get_user_choice()

        if choice == '1':

            all_read_paths = [i for i in const.PROCESSING_FOLDER.glob('*.csv')]
            all_read_paths += [i for i in const.PROCESSING_FOLDER.glob('*.xlsx')]

            save_to_project_dir = input('Save list to project dir? [y/n]: ')
            export = int(input('prepare to export? [0/1]: '))
            dedupe = input('dedupe from mm log? [y/n]: ')

            for read_path in all_read_paths:
                # Collect user input first
                df = list_.ReadList(read_path)
                # df = list_.FixColumns(df)
                df = list_.fix_columns(df)
                # Submit the task to the thread pool after input
                
                with lock:
                    current_task = task_counter
                    task_counter += 1

                print(f"[✓] Task {current_task} - clean list submitted.")
                executor.submit(list_.clean_lists_concurrency, df, read_path, save_to_project_dir, export, dedupe)

        elif choice == '2':
            with lock:
                current_task = task_counter
                task_counter += 1

            print(f"[✓] Task {current_task} - concat lists submitted.")
            executor.submit(list_.concat_lists)

        elif choice == '3':
            all_read_paths = [i for i in const.PROCESSING_FOLDER.glob('*.csv')]
            all_read_paths += [i for i in const.PROCESSING_FOLDER.glob('*.xlsx')]
            for x in all_read_paths:
                print(x.name)
            
            list_A = input('provide the name of the main list: ')
            list_B = input('provide the name of the dedupe list: ')

            with lock:
                current_task = task_counter
                task_counter += 1

            print(f"[✓] Task {current_task} - dedupe lists submitted.")
            executor.submit(list_.deduper_concurrency, list_A, list_B)

        elif choice == '4':
            chunks = int(input("Divide into how many csv's?: "))
            with lock:
                current_task = task_counter
                task_counter += 1

            print(f"[✓] Task {current_task} - divide list submitted.")
            executor.submit(list_.divide_list_concurrency, chunks)

        elif choice == '5':
            project_name = input('Project template name: ')
            db_handler = Database()
            with lock:
                current_task = task_counter
                task_counter += 1

            print(f"[✓] Task {current_task} - extract from database submitted.")
            executor.submit(db_handler.extract_projects_filter_from_internal_database_concurrency, project_name)
            
        elif choice == '6':
            mm_list_total_length = int(input('How many emails to send out?: '))
            with lock:
                current_task = task_counter
                task_counter += 1

            print(f"[✓] Task {current_task} - create mm list submitted.")
            executor.submit(list_.CreateMMList,mm_list_total_length)

        elif choice == '7':
            with lock:
                current_task = task_counter
                task_counter += 1

            print(f"[✓] Task {current_task} - decompose mm list submitted.")
            executor.submit(list_.DecomposeMMList)

        elif choice == '8':
            cc = input('Type email you want to CC: ')
            FROM_EMAIL = input("\nAvailable emails:\n\nnancy@sisinternational.com\nanna@sisinternational.com\njohn@sisinternational.com\ncharles@sisinternational.com\n\nSelect email: ")
            slice_size = int(input("Select how many emails you want to send out: "))
            with lock:
                current_task = task_counter
                task_counter += 1

            print(f"[✓] Task {current_task} - send BCC submitted.")
            executor.submit(send_emails_selenium_concurrency, cc, FROM_EMAIL, slice_size)

        elif choice == '9':
            mailserver = SMTP()
            with lock:
                current_task = task_counter
                task_counter += 1

            print(f"[✓] Task {current_task} - send SMTP submitted.")
            executor.submit(mailserver.send_emails_smtp)
        
        elif choice == '10':
            # with lock:
            #     current_task = task_counter
            #     task_counter += 1

            # print(f"[✓] Task {current_task} - print mailmerge summary submitted.")
            # executor.submit(Log.mailmerge_summary,'')
            input_date = input('Type the required date (enter for today) [DD/MM/YY]:')
            handler = Log()
            handler.mailmerge_summary(input_date)

        elif choice == '11':
            with lock:
                current_task = task_counter
                task_counter += 1

            print(f"[✓] Task {current_task} - import list to GA submitted.")
            executor.submit(import_list)

        elif choice == '12':
            campaign_speed = int(input('select campaigns speed: '))
            not_sent_in = input('Last sent: ')
            if not_sent_in == '':
                not_sent_in = 30
            not_sent_in = int(not_sent_in)

            with lock:
                current_task = task_counter
                task_counter += 1

            print(f"[✓] Task {current_task} - send GA blasts submitted.")
            executor.submit(send_campaigns_testing, campaign_speed, not_sent_in)

        elif choice == '13':
            with lock:
                current_task = task_counter
                task_counter += 1

            print(f"[✓] Task {current_task} - clean against bison submitted.")
            executor.submit(list_.clean_against_email_bison_db)

        elif choice == '14':
            campaign_id = int(input('Campaing id: '))
            list_file_name = input('List name: ')
            with lock:
                current_task = task_counter
                task_counter += 1

            print(f"[✓] Task {current_task} - add list to bison submitted.")
            executor.submit(bison.add_list_and_start_campaign_concurrency, campaign_id, list_file_name)

        elif choice == '15':
            campaign_name = input('Project template name: ')
            timezone = input('time zone: ')
            if not timezone:
                timezone = 'America/New_York'
            with lock:
                current_task = task_counter
                task_counter += 1

            print(f"[✓] Task {current_task} - create project in bison submitted.")
            executor.submit(bison.create_new_project_in_email_bison_concurrency, campaign_name, timezone)

        elif choice == '16':
            with lock:
                current_task = task_counter
                task_counter += 1

            print(f"[✓] Task {current_task} - restart bison campaigns submitted.")
            executor.submit(bison.restart_campaigns_schedule)

        elif choice == '17':
            start_date = input('start date (format: YYYY-MM-DD): ')
            end_date = input('end date (format: YYYY-MM-DD): ')
            campaign_id = input('campaign ID: ')

            with lock:
                current_task = task_counter
                task_counter += 1

            print(f"[✓] Task {current_task} - get_full_normalized_stats_by_date submitted.")
            executor.submit(bison.get_full_normalized_stats_by_date, start_date, end_date, campaign_id)

        elif choice == '18':
            handler = Database()
            emails_to_blocklist = input('Type the emails to block separated by commas:\n')
            with lock:
                current_task = task_counter
                task_counter += 1

            print(f"[✓] Task {current_task} - update blocked emails submitted.")
            #handler.update_blocked_emails_concurrency(emails_to_blocklist)
            executor.submit(handler.update_blocked_emails_concurrency, emails_to_blocklist)

        elif choice == '19':
            project = Project()
            mail_message = input('mail message: ')

            with lock:
                current_task = task_counter
                task_counter += 1

            print(f"[✓] Task {current_task} - save project submitted.")
            executor.submit(project.save_project)

            with lock:
                current_task = task_counter
                task_counter += 1

            print(f"[✓] Task {current_task} - save project submitted.")
            executor.submit(project.save_mail_message, mail_message)

        elif choice == '20':
            with lock:
                current_task = task_counter
                task_counter += 1

            print(f"[✓] Task {current_task} - graphing send emails vs recruits (not concurrent).")
            projects_list = input('Type project numbers or names separated by commas: ')
            projects_list = projects_list.split(',')
            get_all_projects_mailing_and_recruits_numbers(projects_list)
            # executor.submit(get_all_projects_mailing_and_recruits_numbers) # CHECK: impliment this to be compatible with the concurrent thread

        elif choice == '21':
            all_read_paths = [i for i in const.PROCESSING_FOLDER.glob('*.csv')]
            all_read_paths += [i for i in const.PROCESSING_FOLDER.glob('*.xlsx')]

            save_to_project_dir = input('Save list to project dir? [y/n]: ')
            export = int(input('prepare to export? [0/1]: '))
            dedupe = input('dedupe from mm log? [y/n]: ')
            campaign_id = input('bison campaign id: ')
            campaign_speed = input('ga campaign speed: ')
            not_sent_in = input('ga not sent in: ')

            for read_path in all_read_paths:
                # Collect user input first
                df = list_.ReadList(read_path)
                df = list_.FixColumns(df)
                # Submit the task to the thread pool after input
                
                with lock:
                    current_task = task_counter
                    task_counter += 1

                print(f"[✓] Task {current_task} - clean save bison GA list submitted.")
            #clean_save_bison_ga(df, read_path, save_to_project_dir, export, dedupe, campaign_id, read_path.name, campaign_speed, not_sent_in)
            executor.submit(clean_save_bison_ga, df, read_path, save_to_project_dir, export, dedupe, campaign_id, read_path.name, campaign_speed, not_sent_in)

        elif choice == 'q':
            cli_display.quit()
            break

        else:
            print("Invalid option. Try again.")

    print("Waiting for background tasks to finish...")
    executor.shutdown(wait=True)
    print("All tasks completed. Goodbye!")

if __name__ == '__main__':
    main_concurrency()