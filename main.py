import os
import sys
from lists import list_
from greenarrow_bot import import_list
from greenarrow_bot import send_campaigns_testing
import constants as const
from bcc_bot import send_emails_selenium
from sm_api import UpdateSMReport
from display import Display
from database import Database
import database
from smtp_bot import SMTP
from database import Log
import glob
from lists import NewList
import pandas as pd

def mailmerge(mm_len):    
    handler = Database()
    handler.extract_for_project(mm_len)
    list_.CreateMMList()
    send_emails_selenium()

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



if __name__ == '__main__':

    cli_display = Display()
    choice = ''
    os.system('clear')
    while choice != 'q':
        choice = cli_display.get_user_choice()

        if choice == '1':
            list_.CleanLists()
            
        elif choice == '2':
            import_list()

        elif choice == '3': #Save list to valid_emails database 
            save_multiple_new_list_to_valid_db()

        elif choice == '4':
            campaign_speed = int(input('select campaigns speed: '))
            send_campaigns_testing(campaign_speed)

        elif choice == '5': # extract blast needs from valid emails database
            # Get dictionary from blast needs sheet
            path = '/Users/albertoruizcajiga/Downloads/blast_needs.csv'
            df = pd.read_csv(path)
            df['project_id'] = df['project_id'].astype('str')
            df['value'] = df['value'].astype('int')
            dict_from_df = df.set_index('project_id')['value'].to_dict()

            # Extract records from valid database
            db_handler = Database()
            db_df = db_handler.ConnectToValidEmailsDB()
            db_handler.ExtractListValidEmails(db_df, dict_from_df)
        
        elif choice == '6':
            handler = Database()
            handler.UpdateBlockedEmails()
          
        elif choice == '7':
            list_.CreateMMList()

        elif choice == '8':
            cc = input('Type email you want to CC: ')
            send_emails_selenium(cc)

        elif choice == '9':
            UpdateSMReport()

        elif choice == '10':
            mm_len = int(input("Input Desired mm length size: "))
            handler = Database()
            handler.extract_for_project(mm_len)

        elif choice == '11':
            handler = Database()
            handler.GetDailyBlastNeeds()

        elif choice == '12':
            mm_len = int(input("Input Desired mm length size: "))
            mailmerge(mm_len)

        elif choice == '13':
            list_.DecomposeMMList()

        elif choice == '14':
            try:
                mailserver = SMTP()
                mailserver.send_emails_smtp()
            except:
                message = 'something failed, please try again.'
                print(message)

        elif choice == '15':
            input_date = input("input date (DD/MM/YY)\nClick enter to get today's sumary: ")
            logger = Log()
            logger.mailmerge_summary(input_date)

        elif choice == '16':
            list_.GetURLsFromSnoToHunt()

        elif choice == '17':
            list_.CleanListManually()

        elif choice == '18':
            list_.concat_lists()

        elif choice == '19':
            list_.Deduper()

        elif choice == '20':
            db_handler = Database()
            db_df = db_handler.ConnectToValidEmailsDB()
            db_handler.update_valid_emails_against_mm_log(db_df)

        elif choice == '21': # extract blast needs from valid emails database
            # Get dictionary from blast needs sheet
            path = '/Users/albertoruizcajiga/Downloads/blast_needs.csv'
            df = pd.read_csv(path)
            df['project_id'] = df['project_id'].astype('str')
            df['value'] = df['value'].astype('int')
            dict_from_df = df.set_index('project_id')['value'].to_dict()

            # Extract records from valid database
            db_handler = Database()
            db_df = db_handler.ConnectToValidEmailsDB()
            db_handler.ExtractListValidEmails_to_blast(db_df, dict_from_df)
            
        elif choice == 'q':
            cli_display.quit()


