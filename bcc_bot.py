import pandas as pd
import smtplib
from email.mime.text import MIMEText
import random
import time
import datetime
import glob
import os
import numpy as np

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains


email_passwords_dict = {'nancy@sisinternational.com':'Challenge2025!!',
                        'john@sisinternational.com':'Challenge2025!!',
                        'anna@sisinternational.com':'Challenge2025!!',
                        'charles@sisinternational.com':'Challenge2025!!',
                        'sisfieldwork@sisinternational.com':'Challenge2025!!'
                        }

BLAST_MASTER_PATH = '/Users/albertoruizcajiga/Documents/Documents - Alberto’s MacBook Air/SIS final/blast_master_good_final.xlsx'



def fixing_df(list_filename, MESSAGE, FROM_NAME, slice_size):
    df = pd.read_csv(list_filename)

    #with open(msg_filename, 'r', encoding='utf-8') as file:
     #   content = file.read()
        
    with open('footer.txt', 'r', encoding='utf-8') as file:
        footer = file.read()

    message = MESSAGE

    df['message'] = df.apply(lambda row: message.format(First_name=row['First_name'], FROM_NAME=FROM_NAME), axis=1)
    df[slice_size:].to_csv(list_filename, index = False)
    mailing_list = df[:slice_size].to_dict('records')

    return mailing_list

def create_mm_list(FROM_NAME):

    #reading file names
    file_extension = '.csv'
    all_filenames = [i for i in glob.glob(f"*{file_extension}")]

    #iterating over each csv
    for file_name in all_filenames:
        p_number = int(file_name.split('_')[0])
        project_info = get_project_info(file_name)
        MESSAGE = project_info[0]['Blast Message']

        df = pd.read_csv(file_name)
        df = df.rename(columns={'first_name':'First_name',
                                'email':'Email',
                                })

        df['message'] = df.apply(lambda row: MESSAGE.format(First_name=row['First_name'], FROM_NAME=FROM_NAME), axis=1)
        df['project_number'] = p_number
        df.to_csv(file_name, index = False)
    
    concatenated_df = pd.concat([pd.read_csv(f,low_memory=False) for f in all_filenames])
    concatenated_df = concatenated_df.sort_values(by='Email', ascending=True)
    concatenated_df.to_csv('mm_list.csv', index = False)


def start_smtp_connection(SMPT, PORT, EMAIL, PASSWORD):
    mailserver = smtplib.SMTP(SMPT, PORT)
    mailserver.starttls()
    mailserver.ehlo()
    mailserver.login(EMAIL, PASSWORD)
    return mailserver

def create_mail_msg_object(message, FROM_NAME, FROM_EMAIL, to_email):

    from_string = '{FROM_NAME} <{FROM_EMAIL}>'.format(FROM_NAME=FROM_NAME, FROM_EMAIL=FROM_EMAIL)

    msg = MIMEText(message.split('\n',1)[1])  
    msg.set_unixfrom('author')
    msg['From'] = from_string
    msg['To'] = to_email
    msg['Subject'] = message.split('\n',1)[0]

    return msg

def initialize_selenium():
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.implicitly_wait(6)

    return driver

def signin_selenium(driver, FROM_EMAIL, PASSWORD):
    driver.get('https://west.exch031.serverdata.net/owa/auth/logon.aspx?replaceCurrent=1&url=https%3a%2f%2fwest.exch031.serverdata.net%2fowa%2f')
    username = driver.find_element('id','username')
    password = driver.find_element('id','password')
    username.send_keys(FROM_EMAIL)
    password.send_keys(PASSWORD)
    sign_in = driver.find_element(By.CLASS_NAME,'signinbutton')
    sign_in.click()

def send_email_selenium(to_email, message, driver, cc):


    wait = WebDriverWait(driver, 80)
    # Creating new mail
    button = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@title="Write a new message (N)"]')))
    button.click()

    # Writing TO email
    input_field = wait.until(EC.element_to_be_clickable((By.XPATH, '//input[@aria-label="To"]')))
    input_field.send_keys(to_email)
    input_field.send_keys(Keys.RETURN)
    time.sleep(5)
    input_field.send_keys(Keys.RETURN)

    if cc:
        # Writing CC
        input_field = wait.until(EC.element_to_be_clickable((By.XPATH, '//input[@aria-label="Cc"]')))
        input_field.send_keys(cc)
        input_field.send_keys(Keys.RETURN)
        time.sleep(5)
        input_field.send_keys(Keys.RETURN)

    # Writing SUBJECT of the email
    subject_field = wait.until(EC.element_to_be_clickable((By.XPATH, '//input[@aria-label="Subject," or @placeholder="Add a subject"]')))
    subject_field.send_keys(message.split('\n',1)[0])

    message_body = wait.until(EC.element_to_be_clickable((By.XPATH, '//div[@aria-label="Message body"]')))
    message_body.send_keys(Keys.UP)
    message_body.send_keys(Keys.UP)
    message_body.send_keys(Keys.UP)
    message_body.send_keys(Keys.UP)
    message_body.send_keys(Keys.UP)
    message_body.send_keys(Keys.UP)
    message_body.send_keys(Keys.UP)
    message_body.send_keys(Keys.UP)
    message_body.send_keys(Keys.UP)
    message_body.send_keys(Keys.UP)
    message_body.send_keys(Keys.UP)
    message_body.send_keys(Keys.UP)
    message_body.send_keys(Keys.UP)
    message_body.send_keys(Keys.UP)
    message_body.send_keys(Keys.UP)
    message_body.send_keys(Keys.UP)
    message_body.send_keys(message.split('\n',1)[1])

    # Click send
    button = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@aria-label="Send"]')))
    #button = driver.find_element(By.XPATH, '//button[@aria-label="Send"]')
    button.click()

    time.sleep(random.randint(2, 5))

def get_project_info(file_name):
        p_number = int(file_name.split('_')[0])

        df = pd.read_excel(BLAST_MASTER_PATH)
        df.set_index('Unnamed: 1', inplace = True)

        project_info = df.loc[p_number]
        FROM_EMAIL = project_info['bcc_email']
        return project_info, FROM_EMAIL 

def fixing_df_bis(list_filename, slice_size):
    df = pd.read_csv(list_filename)

    if df[slice_size:].empty:
        os.remove(list_filename)
    else:
        df[slice_size:].to_csv(list_filename, index = False)

    mailing_list = df[:slice_size].to_dict('records')
    return mailing_list

"""
def send_emails_smtp(need_to_fix_list):

    mailserver = start_smtp_connection(SMPT, PORT, FROM_EMAIL, PASSWORD)

    if need_to_fix_list:
        project_info = get_project_info(list_filename)
        MESSAGE = project_info[0]['Blast Message']
        mailing_list = fixing_df(list_filename, MESSAGE, FROM_NAME,slice_size)
    else:
        mailing_list = fixing_df_bis(list_filename, slice_size)
    # -----------------
    #mailing_list = fixing_df(list_filename, MESSAGE, FROM_NAME,slice_size)
    #mailing_list = fixing_df_bis(list_filename, slice_size)
    # -----------------

    with open('footer.txt', 'r', encoding='utf-8') as file:
        footer = file.read()
    footer = footer.format(FROM_NAME=FROM_NAME)

    n = 1
    for mail in mailing_list:
        message_1 = mail['message'] + '\n\n' + footer
        msg = create_mail_msg_object(message_1, FROM_NAME, FROM_EMAIL, mail['Email'])
        mailserver.sendmail(msg['From'], msg['To'], msg.as_string())
        wait_time = random.randint(3, 15)
        message = '\nemail sent to {email}\n{total_sent} sent emails in total'.format(email=mail['Email'], total_sent=n)
        print(message)
        time.sleep(wait_time)
        n += 1
        
    mailserver.quit()
"""

def update_log(df):
    df = df[['Email','project_number','status','timestamp']]
    log_path = '/Users/albertoruizcajiga/Documents/final_final/list_master/databases/log/log.csv'
    df_log = pd.read_csv(log_path)

    df_log = pd.concat([df_log,df], ignore_index=True)
    df_log.to_csv(log_path, index=False)


def send_emails_selenium(cc):
    today = datetime.date.today()
    os.chdir('/Users/albertoruizcajiga/Documents/Documents - Alberto’s MacBook Air/final_final/mailing_bot')
    FROM_EMAIL = input("\nAvailable emails:\n\nnancy@sisinternational.com\nanna@sisinternational.com\njohn@sisinternational.com\ncharles@sisinternational.com\n\nSelect email: ")
    slice_size = int(input("Select how many emails you want to send out: "))
    list_filename = 'mm_list.csv'

    PASSWORD = email_passwords_dict[FROM_EMAIL]

    mailing_list = fixing_df_bis(list_filename, slice_size) # This function reads a csv as a dataframe and then turns it into a dict
    new_df = pd.DataFrame(mailing_list)                     # which seems unecessary if I'm turning it into a DF back again here
    new_df['timestamp'] = today

    try:
        driver = initialize_selenium()
        signin_selenium(driver, FROM_EMAIL, PASSWORD)
        time.sleep(2)
    except:
        print('failed loging into selenium')


    n = 1
    for mail in mailing_list:

        try:
            send_email_selenium(mail['Email'], mail['message'], driver, cc)
            message = '\nemail sent to {email}\n{total_sent} sent emails in total'.format(email=mail['Email'], total_sent=n)
            print(message)
            n += 1

            df_index = new_df[new_df['Email'] == mail['Email']].index
            new_df.loc[df_index,'status'] = 'sent'
            

        except Exception as error:
            print('\nfailed sending email to: {email}'.format(email=mail['Email']))
            print("An exception occurred:", error) # An exception occurred: division by zero
            driver.refresh()

            df_index = new_df[new_df['Email'] == mail['Email']].index
            new_df.loc[df_index,'status'] = 'failed'

        except KeyboardInterrupt:
            new_df['status'] = new_df['status'].replace(np.nan,'failed')
            update_log(new_df)
            print('failed emails saved')

        except UnboundLocalError:
            print('update drivers json to continue')

    #condition = new_df['status'] == 'failed'
    update_log(new_df)
    driver.close()
    driver.quit()



slice_size = 10


list_filename = '1009031_cleaned.csv'
#msg_filename = '1000292_uk_kurun.txt' DELETE
FROM_NAME = 'Ruth Stanat'

# SMTP credentials --------------------- SMTP credentials 
SMPT = 'smtp.office365.com'
PORT = 587
PASSWORD = 'Happy1920Daddy!!'
# SMTP credentials --------------------- SMTP credentials 

# EMAIL ACCOUNTS
# SMTP -------------------------------------------- SMTP
#FROM_EMAIL = 'ruthstanat@sisconsulting.org'
#FROM_EMAIL = 'ruthstanat@sis-insight.net'
# SMTP -------------------------------------------- SMTP

# Selenium -------------------------------------------- Selenium
#FROM_EMAIL = 'nancy@sisinternational.com'
#FROM_EMAIL = 'anna@sisinternational.com'
#FROM_EMAIL = 'DBini@sisinternational.com'
# Selenium -------------------------------------------- Selenium


