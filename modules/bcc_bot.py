import pandas as pd
import smtplib
from email.mime.text import MIMEText
import random
import time
import datetime
import glob
import os
import numpy as np
import modules.constants as const
import traceback

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options

# Real passwords live in modules/constants.py (gitignored, not committed) as
# const.EMAIL_PASSWORDS -- keep it that way, this file must stay commit-safe.

BLAST_MASTER_PATH = const.BLAST_MASTER_PATH

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
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    # service=Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(options=options) #, service=service
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

def send_email_selenium(to_email, message, driver, cc, reply_to='', attachment_paths=None):


    wait = WebDriverWait(driver, 80)
    # Creating new mail

    # for debugging headless mode
    # driver.save_screenshot("debug_headless.png")
    # print(driver.title)
    # print(driver.current_url)
    # print(driver.page_source[:5000])

    button = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@title="Write a new message (N)"]')))
    button.click()

    # Writing TO email
    input_field = wait.until(EC.element_to_be_clickable((By.XPATH, '//input[@aria-label="To"]')))
    input_field.send_keys(to_email)
    input_field.send_keys(Keys.RETURN)
    time.sleep(2)
    input_field.send_keys(Keys.RETURN)

    if cc:
        # Writing CC
        input_field = wait.until(EC.element_to_be_clickable((By.XPATH, '//input[@aria-label="Cc"]')))
        input_field.send_keys(cc)
        input_field.send_keys(Keys.RETURN)
        time.sleep(5)
        input_field.send_keys(Keys.RETURN)

    if reply_to:
        # Writing Reply-To
        input_field = wait.until(EC.element_to_be_clickable((By.XPATH, '//input[@aria-label="Reply-to"]')))
        input_field.send_keys(reply_to)
        input_field.send_keys(Keys.RETURN)
        time.sleep(2)
        input_field.send_keys(Keys.RETURN)

    # Writing SUBJECT of the email
    subject_field = wait.until(EC.element_to_be_clickable((By.XPATH, '//input[@aria-label="Subject," or @placeholder="Add a subject"]')))
    time.sleep(1)
    subject_field.send_keys(message.split('\n',1)[0])

    message_body = wait.until(EC.element_to_be_clickable((By.XPATH, '//div[@aria-label="Message body"]')))
    time.sleep(1)
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

    # Writing message body
    with open(const.BCC_FOOTER_PATH, 'r', encoding='utf-8') as file:
        footer = file.read()
    message_1 = message.split('\n',1)[1] # + '\n\n' + footer
    message_body.send_keys(message_1)

    # Attaching files
    if attachment_paths:
        for path in attachment_paths:
            attach_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@title="Attach"]')))
            attach_button.click()
            time.sleep(1)
            file_input = wait.until(EC.presence_of_element_located((By.XPATH, '//input[@type="file"]')))
            driver.execute_script("arguments[0].style.display = 'block';", file_input)
            file_input.send_keys(path)
            time.sleep(3)

    # Click send
    button = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@aria-label="Send"]')))
    # button = driver.find_element(By.XPATH, '//button[@aria-label="Send"]')
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

def update_log(df):
    df = df[['Email','project_number','status','timestamp']]
    log_path = const.LOG_PATH
    df_log = pd.read_csv(log_path)

    df_log = pd.concat([df_log,df], ignore_index=True)
    df_log.to_csv(log_path, index=False)

def send_emails_selenium(cc):
    today = datetime.date.today()
    FROM_EMAIL = input("\nAvailable emails:\n\nnancy@sisinternational.com\nanna@sisinternational.com\njohn@sisinternational.com\ncharles@sisinternational.com\n\nSelect email: ")
    slice_size = int(input("Select how many emails you want to send out: "))
    list_filename = const.MAILING_PATH.joinpath('mm_list.csv')

    PASSWORD = const.EMAIL_PASSWORDS[FROM_EMAIL]

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
            traceback.print_exc()
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

def send_emails_selenium_concurrency(cc, FROM_EMAIL, slice_size, reply_to='', attachment_paths=None):
    today = datetime.date.today()
    list_filename = const.MAILING_PATH.joinpath('mm_list.csv')

    print('[BCC] step 1/5 - looking up password for {0}...'.format(FROM_EMAIL))
    if FROM_EMAIL not in const.EMAIL_PASSWORDS:
        raise ValueError(
            "'{0}' has no entry in const.EMAIL_PASSWORDS (modules/constants.py) "
            "-- check for typos in the email you selected, or add the account's "
            "password to that dict".format(FROM_EMAIL)
        )
    PASSWORD = const.EMAIL_PASSWORDS[FROM_EMAIL]

    print('[BCC] step 2/5 - reading mailing list from {0}...'.format(list_filename))
    if not list_filename.exists():
        raise FileNotFoundError(
            "{0} doesn't exist -- run 'Create MM list' first".format(list_filename)
        )
    mailing_list = fixing_df_bis(list_filename, slice_size) # This function reads a csv as a dataframe and then turns it into a dict
    if not mailing_list:
        raise ValueError('{0} has no records left to send -- nothing to do'.format(list_filename))
    print('[BCC] {0} email(s) loaded for this batch'.format(len(mailing_list)))

    new_df = pd.DataFrame(mailing_list)                     # which seems unecessary if I'm turning it into a DF back again here
    new_df['timestamp'] = today

    print('[BCC] step 3/5 - launching browser...')
    driver = initialize_selenium()

    print('[BCC] step 4/5 - signing in to OWA as {0}...'.format(FROM_EMAIL))
    try:
        signin_selenium(driver, FROM_EMAIL, PASSWORD)
        time.sleep(2)
    except Exception as error:
        driver.quit()
        raise RuntimeError('failed signing in to OWA as {0}: {1}'.format(FROM_EMAIL, error)) from error
    print('[BCC] signed in successfully')

    print('[BCC] step 5/5 - sending {0} email(s)...'.format(len(mailing_list)))
    n = 1
    for mail in mailing_list:
        try:
            send_email_selenium(mail['Email'], mail['message'], driver, cc, reply_to, attachment_paths)
            message = '\nemail sent to {email}\n{total_sent} sent emails in total'.format(email=mail['Email'], total_sent=n)
            print(message)
            n += 1

            df_index = new_df[new_df['Email'] == mail['Email']].index
            new_df.loc[df_index,'status'] = 'sent'


        except Exception as error:
            print('\nfailed sending email to: {email}'.format(email=mail['Email']))
            print("An exception occurred:", error) # An exception occurred: division by zero
            traceback.print_exc()
            try:
                driver.refresh()
            except Exception as refresh_error:
                print('[BCC] failed refreshing the browser after the error above: {0}'.format(refresh_error))

            df_index = new_df[new_df['Email'] == mail['Email']].index
            new_df.loc[df_index,'status'] = 'failed'

        except KeyboardInterrupt:
            new_df['status'] = new_df['status'].replace(np.nan,'failed')
            print('[BCC] interrupted -- saving progress to the log before exiting...')
            update_log(new_df)
            print('failed emails saved')
            raise

    #condition = new_df['status'] == 'failed'
    print('[BCC] updating log...')
    update_log(new_df)
    print('[BCC] log updated')
    driver.close()
    driver.quit()
    sent_count = int((new_df['status'] == 'sent').sum())
    print('[BCC] done -- {0}/{1} sent successfully'.format(sent_count, len(mailing_list)))

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