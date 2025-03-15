import smtplib
import constants as const
import pandas as pd
from email.mime.text import MIMEText
import os
import random
import time
import datetime
from bcc_bot import update_log
from bcc_bot import fixing_df_bis
import numpy as np

today = datetime.date.today()

class SMTP():
    """
    This class handles everything related to sending out emails through smtp connection
    """

    def __init__(self):

        df = pd.read_csv(const.GODADDY_EMAILS_PATH)
        
        self.slize_size = int(input('How many emails?: '))
        self.footer_path = const.FOOTER_PATH
        self.mm_list_path = const.MM_LIST_PATH
        self.from_name = 'Ruth Stanat'
        self.host = const.SMTP_HOST
        self.port = const.SMTP_PORT
        self.email = input(const.GODADDY_EMAILS)
        self.password = const.GODADDY_PASSWORD
        self.mailserver = smtplib.SMTP(self.host, self.port)
        try:
            self.mailserver.starttls()
            self.mailserver.ehlo()
            self.mailserver.login(self.email, self.password)
        except:
            message = 'failed connnecting to {0}, please try another account'.format(self.email)
            print(message)
    

    def create_mail_msg_object(self, message, to_email):
        """
        Creates a email.message object so be sent through SMTP
        """

        from_string = '{0} <{1}>'.format(self.from_name, self.email)

        msg = MIMEText(message.split('\n',1)[1])
        msg.set_unixfrom('author')
        msg['From'] = from_string
        msg['To'] = to_email
        msg['Subject'] = message.split('\n',1)[0]

        return msg


    def send_emails_smtp(self):
        """
        Fixes the message adding the footer to it
        Loops over the mailing list and sends out an email per record
        """
        
        mailing_list = fixing_df_bis(self.mm_list_path, self.slize_size)          # This function reads a csv as a dataframe and then turns it into a dict
        new_df = pd.DataFrame(mailing_list)                     # which seems unecessary if I'm turning it into a DF back again here
        new_df['timestamp'] = today

        with open(self.footer_path, 'r', encoding='utf-8') as file:
            footer = file.read()
        footer = footer.format(FROM_NAME=self.from_name)

        # This loop is what actually sends out the mails
        n = 1
        for mail in mailing_list:
            message_1 = mail['message'] + '\n\n' + footer
            msg = self.create_mail_msg_object(message_1, mail['Email'])

            try:
                self.mailserver.sendmail(msg['From'], msg['To'], msg.as_string())
                df_index = new_df[new_df['Email'] == mail['Email']].index
                new_df.loc[df_index,'status'] = 'sent'

            except smtplib.SMTPRecipientsRefused:
                print(f"Failed to send email to {mail}. Adding it to the list of failed emails.")
                df_index = new_df[new_df['Email'] == mail['Email']].index
                new_df.loc[df_index,'status'] = 'failed'
                update_log(new_df)
                print('failed emails saved')

            except KeyboardInterrupt:
                new_df['status'] = new_df['status'].replace(np.nan,'failed')
                update_log(new_df)
                print('failed emails saved')

            except:
                new_df['status'] = new_df['status'].replace(np.nan,'failed')
                update_log(new_df)
                print('failed emails saved')    

            wait_time = random.randint(1, 6)
            message = '\nemail sent to {email}\n{total_sent} sent emails in total'.format(email=mail['Email'], total_sent=n)
            print(message)
            time.sleep(wait_time)
            n += 1

        print('updating log')
        update_log(new_df)   
        print('log updated')
        self.mailserver.quit()
        