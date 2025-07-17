from types import TracebackType
import constants as const
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from datetime import date
import datetime
import glob

from lists import list_

today = date.today()
datetime_obj = datetime.datetime.combine(today, datetime.time())


class Hetz_ga(webdriver.Chrome):
    def __init__(self,driver_path=r"C:\SeleniumDrivers", teardown=False):
        self.driver_path = driver_path
        self.teardown = teardown
        os.environ['PATH'] += self.driver_path
        #self.find_element_by_css_selector()
        #self.implicitly_wait(10)
        super(Hetz_ga, self).__init__()
        self.implicitly_wait(15)

    def land_login_page(self, login_page_url):
        try:
            self.get(login_page_url)
        except:
            print('failed landing login page')

    def sign_in(self,server_username,server_password):
        try:
            username = self.find_element('id','user_session_username')
            password = self.find_element('id','user_session_password')
            username.send_keys(server_username)
            password.send_keys(server_password)

            sign_in = self.find_element(By.CLASS_NAME,'button')
            sign_in.click()
        except:
            print('failed signing in')

    def land_new_list_page(self,new_list_page_url):
        self.get(new_list_page_url)




    def setup_list_name(self,List_name):
        list_name = self.find_element('id','mailing_list_name')
        List_name = List_name.split('.')[0]
        list_name.send_keys(List_name)


    def setup_sending_speed(self,Sending_speed=300):
        list_name = self.find_element('id','mailing_list_d_speed')
        list_name.send_keys(Sending_speed)


    def setup_from_name(self,From_name='Research'):
        list_name = self.find_element('id','mailing_list_d_from_name')
        list_name.send_keys(From_name)


    def setup_from_email(self,From_email):
        list_name = self.find_element('id','mailing_list_d_from_email')
        list_name.send_keys(From_email)


    def setup_virtual_mta(self,mta='Default-Adil'):
        list_name = self.find_element('id','mailing_list_d_virtual_mta_id')
        list_name.send_keys(mta)


    def setup_url_domain(self,Url_domain):
        list_name = self.find_element('id','mailing_list_d_url_domain_id')
        list_name.send_keys(Url_domain)


    def setup_bounce_email(self,Bounce_email):
        list_name = self.find_element('id','mailing_list_d_bounce_email_id')
        list_name.send_keys(Bounce_email)


    def create_list(self):
        button = self.find_element(By.XPATH,"//button[contains(text(), 'Create this mailing list')]")
        button.click()


    def land_import_page(self):
        button = self.find_element(By.XPATH,"//a[contains(text(), 'Import')]")
        button.click()


    def dont_update_suscribers(self):
        radio_button = self.find_element('id','subscriber_import_overwrite_subscribers_false')
        
        if not radio_button.is_selected():
            radio_button.click()

    def set_char_set(self):
        radio_button = self.find_element('id','subscriber_import_file_character_set_iso-8859-1')
        
        if not radio_button.is_selected():
            radio_button.click()


    def select_upload_file(self,File_path):
        try:
            file_input = self.find_element('id',"subscriber_import_file")
            file_input.send_keys(File_path)
        except:
            print('failed selecting the upload file')


    def continue_import(self):
        button = self.find_element(By.XPATH,"//button[contains(text(), 'Continue with this import')]")
        button.click()

    def schedule_import(self):
        button = self.find_element(By.XPATH,"//button[contains(text(), 'Schedule this import')]")
        button.click()

    def read_server_credentials(self,Server):
        try:
            filename_in = const.CREDENTIALS_PATH
            df = pd.read_csv(filename_in, low_memory=False)
            df.set_index('Unnamed: 0', inplace = True)
            Credentials = dict(df[Server])
            domains = Credentials['available_domains'].split(',')
            domain = domains[int(Credentials['next_domain'])]
            Credentials['url_domain'] = domain
            mail_domain = domain.split('.',1)[1]
            from_email = 'insights@{mail_domain}'.format(mail_domain=mail_domain)
            bounce_email = 'return@bounce.{mail_domain}'.format(mail_domain=mail_domain)
            Credentials['from_email'] = from_email
            Credentials['bounce_email'] = bounce_email
            
            # Updating the credentials file
            if int(df[Server]['next_domain']) + 1 == int(df[Server]['max_domain']) + 1:
                df.loc['next_domain', Server] = 0
            else:
                df.loc['next_domain', Server] = int(df.loc['next_domain', Server]) + 1
            df.to_csv(filename_in)
        except:
            print('failed reading the credentials')

        return Credentials
    
    # --------------------------

    def land_template_pages(self,template_page_url):
        self.get(template_page_url)

    def select_template(self,Project):
        template = self.find_element(By.XPATH,f"//a[contains(text(), '{Project}')]")
        template.click()

    def create_campaign(self):
        time.sleep(3)
        Edit_segment = self.find_element(By.LINK_TEXT,'Create a campaign from this template')
        Edit_segment.click()

    def name_campaign(self,campaign_name):
        Edit_segment = self.find_element(By.ID,'campaign_name')
        Edit_segment.send_keys(campaign_name)
    
    def select_mailing_list(self, list_name_1):
        Edit_segment = self.find_element(By.ID,'campaign_mailing_list_id')
        Edit_segment.send_keys(list_name_1)

    def create_campaign_1(self):
        time.sleep(3)
        button = self.find_element(By.XPATH,"//button[contains(text(), 'Create this campaign')]")
        button.click()

    def edit_segment(self):
        Edit_segment = self.find_element(By.LINK_TEXT,'Edit segment')
        Edit_segment.click()
    
    def add_criteria(self):
        add_button = self.find_element(By.CLASS_NAME,'ir')
        add_button.click()

    def not_sent_in(self,period_time):
        select2_box = self.find_element(By.CLASS_NAME, "select2-container")
        select2_box.click()
        desired_option = self.find_element(By.XPATH, "//div[contains(text(), 'Subscriber Most Recently Sent')]")
        desired_option.click()
        time.sleep(3)

        # Locate the input element by its class name (or use other locators as appropriate)
        input_element = self.find_element(By.CLASS_NAME, "input-medium")
        # Clear the current value
        input_element.clear()
        # Set the new value
        input_element.send_keys(period_time)

        #button = wait.until(EC.element_to_be_clickable((By.ID, "button_id")))


        Edit_segment = self.find_element(By.CLASS_NAME,'serialize-segmentation-criteria')
        Edit_segment.click()

    def click_save_segment_button(self):
        Edit_segment = self.find_element(By.CLASS_NAME,'serialize-segmentation-criteria')
        Edit_segment.click()



    def land_campaigns_page(self, campaigns_page_url):
        self.get(campaigns_page_url)


    def get_active_campaigns_freq_dict(self):
        # Gets a dictionary with the frequency of the active campaigns

        # Scroll to the "Campaigns in Progress" section if necessary
        self.execute_script("document.getElementById('campaigns-in-progress').scrollIntoView();")

        # Locate the table for "Campaigns in Progress"
        table = self.find_element(By.ID, "in-progress-campaigns-table")
        rows = table.find_elements(By.TAG_NAME, "tr")

        # List to hold all campaign details
        campaigns = []

        # Skip the first row if it's headers
        for row in rows[1:]:
            # Dictionary to store details of a single campaign
            campaign_dict = {}
            
            # Assuming each row has 'td' elements for campaign details
            columns = row.find_elements(By.TAG_NAME, "td")
            
            # Extracting details. Adjust the indices based on the table structure
            if len(columns) >= 4:  # Ensure there are enough columns
                campaign_dict['Name'] = columns[0].text.split('\n')[0]
                campaign_dict['List_Name'] = columns[0].text.split('\n')[1]
                campaign_dict['Segment'] = columns[1].text
                campaign_dict['Started'] = columns[2].text
                campaign_dict['Status'] = columns[3].text

                # Add the dictionary to the list
                campaigns.append(campaign_dict)

        # Now 'campaigns' list contains all the campaign details

        campaign_names_list = []
        project_numbers_list = []

        for campaign in campaigns:
            campaign_names_list.append(campaign['Name'])
            #project_number = campaign['Name'].split('_')[0]

            project_number = campaign['Name'].split('_')

            try:
                a = float(project_number[-1])
            except:
                a = project_number[-1]
                
            if isinstance(a, float):
                project_number = '_'.join(project_number[:-1])
            else:
                project_number = '_'.join(project_number[:-2])
                

            project_numbers_list.append(project_number)

        uniques = set(project_numbers_list)
        active_campaig_freq_dict = {}

        for x in uniques:
            active_campaig_freq_dict[x] = project_numbers_list.count(x)

        return active_campaig_freq_dict
    

    def get_active_campaigns_names(self):
        # Gets a dictionary with the frequency of the active campaigns

        # Scroll to the "Campaigns in Progress" section if necessary
        self.execute_script("document.getElementById('campaigns-in-progress').scrollIntoView();")

        # Locate the table for "Campaigns in Progress"
        table = self.find_element(By.ID, "in-progress-campaigns-table")
        rows = table.find_elements(By.TAG_NAME, "tr")

        # List to hold all campaign details
        campaigns = []
        campaign_name_list = []

        # Skip the first row if it's headers
        for row in rows[1:]:
            # Dictionary to store details of a single campaign
            campaign_dict = {}
            #campaign_name_list = []
            
            # Assuming each row has 'td' elements for campaign details
            columns = row.find_elements(By.TAG_NAME, "td")
            
            # Extracting details. Adjust the indices based on the table structure
            if len(columns) >= 4:  # Ensure there are enough columns
                #campaign_dict['Name'] = columns[0].text.split('\n')[0]
                campaign_name_list.append(columns[0].text.split('\n')[0])

                # Add the dictionary to the list
                campaigns.append(campaign_dict)

        return campaign_name_list

    
    def get_project_info(self, file_name, df_blastmaster):

        try:
            df_blastmaster1 = df_blastmaster.astype({'Unnamed: 1':str})
            df_blastmaster1['Unnamed: 1'] = df_blastmaster1['Unnamed: 1'].apply(lambda x: x.split('.')[0])
            df_blastmaster1 = df_blastmaster1.set_index('Unnamed: 1')
            p_number = str(file_name.split('_')[0])
            project_info = df_blastmaster1.loc[p_number]
            
        except:
            print('Error getting project information for {0}'.format(file_name))
        return project_info

    def get_project_info_from_pending_list(self, file_name):
        df['Pending lists'] == file_name
        p_number = int(file_name.split('_')[0])

        df = pd.read_excel(const.BLAST_MASTER_PATH)
        df.set_index('Unnamed: 1', inplace = True)
        project_info = df.loc[p_number]
        return project_info
    
    def edit_delivery(self):
        Edit_segment = self.find_element(By.CLASS_NAME,'campaign_dispatch_editable')
        Edit_segment.click()

    def set_speed(self,speed):
        Edit_segment = self.find_element(By.ID,'campaign_dispatch_attributes_speed')
        Edit_segment.clear()
        Edit_segment.send_keys(speed)

    def set_from_name(self):
        Edit_segment = self.find_element(By.ID,'campaign_dispatch_attributes_from_name')
        Edit_segment.clear()
        Edit_segment.send_keys('Research')

    def set_from_email(self,input_email):
        Edit_segment = self.find_element(By.ID,'campaign_dispatch_attributes_from_email')
        Edit_segment.clear()
        Edit_segment.send_keys(input_email)

    def set_virtual_mta(self,mta):
        Edit_segment = self.find_element(By.ID,'campaign_dispatch_attributes_virtual_mta_id')
        Edit_segment.send_keys(mta)

    def set_url_domain(self, url):
        Edit_segment = self.find_element(By.ID,'campaign_dispatch_attributes_url_domain_id')
        Edit_segment.send_keys(url)

    def set_tracking_off(self):
        button = self.find_element(By.ID, "campaign_dispatch_attributes_track_opens_false")
        button.click()

        button = self.find_element(By.ID, "campaign_dispatch_attributes_track_links_false")
        button.click()

    def set_bounce_email(self,email):
        Edit_segment = self.find_element(By.ID,'campaign_dispatch_attributes_bounce_email_id')
        Edit_segment.send_keys(email)

    def update_campaign(self):
        desired_option = self.find_element(By.XPATH, "//button[contains(text(), 'Update this campaign')]")
        desired_option.click()
    
    def send_campaign(self):
        time.sleep(3)
        #desired_option = self.find_element(By.LINK_TEXT, 'Send campaign')
        desired_option = self.find_elements(By.CLASS_NAME, 'button')
        desired_option[1].click()

    def schedule_campaign(self):
        time.sleep(3)
        desired_option = self.find_element(By.XPATH, "//button[contains(text(), 'Schedule this campaign')]")
        desired_option.click()

    def send_campaign_final(self):
        time.sleep(3)
        button = self.find_element(By.XPATH, "//button[@class='button' and @type='submit' and text()='Send this campaign']")
        button.click()
    
    def get_new_url(self):
        current_url = self.current_url
        current_url = current_url.split('#')
        new_url = current_url[0]
        return new_url


    def get_pending_lists(self):
        
        df = pd.read_excel(const.BLAST_MASTER_PATH)
        df.set_index('Unnamed: 1', inplace = True)
        df.rename({'Unnamed: 0':'project_number'}, axis='columns', inplace = True)

        todays_blasts = df[[datetime_obj,'project_number','Pending lists','template_name']].dropna(subset = datetime_obj)
        todays_blasts = todays_blasts[todays_blasts[datetime_obj] == 'x']
        todays_blasts=todays_blasts[['project_number','Pending lists','template_name']]

        todays_blasts['next_list'] = todays_blasts['Pending lists']

        #todays_blasts['next_list'] = todays_blasts['next_list'].apply()

        todays_blasts['today_blast'] = todays_blasts['project_number'].astype(str) + '_' + todays_blasts['next_list'].astype(str)
        pending_lists = list(todays_blasts['today_blast'])
        return pending_lists
    
    # MODIFY: This might be useless now
    # def delete_sent_pending_lists(self,pending_list):
    #     df = pd.read_excel(const.BLAST_MASTER_PATH)
    #     df.set_index('Unnamed: 1', inplace = True)
    #     df.rename({'Unnamed: 0':'project_number'}, axis='columns', inplace = True)

    #     df.replace(pending_list, None, inplace=True)

    #     df.to_excel('/Users/albertoruizcajiga/Desktop/blast_master_good_final_RENEWED.xlsx') 

    def pause_all_campaigns(self):
        self.execute_script("document.getElementById('campaigns-in-progress').scrollIntoView();")
        table = self.find_element(By.ID, "in-progress-campaigns-table")
        pause_buttons = table.find_elements(By.CLASS_NAME, "pause-button")
        number_of_buttons = len(pause_buttons)

        for i in range(number_of_buttons):
            # Refresh pause buttons list and click on the first one
            table = self.find_element(By.ID, "in-progress-campaigns-table")
            pause_buttons = table.find_elements(By.CLASS_NAME, "pause-button")
            pause_buttons[0].click()

            # Wait for the page to reload
            WebDriverWait(self, 10).until(lambda d: d.execute_script("return document.readyState") == "complete")

    
    def start_campaigns_in_reverse(self):
        self.execute_script("document.getElementById('campaigns-in-progress').scrollIntoView();")
        table = self.find_element(By.ID, "in-progress-campaigns-table")
        pause_buttons = table.find_elements(By.CLASS_NAME, "resume-button")
        number_of_buttons = len(pause_buttons)

        for i in range(number_of_buttons -1, -1, -1):
            # Refresh pause buttons list and click on the first one
            table = self.find_element(By.ID, "in-progress-campaigns-table")
            pause_buttons = table.find_elements(By.CLASS_NAME, "resume-button")
            pause_buttons[i].click()

            # Wait for the page to reload
            #wait_for_page_load(driver)  # Wait for page load completion
            WebDriverWait(self, 10).until(lambda d: d.execute_script("return document.readyState") == "complete")

    def land_query_lists_page(self, keyword):
        """Looks up a keyword in the GreenArrow list names"""
        query_url = 'https://aws-new.sisfocusgroups.com/ga/mailing_lists/?utf8=%E2%9C%93&q_name={0}&button='.format(keyword)
        self.get(query_url)

    def get_list_names_from_query(self):
        
        table = self.find_element(By.CSS_SELECTOR, '.data-table')
        rows = table.find_elements(By.TAG_NAME, 'tr')
        list_names = []

        for row in rows[1:]:
            columns = row.find_elements(By.TAG_NAME, 'td')
            list_names.append(columns[0].text)

        return list_names






def import_list():
    today = date.today()
    d1 = today.strftime("%Y%m%d")
    suffix = str('_' + d1)
    folder = const.ACTIVE_TO_PROCESS_PATH
    os.chdir(folder)
    to_blast_path = const.TO_BLAST_PATH
    to_blast_df = pd.read_csv(to_blast_path)

    file_extension = '.csv'
    all_filenames = [i for i in glob.glob(f"*{file_extension}")]

    
    with Hetz_ga() as bot:
        df_blastmaster = pd.read_excel(const.BLAST_MASTER_PATH)
        for file_name in all_filenames:

            # Not sure what's happening here ------
            # everything turns out to be a failed file, but it doesn't makes sense
            # since the function still works
            try:
                df = list_.ReadList(file_name)
                df = df.rename({'First_name':'first_name','Email':'email'})
                df = df.loc[:,['first_name','email']]
                df.to_csv(file_name, index = False)
            except:
                print('failed file: {0}'.format(file_name))
            # -------------------------------------

            try:
                project_info = bot.get_project_info(file_name,df_blastmaster)
                Server = project_info['GA']
                Template_name = project_info['template_name']
                #Campaign_name = Template_name + suffix
                List_name_2 = file_name.split('.')[0]
                List_name_2 = List_name_2.split('_', 1)[1]
                Credentials = bot.read_server_credentials(Server)
                bot.land_login_page(Credentials['login_page'])
                time.sleep(0.5)
                bot.sign_in(Credentials['username'],Credentials['password'])
                time.sleep(0.5)
                bot.land_new_list_page(Credentials['new_list_page'])
                time.sleep(0.5)
                bot.setup_list_name(List_name_2)
                time.sleep(0.5)
                bot.setup_sending_speed()
                time.sleep(0.5)
                bot.setup_from_name()
                time.sleep(0.5)
                bot.setup_from_email(Credentials['from_email'])
                time.sleep(0.5)
                bot.setup_virtual_mta()
                time.sleep(0.5)
                bot.setup_url_domain(Credentials['url_domain'])
                time.sleep(0.5)
                bot.setup_bounce_email(Credentials['bounce_email'])
                time.sleep(0.5)
                bot.create_list()
                time.sleep(0.5)
                bot.land_import_page()
                time.sleep(0.5)
                bot.dont_update_suscribers()
                time.sleep(0.5)
                bot.set_char_set()
                time.sleep(5)
                print('{0}/{1}'.format(folder,file_name))
                bot.select_upload_file('{0}/{1}'.format(folder,file_name))
                time.sleep(5)
                bot.continue_import()
                time.sleep(0.5)
                bot.continue_import()
                time.sleep(0.5)
                bot.schedule_import()
                time.sleep(0.5)
                to_blast_df.loc[len(to_blast_df.index)] = file_name
                os.remove(file_name)
                time.sleep(5)
                print('')
            except:
                print(f'failed uploading: {file_name}')
            
            to_blast_df.to_csv(to_blast_path, index=False)


def send_campaigns_testing(campaign_speed, not_sent_in=30):
    today = date.today()
    d1 = today.strftime("%Y%m%d")
    suffix = str('_' + d1)
    to_blast_path = const.TO_BLAST_PATH
    to_blast_df = pd.read_csv(to_blast_path)
    # Send out campaigns
    with Hetz_ga() as bot:
        sent_lists = []
        df_blastmaster = pd.read_excel(const.BLAST_MASTER_PATH)
        for file_name in to_blast_df['list_name']:
            try:
                project_info = bot.get_project_info(file_name,df_blastmaster)
                Server = project_info['GA']
                Template_name = project_info['template_name']
                Credentials = bot.read_server_credentials(Server)
                bot.land_login_page(Credentials['login_page'])
                time.sleep(0.5)
                bot.sign_in(Credentials['username'],Credentials['password'])
                time.sleep(2)
                bot.land_campaigns_page(Credentials['campaigns_page_url'])
                time.sleep(2)
                campaign_names_list = bot.get_active_campaigns_names()
                time.sleep(0.5)
                Campaign_name = Template_name + suffix

                # This is awful programming, solve it better

                if Campaign_name + '_I' in campaign_names_list:
                    Campaign_name = Template_name + suffix + '_J'

                if Campaign_name + '_H' in campaign_names_list:
                    Campaign_name = Template_name + suffix + '_I'
                
                elif Campaign_name + '_G' in campaign_names_list:
                    Campaign_name = Template_name + suffix + '_H'
                
                elif Campaign_name + '_F' in campaign_names_list:
                    Campaign_name = Template_name + suffix + '_G'
                
                elif Campaign_name + '_E' in campaign_names_list:
                    Campaign_name = Template_name + suffix + '_F'
                
                elif Campaign_name + '_D' in campaign_names_list:
                    Campaign_name = Template_name + suffix + '_E'
                
                elif Campaign_name + '_C' in campaign_names_list:
                    Campaign_name = Template_name + suffix + '_D'
                
                elif Campaign_name + '_B' in campaign_names_list:
                    Campaign_name = Template_name + suffix + '_C'
                
                elif Campaign_name in campaign_names_list:
                    Campaign_name = Template_name + suffix + '_B'


                List_name_2 = file_name.split('.')[0]
                List_name_2 = List_name_2.split('_', 1)[1]
                List_name_2 = List_name_2.split(' ')[0] #new
                
                bot.land_template_pages(Credentials['template_page'])
                time.sleep(0.5)
                bot.select_template(Template_name)
                time.sleep(0.5)
                bot.create_campaign()
                time.sleep(0.5)
                bot.name_campaign(Campaign_name)
                time.sleep(0.5)
                bot.select_mailing_list(List_name_2)
                time.sleep(0.5)
                bot.create_campaign_1()
                time.sleep(3)
                bot.edit_segment()
                time.sleep(0.5)
                bot.add_criteria()
                time.sleep(0.5)
                bot.not_sent_in(not_sent_in)
                #bot.click_save_segment_button()
                time.sleep(0.5)
                bot.edit_delivery()
                time.sleep(0.5)
                bot.set_speed(campaign_speed)
                time.sleep(0.5)
                bot.set_from_name()
                time.sleep(0.5)
                bot.set_from_email(Credentials['from_email'])
                time.sleep(0.5)
                bot.set_virtual_mta(Credentials['virtual_mta'])
                time.sleep(0.5)
                bot.set_url_domain(Credentials['url_domain'])
                time.sleep(0.5)
                #bot.set_tracking_off()
                #time.sleep(0.5)
                bot.set_bounce_email(Credentials['bounce_email'])
                time.sleep(0.5)
                bot.update_campaign()
                time.sleep(0.5)
                new_url = bot.get_new_url()
                time.sleep(0.5)
                bot.get(new_url)
                time.sleep(0.5)
                bot.send_campaign()
                time.sleep(0.5)
                bot.schedule_campaign()
                time.sleep(0.5)
                bot.send_campaign_final()
                time.sleep(0.5)
                print('')
                print(Campaign_name)
                print('')
                time.sleep(10)
                sent_lists.append(file_name)

            except:
                print('\nfailed file:')
                print(file_name)
                print(Campaign_name)
                print('')


        to_blast_df = to_blast_df[~to_blast_df['list_name'].isin(sent_lists)]
        to_blast_df.to_csv(to_blast_path, index=False)



if __name__ == '__main__':
    with Hetz_ga() as bot:
        Credentials = bot.read_server_credentials('new_aws_ga')
        bot.land_login_page(Credentials['login_page'])
        time.sleep(0.5)
        bot.sign_in(Credentials['username'],Credentials['password'])
        time.sleep(0.5)
        bot.land_query_lists_page('germany')
        time.sleep(0.5)
        list_names = bot.get_list_names_from_query()

        project_number = '1431082_'
        list_names = [project_number + x + '.csv\n' for x in list_names]
        filename = const.TO_BLAST_PATH
        with open(filename, 'a') as f:
            f.writelines(list_names)