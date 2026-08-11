# Libraries
import requests, json, datetime, time
import pandas as pd
import datetime
import modules.constants as const

# SM Permissions: view collectors and view surveys
# Real tokens live in modules/constants.py (gitignored, not committed) as
# const.SM_API_TOKENS -- keep it that way, this file must stay commit-safe.
headers = {
    'Authorization': "bearer {0}".format(const.SM_API_TOKENS[0]),
    'Content-Type' : 'application/json'
    }
headers_1 = {
    'Authorization': "bearer {0}".format(const.SM_API_TOKENS[1]),
    'Content-Type' : 'application/json'
    }
headers_2 = {
    'Authorization': "bearer {0}".format(const.SM_API_TOKENS[2]),
    'Content-Type' : 'application/json'
    }
headers_3 = {
    'Authorization': "bearer {0}".format(const.SM_API_TOKENS[3]),
    'Content-Type' : 'application/json'
    }


# Dates
today = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
#yesterday = today - datetime.timedelta(1)

def GetValueFromSM(project_id, df):
    try:
        # Interacting with SM through the API
        url = 'https://api.surveymonkey.com/v3/collectors/{0}'.format(project_id)
        response = requests.get(url, headers=headers)
        resp_dict = response.json()

        if 'error' in resp_dict.keys():
            # Interacting with SM through the API
            url = 'https://api.surveymonkey.com/v3/collectors/{0}'.format(project_id)
            response = requests.get(url, headers=headers_1)
            resp_dict = response.json()      

            if 'error' in resp_dict.keys():
            # Interacting with SM through the API
                url = 'https://api.surveymonkey.com/v3/collectors/{0}'.format(project_id)
                response = requests.get(url, headers=headers_2)
                resp_dict = response.json()      

                if 'error' in resp_dict.keys():
                # Interacting with SM through the API
                    url = 'https://api.surveymonkey.com/v3/collectors/{0}'.format(project_id)
                    response = requests.get(url, headers=headers_3)
                    resp_dict = response.json()                 

        today_value = int(resp_dict['response_count'])
    except:
        project_name = df.loc[project_id]['project_name']
        print('failed getting the SM value for {0}'.format(project_name))
        print(resp_dict)
    return today_value

def UpdateSMReport():
    # Dates
    today = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
    #yesterday = today - datetime.timedelta(1)
    try:
        # Variables
        filename_out = const.SM_RESPONSES_DIR.joinpath('sm_responses.xlsx')

        # Fixing df before entering the for loop
        df = pd.read_excel(filename_out)
        df = df.set_index('project_id')
        df = df.drop(['TOTAL'],axis=1)
        df = df.fillna(0)
        df_columns = df.columns.to_list()

        if today in df_columns:
            df = df.drop([today],axis=1)

        summed_df = df.sum(axis=1,numeric_only=True)

        for project_id in df.index:
            today_value = GetValueFromSM(project_id,df)
            df.loc[project_id, today] =  today_value - summed_df[project_id]
            df.loc[project_id, 'TOTAL'] = today_value
        
        # Fixing dataframe to include only integer values
        cols = list(df.columns)
        cols.pop(0)
        df[cols] = df[cols].astype('int')

        # Saving to csv
        df.to_excel(filename_out)

        # Fixing dataframe to print resume
        df = df.set_index('project_name')
        print(df.iloc[:,-3:])

    except:
        error_message = 'failed updating the SM report'
        print(error_message)