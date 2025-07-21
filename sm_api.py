# Libraries
import requests, json, datetime, time
import pandas as pd
import datetime
import constants as const

# SM Permissions: view collectors and view surveys
headers = {
    'Authorization': "bearer aH53VJ63H0NmzlwgO-fYiwxjBnIwENTTmeCHpTG1b6EkU-0GxjYHWQSdpPMUyqC9eDZ6pL2iYnGveWZWWdscCurLdbJFVWcDBvjCy124izOVS5ZSuKsy6miRbKFFCJY2",
    'Content-Type' : 'application/json'
    }
headers_1 = {
    'Authorization': "bearer lMsBrzQ04CQ89elrGwz-gwuIGR1IKxXHZORz448crIJwTo.N7tqFZNH1D5hElJgu9cMTNce.EV2F1aqgxQvteXY3iHEtyxtFOu6zPvCSYK6rP3ZB4SBMh1.jg6ii.UcQ",
    'Content-Type' : 'application/json'
    }
headers_2 = {
    'Authorization': "bearer MKGIdImxFYBgyHKOyrFs-Jf9Dmu8R6UcT06DT8vBWMkZdXWjh7AnH8aquHhiGDqWxiE4O8LiuMt5kBxm.9k-40Lsa0W370wPVoYw0suY6pE5vbi07IILZWpv5EH82NN.",
    'Content-Type' : 'application/json'
    }

headers_3 = {
    'Authorization': "bearer klkujOLcGEOka0vbad-wiHt4tnpLG00RnEzloLe-dRok9OJ7qFuZJ0--sWcI5JWkrg3zmo4g8mNMQqZAKm2auOFl-rTucX.bTPIExluWNpMF4QvDndz0JCM35M780xWu",
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