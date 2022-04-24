from __future__ import print_function

import os.path
import pickle


import multiprocessing

from google.auth.transport.requests import Request

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import date
import time
import xlsxwriter
from os.path import exists
import datetime
# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


def gmail_authenticate():
    creds = None
    # the file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)
    # if there are no (valid) credentials availablle, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('client.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # save the credentials for the next run
        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)
    return build('gmail', 'v1', credentials=creds)

# get the Gmail API service
service = gmail_authenticate()


messages = []
raw_mes = []
next_page_token = None
kolumn = 2
for _ in range(60):
    try:
        if next_page_token:
            msgs = service.users().messages().list(userId='me',maxResults=500, pageToken=next_page_token).execute()
            
        else:
            msgs = service.users().messages().list(userId='me',maxResults=500).execute()


        raw_mes+=msgs['messages']

        next_page_token = msgs['nextPageToken']
    except:
        break
length = len(raw_mes)

m_id = [msg['id'] for msg in raw_mes]  # get id of individual message

def get_mes(messag):
    red_mes = []

    head = []
    months = {'Jan': 1,
              'Feb': 2,
              'Mar': 3,
              'Apr': 4,
              'May': 5,
              'Jun': 6,
              'Jul': 7,
              'Aug': 8,
              'Sep': 9,
              'Oct': 10,
              'Nov': 11,
              'Dec': 12}
    try:

        message = service.users().messages().get(userId='me', id=messag).execute()
        payload = message['payload']
        header = payload['headers']
        head.append(header)
        type_mes = header[0]['name']

        if type_mes == 'MIME-Version':
            data = header[1]['value']
            data = data.split()
            d = data[1:4]
            try:
                time.sleep(0.2)

                return ['Исходяцее', date(int(d[2]),months[d[1]],int(d[0]))]

            except:
                time.sleep(0.1)
                pass

        else:
            try:
                d = header[1]['value'].split()[7:10]

                time.sleep(0.2)
                print(['Входящее', date(int(d[2]),months[d[1]],int(d[0]))])
                return ['Входящее', date(int(d[2]),months[d[1]],int(d[0]))]
            except:
                time.sleep(0.1)
                pass


    except HttpError:
            # If the error is a rate limit or connection error,
            # wait and try again.
        print('wait, too many Requests')
        time.sleep(15)



def get_dates(date):
    try:
        return date[1]
    except:
        return datetime.date(3000,2,1)

if __name__ == '__main__':

    print(1)


    with multiprocessing.Pool(multiprocessing.cpu_count()*2) as pool:

        new_item_list = [item for item in pool.map(get_mes,m_id)]
        pool.terminate()
        print(new_item_list)
    with multiprocessing.Pool(multiprocessing.cpu_count() * 2) as pool_date:
        dates = [d for d in pool_date.map(get_dates,new_item_list)]
        dates = set(dates)
        print(dates)
        dates = sorted(dates)
    data_to_write = []


    for da in dates:
        try:
            if da == datetime.date(3000,2,1):
                continue

            in_mes = new_item_list.count(['Входящее', da])
            out_mes = new_item_list.count(['Исходяцее', da])
            data_to_write.append([da,in_mes,out_mes])
        except:
            continue

    data_to_write = data_to_write[::-1]
    name ='Mail'
    count = 0
    if exists(name) == False:
        workbook = xlsxwriter.Workbook(f'{name}.xlsx')
        worksheet = workbook.add_worksheet()
    else:
        while exists(f'{name}.xlsx')== True:
            count+=1
            name = f'{name}_{count}.xlsx'
        workbook = xlsxwriter.Workbook(f'{name}.xlsx')
        worksheet = workbook.add_worksheet()
    worksheet.write('A1', 'Дата')
    worksheet.write('B1', 'Количество входящих')
    worksheet.write('C1','Количество исходящих')
    for i,(dt,in_m,out) in enumerate(data_to_write,start=2):
        worksheet.write(f'A{i}', str(dt))
        worksheet.write(f'B{i}', in_m)
        worksheet.write(f'C{i}', out)
    workbook.close()
    print('Документ успешно создан')






