from selenium import webdriver
from selenium.webdriver.common.by import By
import asyncio

import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
import xlsxwriter
import os
import requests
import openpyxl as ox
import os
import urllib.request
from selenium.common.exceptions import SessionNotCreatedException
from selenium.common.exceptions import WebDriverException
import chromedriver_autoinstaller
import urllib.request
import zipfile
import multiprocessing
class Email_parser():


    def __init__(self,url,reqs,names):

        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument("--log-level=3")

        try:
            self.driver = webdriver.Chrome(options=options)
            self.driver.get(url)

        except:

            print("Обновление драйвера")
            chromedriver_autoinstaller.install()
            self.driver = webdriver.Chrome(options=options)
            self.driver.get(url)
        self.url = url
        self.reqs = reqs
        self.names = names


    def click(self,ad):
        line = self.driver.find_element(By.TAG_NAME,'input').send_keys(ad)
        but = self.driver.find_element(By.TAG_NAME,'button').click()


    def entrepreneur(self):
        k = 0

        i = 1
        while True:
            try:
                try:

                    self.driver.find_element(By.XPATH,f"//div[@class='my-2 border-l-2 border-transparent hover:border-indigo-600 pl-2 flex flex-row print:border-b print:border-l-0 print:border-indigo-600'][{i}]/div[@class='flex-grow']/span[@class='text-red-500']")
                    i+=1
                except:
                    isentrepreneur = self.driver.find_element(By.XPATH,f"//div[@class='my-2 border-l-2 border-transparent hover:border-indigo-600 pl-2 flex flex-row print:border-b print:border-l-0 print:border-indigo-600'][{i}]/div[@class='flex-grow']/a[@class='text-lg bl']").text
                    if ('ИП' in isentrepreneur) :
                        i+=1
                    else:
                        entrepreneur = self.driver.find_element(By.XPATH,f"//div[@class='my-2 border-l-2 border-transparent hover:border-indigo-600 pl-2 flex flex-row print:border-b print:border-l-0 print:border-indigo-600'][{i}]/div[@class='flex-grow']/a[@class='text-lg bl']").get_attribute('href')
                        return entrepreneur


            except :
                if k == 5:
                    return ''
                k+=1
                self.driver.refresh()
                continue


    def business_links(self,url):
        site = requests.get(url).text
        soup = BeautifulSoup(site, 'html.parser')
        links = soup.find('li')
        links = links.find_all('a')
        links = [i['href'] for i in links]
        links = ['https://e-ecolog.ru/'+i for i in links]
        links.append(self.director())



        return links
    def session_for_requests(self):
        data = {
            '_token': '-empty-',
            'email': 'bubunduc@gmail.com',
            'password': 'm5:f7XH7ikm-jzN',
            'button': ''
        }
        with requests.Session() as session:
            # --- first GET page ---
            headers = {
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36'
            }
            response = session.get(url='https://e-ecolog.ru/login', headers=headers)
            # print(response.text)

            # --- search fresh token in HTML ---

            soup = BeautifulSoup(response.text, 'html.parser')
            token = soup.find('input', {'name': "_token"})['value']

            # --- run POST with new token ---

            data['_token'] = token

            response = session.post(url='https://e-ecolog.ru/login', data=data, headers=headers)
        return session
    async def call_url(self,url):

        async with aiohttp.ClientSession(auth= aiohttp.BasicAuth('bubunduc@gmail.com','m5:f7XH7ikm-jzN')) as session:

            async with session.get(url) as response:
                data = await response.text()
                soup = BeautifulSoup(data, 'html.parser')
                mails = soup.find_all(itemprop="email")

                print()
                if mails == []:
                    return ''
                else:
                    mails = [i.string for i in mails]
                    return mails


    def asynced_search(self,links):
        lins = []
        for url in links:
            try:
                if '//person' in url:
                    url = url.replace('/person','entity')
                    lins.append(url)
                else:
                    url = url.replace('person', 'entity')
                    lins.append(url)

            except:
                lins.append(url)
        loop = asyncio.get_event_loop()
        futures = [loop.create_task(self.call_url(url)) for url in lins]
        loop.run_until_complete(asyncio.gather(*futures))
        results = [task.result() for task in futures]
        results =  [x for x in results if x!='']
        return results


    def director(self):
        director_mail = self.driver.find_elements(By.CLASS_NAME,'bl')[1]
        return director_mail.get_attribute('href')


    def mails_1(self,req):

        self.driver.get('https://e-ecolog.ru/')

        self.click(f'{req[0]} {req[1]}')

        url = self.entrepreneur()
        if url == '':
            self.driver.get(self.driver.current_url)
            url = self.driver.current_url

        else:

            self.driver.get(url)

        try:

            links = self.business_links(url)
            main_adress = self.driver.find_elements(By.PARTIAL_LINK_TEXT, '@')[:-1]

            main_adress = [i.text for i in main_adress]
            ready_mails = []

            mails = self.asynced_search(links)
            for ma in mails:
                for r in ma:
                    ready_mails.append(r)
            for main in main_adress:
                ready_mails.append(main)
            ready_mails = set(ready_mails)
            ready_mails = list(ready_mails)

            self.driver.get(self.url)
            return ready_mails
        except:
            #self.driver.get(self.url)
            return []


    def mails_2(self,req):
        flag = False
        self.driver.get('https://vbankcenter.ru/contragent/search?searchStr=' + req[0])
        try:
            pages = self.driver.find_elements(By.XPATH,"//li[@class='page-item mx-1.5 mb-1.5 first:ml-0 last:mr-0']")[:-1]
            pages = int(pages[-1].text)+1
        except:
            pages = 2
        for j in range(1,pages):
            self.driver.get('https://vbankcenter.ru/contragent/search?searchStr='+req[0]+f'&page={j}')

            cards = self.driver.find_elements(By.XPATH,"//li[@class='pageable-item']/div[@class='p-7 mb-5 rounded border border-night-sky-200']")

            for i in range(0,len(cards)):

                adress = self.driver.find_element(By.XPATH,f"//li[@class='pageable-item'][{i+1}]/div[@class='p-7 mb-5 rounded border border-night-sky-200']/gweb-search-results-card[@class='gweb-search-results-card flex flex-col lg:flex-row justify-between']/article[@class='lg:w-3/4']/div[@class='overlap pt-2 lg:pt-0 text-sm lg:text-base']/div[@class='flex items-baseline pt-1.5']/gweb-schema-for-region/address[@class='text-truncate not-italic']/p[@class='whitespace-pre-wrap mb-0']/span[1]").text[:-1]

                if (adress == req[1].split(', ')[0]):
                    url =self.driver.find_element(By.XPATH,f"//li[@class='pageable-item'][{i+1}]/div[@class='p-7 mb-5 rounded border border-night-sky-200']/gweb-search-results-card[@class='gweb-search-results-card flex flex-col lg:flex-row justify-between']/article[@class='lg:w-3/4']/h5[@class='flex items-center pb-2 lg:pb-3.5 text-xl']/a[@class='overlap text-blue']").get_attribute('href')+'/contacts'
                    self.driver.get(url)
                    try:
                        self.driver.find_element(By.XPATH,"//div[@class='w-full md:w-1/2 md:w-full max-w-lg mt-8 md:mt-0']/div[@class='text-blue cursor-pointer']/span").click()

                        mails = self.driver.find_elements(By.XPATH,"//div[@class='w-full md:w-1/2 md:w-full max-w-lg mt-8 md:mt-0']/p[@class='mt-3']")
                        mails = [mail.text for mail in mails]

                        return mails

                    except:
                        self.driver.get(self.driver.current_url)
                        try:
                            mails = self.driver.find_element(By.XPATH,"//div[@class='w-full md:w-1/2 md:w-full max-w-lg mt-8 md:mt-0']/p[@class='mt-3']").text
                            return [mails]
                        except:
                            return []



        if flag == False:

            return []


    def x(self):
        all_mails = []
        session = self.session_for_requests()
        for req in self.reqs:
            mails_from_site_1 = []
            mails_from_site_2 = []
            try:
                mails_from_site_1 = self.mails_1(req)
            except:
                print(f"В {req} на сайте https://e-ecolog.ru/ возникла ошибка")
            try:
                mails_from_site_2 = self.mails_2(req)
            except:
                print(f"В {req} на сайте https://vbankcenter.ru возникла ошибка ")
            print(list(set(mails_from_site_1+mails_from_site_2)))
            mails = list(set(mails_from_site_1+mails_from_site_2))
            all_mails.append(', '.join(mails))
        self.driver.close()
        self.driver.quit()
        
        return all_mails
    def check_driver(self):
        destination = 'edgedriver_win64.zip'
        url = 'https://developer.microsoft.com/en-us/microsoft-edge/tools/webdriver/'

        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser")
        url = soup.find_all("a", class_="driver-download__link")[4]['href']
        urllib.request.urlretrieve(url, destination)
        with zipfile.ZipFile(destination, 'r') as zip_ref:
            zip_ref.extract('msedgedriver.exe')





if __name__ == "__main__":
    file_name = input('Введите название файла: \n')
    cols = [9, 10]
    requisits = pd.read_excel(f'{file_name}.xlsx',usecols=cols,header=None,sheet_name="сегодня")


    adresses = requisits[10].tolist()[2:]

    names = requisits[9].tolist()[2:]



    reqs = []
    for i in range(0,len(adresses)):
        reqs.append([names[i],adresses[i]])

    f = Email_parser('https://e-ecolog.ru/',reqs,names)

    emails = f.x()

    df = pd.DataFrame({'1':emails+["",""]})


    def update_spreadsheet(path: str, _df, starcol: int = 12, startrow: int = 3, sheet_name: str = "сегодня"):
        '''

        :param path: Путь до файла Excel
        :param _df: Датафрейм Pandas для записи
        :param starcol: Стартовая колонка в таблице листа Excel, куда буду писать данные
        :param startrow: Стартовая строка в таблице листа Excel, куда буду писать данные
        :param sheet_name: Имя листа в таблице Excel, куда буду писать данные
        :return:
        '''
        wb = ox.load_workbook(path)
        for ir in range(0, len(_df)):
            for ic in range(0, len(_df.iloc[ir])):
                wb[sheet_name].cell(startrow + ir, starcol + ic).value = _df.iloc[ir][ic]
        wb.save(path)
    name = f'{file_name}.xlsx'
    path = os.path.dirname(__file__)+rf'{name}'
    update_spreadsheet(name,df)
    r = input('Парсинг завершен!!!')






