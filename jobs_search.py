from urllib.request import urlopen
from bs4 import BeautifulSoup
import csv
from time import sleep
from typing import Tuple, TextIO, Any, List
from os import path
import random
import re
from nltk.corpus import stopwords
import pandas as pd

# import sys

date_start: str = ""
company_name: str = ""
title: str = ""
description: str = ""
company_url: str = ""
keyword: str = 'django'
number: int = 0
link: str = f"https://www.jobs.bg/front_job_search.php?frompage={number}&keywords%5B0%5D={keyword}#paging"
base_link: str = "https://www.jobs.bg/"
number: int = 0
# // TODO writer-a да го направя, да може да допълва.

""" number is 0, 15, 30 - 1st page, 2nd page, 3rd page """
keyword_save = keyword.replace('+', '_')
keyword_save = "all_programming_lang_14AVGUSTdjango"
file_path = f"jobs_search_{keyword_save}.csv"


file_path_languages_matches = f"languages\\laguages_matches.xlsx"
languages_modules_desc = pd.read_excel(file_path_languages_matches)
print(languages_modules_desc.head())
print(languages_modules_desc.columns)
languages_modules = languages_modules_desc.drop(['Description'], axis='columns')
print(languages_modules.columns)


""" dictionaries for programming words"""

program_languages = ['bash', 'r', 'python', 'java', 'c++', 'ruby', 'perl', 'matlab', 'javascript', 'scala', 'php']
analysis_software = ['excel', 'tableau', 'd3.js', 'sas', 'spss', 'd3', 'saas', 'pandas', 'numpy', 'scipy', 'sps',
                     'spotfire', 'scikits.learn', 'splunk', 'powerpoint', 'h2o']
bigdata_tool = ['hadoop', 'mapreduce', 'spark', 'pig', 'hive', 'shark', 'oozie', 'zookeeper', 'flume', 'mahout',
                'airflow']
databases = ['sql', 'nosql', 'hbase', 'cassandra', 'mongodb', 'mysql', 'mssql', 'postgresql', 'oracle db', 'rdbms']
big_data_tect_tools = ['spark', 'hdfs', 'jupyter', 'mlflow', 'airflow']
terminology = ['etl', 'big data', 'aws', 'azure']

overall_dict = program_languages + analysis_software + bigdata_tool + databases \
               + languages_modules['Programming languages'].values.tolist() \
               + languages_modules['Python modules'].values.tolist() + \
               languages_modules['Python ETL tools'].values.tolist() + \
               languages_modules['Python framewors'].values.tolist() + \
               languages_modules['The 30 Best Python Libraries and Packages for Beginners'].values.tolist() + \
               big_data_tect_tools + terminology

""" dictionaries for programming words"""


def create_csv_file(file_path: str, mode='w') -> Tuple[TextIO, Any]:
    csv_file = open(file_path, mode, newline='', encoding="utf-8")
    csv_writer = csv.writer(csv_file)
    if mode is 'w':
        # csv_writer.writerow(['date_start', 'link', 'company_name', 'title', 'description', 'company_url'])
        csv_writer.writerow(['date_start', 'title', 'lst_keywords', 'len_keywords', 'link', 'extracted'])
    return csv_file, csv_writer


if path.exists(file_path):
    print('The csv file exist')
    csv_file, csv_writer = create_csv_file(file_path, mode='a')
else:
    create_csv_file(file_path)
    csv_file, csv_writer = create_csv_file(file_path, mode='w')


def keywords_f(text):
    lines = (line.strip() for line in text.splitlines())  # break into line
    chunks = [phrase.strip() for line in lines for phrase in line.split("  ")]  # break multi-headlines into a line each
    text = ''.join(chunk for chunk in chunks if chunk).encode('utf-8')  # Get rid of all blank lines and ends of line
    # print(text)
    try:
        text = text.decode('unicode_escape').encode('ascii', 'ignore')  # Need this as some websites aren't formatted
        text = text.decode('utf-8')
    except Exception as e:
        print(e)
        return
    text = re.sub("[^a-zA-Z+3]", " ", text)
    text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)  # Fix spacing issue from merged words
    """ save in table may be """
    full_text = text.lower().split()  # Go to lower case and split them apart
    # //TODO I can save full text here
    """ save in table may be """
    stop_words = set(stopwords.words("english"))  # Filter out any stop words
    text = [w for w in full_text if not w in stop_words]
    text = list(set(text))  # only care about if a word appears, don't care about the frequency
    # 'python' in overall_dict # word
    keywords = [str(word) for word in text if word in overall_dict]  # if a skill keyword is found, return it.
    print(type(keywords))
    keywords = list(set(keywords))  # removing duplicate strings from list
    keywords = ','.join(map(str, keywords))
    return keywords


def extract_info(link: str, csv_writer: TextIO) -> None:
    html = urlopen(link)
    bs_obj_jobs = BeautifulSoup(html, 'lxml')
    table = bs_obj_jobs.find_all('table')
    description = 'None'
    try:
        if len(table[2].table.find_all('tr')[1].td.td.text) == 10:
            # if date only: '03.08.2020'
            date_start = table[2].table.find_all('tr')[1].td.td.text
        else:
            # if this format: '03.08.2020, Ref.#: DE' get only date
            date_start = table[2].table.find_all('tr')[1].td.td.text.split(',')[0]
    except Exception as ex:
        print(f'Ellement not found {ex}')

    try:  # header info # address, working hours etc.
        header_info = table[2].table.find_all('tr')[1].find('td',
                                                            style="font-style:italic;").get_text()  # also contain GPS
        print(header_info)
    except Exception as ex:
        print(f'Ellement not found {ex}')

    try:
        title = table[2].table.find_all('tr')[3].b.text
        company_url = table[2].table.find_all('tr')[3].a['href']
        print(f'Title: {title} and company url, {company_url}')
    except Exception as ex:
        print(f'Ellement not found {ex}')

    try:
        full_text_len = len(table[2].table.find_all('tr')[7].get_text())
        if full_text_len < 700:
            description = table[2].table.find_all('tr')[6].get_text()
            all_text = 'true'
            if len(description) < 700:
                description = table[2].table
                all_text = 'true'
        elif full_text_len > 700:
            description = table[2].table.find_all('tr')[7].get_text()
            all_text = 'true'
        else:
            description = 'None'
            all_text = 'false'
    except Exception as ex:
        print(ex)

    try:
        keywords = keywords_f(description)
    except Exception as ex:
        keywords: str = 'None'
        print(Exception)
        # 'date_start', 'str_keywords', 'len_keywords', 'title', 'link'
    print(date_start, title, keywords, len(keywords), link, all_text)  # no list of keywords
    data = date_start, title, keywords, len(keywords), link, all_text
    # data = date_start, link, company_name, title, description, company_url, all_text
    # Writing data in file
    csv_writer.writerow(data)
    # df.to_csv('my_csv.csv', mode='a', header=False)
    return None


def links_csv(path_csv_file: str) -> List:
    """ Opens csv file if exist and get all urls who already been scraped"""
    with open(path_csv_file, encoding="utf-8") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        next(csv_reader)
        links = []
        for row in csv_reader:
            print(row[1])
            links.append(row[1])
    return links


try:
    existing: list = links_csv(file_path)
    print('links exist')
except Exception as e:
    existing: list = []
    print(f'file not exist yet {e}')


def get_number_of_pages(link: str) -> int:
    """ visiting with base link and after that with dynamic link link """
    html = urlopen(link)
    bs_obj_jobs = BeautifulSoup(html, 'lxml')
    pages_number = bs_obj_jobs.find('td', style="height:25px;width:220px;font-weight:bold;padding-bottom:5px;").text
    all_posts = pages_number.split('от')[1][1:]
    print(all_posts)
    number_of_pages = round(int(all_posts) / 15) - 1
    last_page_num = number_of_pages * 15
    return last_page_num


""" getting number of pages as search results """
last_page_num = get_number_of_pages(link)
list_lisnks_html: list = []
html = urlopen(base_link)


for number in range(0, last_page_num + 15, 15):
    link: str = f"https://www.jobs.bg/front_job_search.php?frompage={number}&keywords%5B0%5D={keyword}#paging"  # original
    print(link)
    html = urlopen(link)
    bs_obj_jobs = BeautifulSoup(html, 'lxml')
    list_lisnks_html = list_lisnks_html + bs_obj_jobs.findAll('a', class_='joblink')
    sleep(5)


def get_unique():
    list_new_links = [f'{base_link}{link_raw["href"][4:]}' for link_raw in list_lisnks_html]
    unique_new_links = [i for i in list_new_links if i not in existing]
    return unique_new_links


""" get unique from two lists """
""" list comprehension """
# //TODO This generators and test them
# list_links = [f'{base_link}{link_raw["href"][4:]}' for link_raw in bs_obj_jobs.findAll('a', class_='joblink')]
""" generator comprehension """
# list_links_yield = (f'{base_link}{link_raw["href"][4:]}' for link_raw in bs_obj_jobs.findAll('a', class_='joblink'))

unique = get_unique()
# new with generators:

for num, link in enumerate(unique):
    print(link)
    extract_info(link, csv_writer=csv_writer)
    sleep(random.randint(1, 7)*2)
    if num % 30 == 0:
        sleep(random.randint(20, 30) * 4)

try:
    csv_file.close()
    csv_file.close()
    csv_file.close()
    print(f'{csv_file} is closed')
except Exception as ex:
    print(ex)