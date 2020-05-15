#!/usr/bin/env python
# coding: utf-8

# Importing libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import json
import sqlalchemy
from pyspark import SparkContext
from pyspark.sql import SQLContext

database_password = 'My Password'
database_ip       = 'My database endpoint'
database_port = 'My port number'

database_connection = sqlalchemy.create_engine('mysql+pymysql://admin:{0}@{1}/mydb?host={1}?port={2}'.
                                               format(database_password, database_ip, database_port))

#Accessing the webpage
myinput = input("Enter job to search:")
searchval = myinput.replace(" ", "-")
URL = 'https://www.monster.com/jobs/search/?q={0}&where=united-states'.format(searchval)
page = requests.get(URL)

#Access the entire html content
soup = BeautifulSoup(page.content, 'html.parser')
results = soup.find(id='SearchResults')

#Accessing all of the job listed
job_elems= results.find_all('section', 'card-content')

#Accessin the job URL
jobUrl = []
for a in soup.find_all('a', href = True):
	if "https://job-openings" in a['href']:
		jobUrl.append(a['href'])
print(jobUrl)


#Accessing the Job Elements
for job_elem in job_elems:
	# Each job_elem is a new BeautifulSoup object.
	title = job_elem.find('h2', class_='title')
	company = job_elem.find('div', class_='company')
	location = job_elem.find('div', class_='location')
	date_posted =job_elem.find('time', datetime="2017-05-26T12:00")
	if None in (title, company, location, date_posted):
		continue
	print(title.text)
	print(company.text)
	print(location.text)
	print(date_posted.text)
	print()


titlelist = [element.text for element in soup.find_all("h2", "title")]
datelist = [element.text for element in soup.find_all("time", datetime= "2017-05-26T12:00")]
locationlist = [element.text for element in soup.find_all("div","location")]
companylist = [element.text for element in soup.find_all("div","company")]
readdf = pd.read_sql_table('Monster', database_connection)
URLList = readdf['URL'].to_list()

#Creating the DataFrame and saving to Database
for x in range(len(titlelist)):
	pddf = pd.DataFrame({'Date': datelist[x], 'Job_Title': titlelist[x].replace("\n","").replace("\r",""), 'Company': companylist[x].replace("\n",""),'Location': locationlist[x].replace("\n","").replace("\r","") , 'URL': jobUrl[x].replace("\n","")}, index=[x+1])
	if jobUrl[x] not in URLList:
		df= sqlc.createDataFrame(pddf)
		df.show()
		pddf.to_sql(con=database_connection, name='Monster', if_exists='append')
		URLList.append(jobUrl[x])






