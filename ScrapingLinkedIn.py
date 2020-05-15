from bs4 import BeautifulSoup
import requests
import pandas as pd
import sqlalchemy
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark import sql
from pyspark.sql import SQLContext
from pyspark.sql import types

database_password = 'Password'
database_ip       = 'mydb.cqdk5nbfyybo.us-east-2.rds.amazonaws.com'
database_port = '3306'
database_connection = sqlalchemy.create_engine('mysql+pymysql://admin:{0}@{1}/mydb?host={1}?port={2}'.
                                               format(database_password, database_ip, database_port))
sc =SparkContext()
sqlContext = sql.SQLContext(sc)
searchval = raw_input("Enter job to search:")
searchval = searchval.replace(" ","%20")

url = 'https://www.linkedin.com/jobs/search/?geoId=103644278&keywords={0}&location=United%20States&start=0'.format(searchval)
x = requests.get(url)
soup = BeautifulSoup(x.text, "html.parser")
linklist = []
datelist = []

count = 1
readdf = pd.read_sql_table('LinkedIn', database_connection)
URLList = readdf['URL'].to_list()

	
for a in soup.find_all('a', href=True):
	if "linkedin.com/company/" in a['href']:	
		linklist.append(a['href'])

for time in soup.find_all('time', datetime =True):
	datelist.append(time['datetime'])


titlelist = [element.text for element in soup.find_all("span", "screen-reader-text")]
companylist = [element.text for element in soup.find_all("a","result-card__subtitle-link job-result-card__subtitle-link")]
locationlist = [element.text for element in soup.find_all("span","job-result-card__location")]

for x in range(len(titlelist)):
	df = pd.DataFrame({'Date': datelist[x],'Job_Title': titlelist[x].replace("\n",""), 'URL': linklist[x],'Organization': companylist[x].replace("\n","")},index=[count])
	if linklist[x] not in URLList:
		sdf = sqlContext.createDataFrame(df)
		sdf.show()
		df.to_sql(con=database_connection, name='LinkedIn', if_exists='append')
		count +=1
		URLList.append(linklist[x])

print("Search Complete.")
