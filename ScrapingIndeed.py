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

pagenums = int(raw_input("Enter number of webpages to scan (digits only):"))
dfcache = []
count = 1

readdf = pd.read_sql_table('Indeed', database_connection)
tabledescriptions = readdf['Job_Work_Task'].to_list()

for f in range(pagenums):
	url = 'https://www.indeed.com/jobs?q={0}&start={1}'.format(searchval,f*10)
	x = requests.get(url)
	soup = BeautifulSoup(x.text, "html.parser")
	linklist = []

	for a in soup.find_all('a', href=True):
		if "/pagead/" in a['href'] or "/rc/" in a['href'] or "/company/" in a['href']:	
			linklist.append("https://www.indeed.com/"+ a['href'])

	titlelist = [element.text for element in soup.find_all("a", "jobtitle turnstileLink")]
	companylist = [element.text for element in soup.find_all("span","company")]
	datelist = [element.text for element in soup.find_all("span","date")]
	descriptionlist = [element.text for element in soup.find_all("div","summary")]

	for x in range(len(titlelist)):
		df = pd.DataFrame({'Date': datelist[x],'Job_Title': titlelist[x].replace("\n",""),'Job_Work_Task' : descriptionlist[x].encode('utf-8'),'URL': linklist[x],'Organization': companylist[x].replace("\n","")},index=[count])
		if descriptionlist[x].encode('utf-8') not in tabledescriptions:
			sdf = sqlContext.createDataFrame(df)
			sdf.show()
			df.to_sql(con=database_connection, name='Indeed', if_exists='append')
			count+=1

 
print("Search Complete.")
