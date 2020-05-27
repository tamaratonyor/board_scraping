#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Importing libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import json

from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql import DataFrame



#sc = SparkContext()
sc=SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)


# ## Defining helper functions

# In[2]:


def userinput(userinput1,userinput2,userinput3):
    pages_list=[]
    for i in range(1,int(userinput3)+1):
        pages_list.append(f'https://www.simplyhired.com/search?q={userinput1}&l={userinput2}&sb=dd&pn={i}')#page1='https://www.simplyhired.com/search?q=big+data+engineer&l=united+states&sb=dd&pn=2'#&job=MrNtePJJR-mAmQM8Tiba5rnHB16pySp-UvaY0XdiWugpNfotFPsEtQ'        
    return pages_list




def scraping(userinput1,pages_list):
        mydf=[]
        total_jobs=0
        for i in pages_list:
                page = requests.get(i)
                soup = BeautifulSoup(page.content, 'html.parser') # Access on the whole web front-end content
                #print(soup)
                #print(soup.find_all('a'))# find all links

                job_container=soup.find(id="job-list")#fAccess on the job-list container
                #print(job_list)


                jobs=job_container.find_all(class_ ='SerpJob')#Accessing all of the job listed
                total_jobs+=len(jobs)
                # for i in range(len(jobs)):
                #         print(jobs[i].find(class_='jobposting-title-container').get_text())#title,
                #         print(jobs[i].find(class_='JobPosting-labelWithIcon jobposting-company').get_text())#company
                #         print(jobs[i].find(class_='JobPosting-labelWithIcon jobposting-location').get_text())#location
                #         print(jobs[i].find(class_='jobposting-salary').get_text())#salary
                #         print(jobs[i].find(class_='jobposting-snippet').get_text())#description
                #         print(jobs[i].find(class_='SerpJob-timestamp').get_text())#date
                #         #print(jobs[i].find(SerpJob-timestamp.datetime))#.get_text())#
                #         #print(jobs[i].find_all('a'))# find all links      

                searchKeyword=[userinput1 for i in range(len(jobs))]
                titles=[jobs[i].find(class_='jobposting-title-container').get_text() for i in range(len(jobs))]
                companies=[jobs[i].find(class_='JobPosting-labelWithIcon jobposting-company').get_text() for i in range(len(jobs))]
                locations=[jobs[i].find(class_='JobPosting-labelWithIcon jobposting-location').get_text() for i in range(len(jobs))]
                salaries=[jobs[i].find(class_='SerpJob-metaInfoLeft').get_text() for i in range(len(jobs))]#jobposting-salary
                descriptions=[jobs[i].find(class_='jobposting-snippet').get_text() for i in range(len(jobs))]
                timestamps=[jobs[i].find(class_='SerpJob-timestamp').get_text() for i in range(len(jobs))]
                links=['https://www.simplyhired.com'+jobs[i].find('a').get('href') for i in range(len(jobs))]#
                
                #Spliting locations into Cities and States
                cities=[]
                states=[]
                
                for i in locations:
                    loc=i.split(',')
                    cities.append(loc[0])
                    states.append(loc[-1])

                    
                    
                jobs_df=pd.DataFrame(
                    {  
                      'Date':timestamps,
                      'searchKeyword':searchKeyword,
                      'Job-title':titles,
                      'Job-description':descriptions,
                      'City':cities,
                      'State':states,
                      'Rate':salaries,
                      'CompanY-name':companies,                    
                      'Links':links

                    })
                mydf.append(jobs_df)# appending available df 
        
        print('Sample link:')
        print(links[2])        
        result = pd.concat(mydf)
        return total_jobs,result
        


# ## Getting input from User

# In[3]:


userinput1=input('Please Enter the Title:')
userinput2=input('Please Enter the Location:')
userinput3=input('Please Enter the Number of Pages to search from:')

pages_list = userinput(userinput1,userinput2,userinput3)
total_jobs, result = scraping(userinput1,pages_list)
print('Total Jobs:',total_jobs)


# ## Displaying the Dataframe

# In[4]:



mySchema = StructType([ StructField("Date", StringType(), True)                       ,StructField("SearchKeyword", StringType(), True)                       ,StructField("Job-title", StringType(), True)                       ,StructField("Job-description", StringType(), True)                       ,StructField("City", StringType(), True)                       ,StructField("State", StringType(), True)                       ,StructField("Rate", StringType(), True)                       ,StructField("Company-name", StringType(), True)                       ,StructField("Link", StringType(), True)])
df = spark.createDataFrame(result,schema=mySchema)

df.show(5)


# ## Saving the Dataframe into a Database as a table

# In[5]:


#add the jar to the spark/jar to link spark to mysql (append or overwrite)
try:
    df1=spark.read.format('jdbc').options(url='jdbc:mysql://mydb.cqdk5nbfyybo.us-east-2.rds.amazonaws.com:3306/mydb',driver='com.mysql.jdbc.Driver',dbtable='SIMPLYHIRED',user='admin',password='Password').load()
    df2=df1.union(df).distinct()#union of both leaving out duplicates
    df2.show()
    #df2=df.join(df1,'Link','leftanti').show()
    #df1=df1.union(df2)
    print('saving......1')
    df2.write.format('jdbc').options(url='jdbc:mysql://mydb.cqdk5nbfyybo.us-east-2.rds.amazonaws.com:3306/mydb',driver='com.mysql.jdbc.Driver',dbtable='SIMPLYHIRED',user='admin',password='Password').mode('append').save()
    print('saving......done')
except:
    print('Runing except')
    df1= df
    df1.write.format('jdbc').options(url='jdbc:mysql://mydb.cqdk5nbfyybo.us-east-2.rds.amazonaws.com:3306/mydb',driver='com.mysql.jdbc.Driver',dbtable='SIMPLYHIRED',user='admin',password='Password').mode('append').save()
    


# In[6]:


#https://www.youtube.com/watch?v=E5cSNSeBhjw
#https://pandas.pydata.org/pandas-docs/stable/user_guide/merging.html
#https://hackersandslackers.com/scraping-urls-with-beautifulsoup/
#https://opensource.com/article/19/5/log-data-apache-spark


# In[7]:


#S3
#http://carolynlangen.com/2017/11/22/interacting-with-aws-s3-using-python-in-a-jupyter-notebook/

