
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException
from selenium import webdriver
import time
import pandas as pd
import pymysql
from sqlalchemy import create_engine


def parse_detail(keyword, job_num,verbose):
	hostname = 'mydb.cqdk5nbfyybo.us-east-2.rds.amazonaws.com'
	port = '3306'
	user = 'admin'
	password = 'Password'
	db = 'mydb'	
	db_data = 'mysql+pymysql://' + user + ':' + password + '@' + hostname +':' + port + '/' + db + '?charset=utf8mb4'
	connection = pymysql.connect(host = 'mydb.cqdk5nbfyybo.us-east-2.rds.amazonaws.com',
				user = user,
				password = password,
				db = db)
	#connect to db				
	cursor = connection.cursor()
	print("connected to sql")
	check = "SHOW TABLES LIKE 'glassdoor'"
	cursor.execute(check)
	existed = cursor.fetchone()
	if existed:
		sql = "SELECT Listing_Id FROM mydb.glassdoor"
		cursor.execute(sql)
		existing_listing_list = cursor.fetchall()
		existing_listing=[]
		for i in existing_listing_list:
			existing_listing.append(i[0])
		print(existing_listing)
	else:
		existing_listing = []
		print(existing_listing)
	#connect to web
	options = webdriver.ChromeOptions()
	options.add_argument("headless")
	driver = webdriver.Chrome("/home/fieldemployee/opt/chromedriver")
	driver.maximize_window()
	url = 'https://www.glassdoor.com/profile/login_input.htm?userOriginHook=HEADER_SIGNIN_LINK'
	driver.get(url)	
	driver.find_element_by_id("userEmail").send_keys("yuchen.zhou@enhance.it")
	time.sleep(.1)		
	driver.find_element_by_id("userPassword").send_keys("Glassdoor1234")
	time.sleep(.1)
	driver.find_element_by_xpath("//button[@type='submit']").click()
	time.sleep(3)
	driver.find_element_by_id("sc.keyword").click()
	time.sleep(.1)
	driver.find_element_by_id("sc.keyword").send_keys(keyword)
	time.sleep(.1)
	driver.find_element_by_id("sc.location").clear()
	time.sleep(.1)
	driver.find_element_by_id("sc.location").send_keys("United States")
	time.sleep(.1)
	driver.find_element_by_xpath("//button[@type='submit']").click()
	time.sleep(2)

	jobs = []
	while len(jobs) < job_num:
		time.sleep(3)
		jl = driver.find_elements_by_class_name("jl")
		for j in jl:
			print("Progress: {}".format("" + str(len(jobs)) + "/" + str(job_num)))
			if len(jobs) >= job_num:
				break
			j.click()
			
			time.sleep(1)
			collected_successfully = False

			while not collected_successfully:
				try:	
					print("start scraping")
					link = j.find_element_by_xpath('.//div[@class="jobHeader"]/a').get_attribute("href")
					company_name = driver.find_element_by_xpath('.//div[@class="employerName"]').text
					location = driver.find_element_by_xpath('.//div[@class="location"]').text
					job_title = driver.find_element_by_xpath('.//div[contains(@class, "title")]').text
					job_description = driver.find_element_by_xpath('.//div[@class="jobDescriptionContent desc"]').text
					date = j.find_element_by_xpath('.//div[@class="d-flex align-items-end pl-std minor css-65p68w"]').text
					print("done scrape")
					
					collected_successfully = True
					print("collected")
				except:
					time.sleep(5)
			print("collecting salary...")
			try:
				salary_estimate = j.find_element_by_xpath('.//span[@class="gray salary"]').text
				print("salary is : " + salary_estimate)
			except NoSuchElementException:
				salary_estimate = -1 #You need to set a "not found value. It's important."

            		#Printing for debugging
			if verbose:
				print("Job Title: {}".format(job_title))
				print("Salary Estimate: {}".format(salary_estimate))
				print("Job Description: {}".format(job_description[:500]))
				print("Company Name: {}".format(company_name))
				print("Link: {}".format(link))	
				print("Date: {}".format(date))		
				print("Location: {}".format(location))

            		#Going to the Company tab...
            		#clicking on this:
            		#<div class="tab" data-tab-type="overview"><span>Company</span></div>
			try:
				driver.find_element_by_xpath('.//div[@class="tab" and @data-tab-type="overview"]').click()
				
				try:
                    			size = driver.find_element_by_xpath('.//div[@class="infoEntity"]//label[text()="Size"]//following-sibling::*').text
				except NoSuchElementException:
					size = -1
			except NoSuchElementException:
				size = -1
	                
			if verbose:
        		        print("Size: {}".format(size))
        		        print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
			listing_id = link[-10:]
			#add job to jobs
			if listing_id not in existing_listing:
				jobs.append({"Listing_Id" : listing_id,
				"Date" : date,
				"Job_Title" : job_title,
            			"Salary_Estimate" : salary_estimate,
            			"Job_Description" : job_description,
				"Date": date,
            			"Company_Name" : company_name[0:len(company_name)-4],
            			"Location" : location,
				"Link" : link})
            		#print("Appended to list")
		#Clicking on the "next page" button
		try:
            		driver.find_element_by_xpath('.//li[@class="next"]//a').click()
		except NoSuchElementException:
            		print("Scraping terminated before reaching target number of jobs. Needed 100, got {}.".format(len(jobs)))
            		break
				
	#This line converts the dictionary object into a pandas DataFrame.
	data = pd.DataFrame(jobs)
	pd.set_option('display.max_columns', 999)
	pd.set_option('display.width', 10)
	print(data)
	#save the table to database
	engine = create_engine(db_data)
	data.to_sql('glassdoor', engine, if_exists='append', index=False) 		
	sql1 = "SELECT * FROM mydb.glassdoor"
	cursor.execute(sql1)		
	engine.dispose()
	connection.close()
	print("connection closed")
	df1 = pd.DataFrame(jobs)
	return df1
if __name__=="__main__":
	keyword = str(input("Keyword:"))
	job_num = int(input("Number of records:"))
	df= parse_detail(keyword, job_num, False)
"""			
class JobScrape(scrapy.Spider):
	name = "jobScrape"
	start_urls = ['https://www.monster.com/jobs/search/?q=data-engineer&where=USA']
	
	def parse(self, response):
		self.logger.info("hi")
		result = json.loads(response.body)		
		for result in results:


	def parse_detail(self, response):
		result = json.loads(response.body)
https://www.glassdoor.com/Job/jobs.htm?suggestCount=0&suggestChosen=false&clickSource=searchBtn&typedKeyword=data+engineer&locT=&locId=0&jobType=&context=Jobs&sc.keyword=data+engineer
"""
