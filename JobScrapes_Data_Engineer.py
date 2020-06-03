from bs4 import BeautifulSoup
import requests
import pandas as pd
import sqlalchemy
import re
from datetime import datetime, timedelta
import dateutil.relativedelta
import calendar

database_password = 'Password'
database_ip       = 'mydb.cqdk5nbfyybo.us-east-2.rds.amazonaws.com'
database_port = '3306'
database_connection = sqlalchemy.create_engine('mysql+pymysql://admin:{0}@{1}/mydb?host={1}?port={2}'.
                                               format(database_password, database_ip, database_port))
strdate = datetime.today().strftime("%Y-%m-%d")

paramlist = ["Data%20Engineer","AWS%20Engineer","Big%20Data%20Engineer","Hadoop%20Engineer","Spark%20Engineer"]
try:
	df = pd.read_sql_table('JobPostings', database_connection)
	if 'level_0' in df:
		df = df.drop(columns=['level_0'])

	for searchval in paramlist:
		url1 = 'https://www.indeed.com/jobs?q={0}&start=0&fromage=30'.format(searchval)
		x1 = requests.get(url1)
		soupindeed = BeautifulSoup(x1.text, "html.parser")
		url2 = 'https://www.linkedin.com/jobs/search/?geoId=103644278&keywords={0}&location=United%20States&start=0&f_TP=1%2C2%2C3%2C4'.format(searchval)
		x2 = requests.get(url2)
		souplinkedin = BeautifulSoup(x2.text, "html.parser")
		url3 = 'https://www.simplyhired.com/search?q={0}&fdb=30'.format(searchval)
		x3 = requests.get(url3)
		soupsimply = BeautifulSoup(x3.text, "html.parser")
		url4 = 'https://www.monster.com/jobs/search/?q={0}&where=united-states&tm=30'.format(searchval)
		x4 = requests.get(url4)
		soupmonster = BeautifulSoup(x4.text, "html.parser")
		indeedlinklist = []
		for a in soupindeed.find_all('a', href=True):
			if "/pagead/" in a['href'] or "/rc/" in a['href'] or "/company/" in a['href']:	
				indeedlinklist.append("https://www.indeed.com"+ a['href'])
		
		indeedtitlelist = [element.text.replace("\n","").replace("\r","") for element in soupindeed.find_all("a", "jobtitle turnstileLink")]
		indeedcompanylist = [element.text.replace("\n","").replace("\r","") for element in soupindeed.find_all("span","company")]
		indeeddatelist = [element.text for element in soupindeed.find_all("span","date")]
		indeedlocationlist = [element['data-rc-loc'] for element in soupindeed.find_all("div","recJobLoc")]
		indeedcitylist = []
		indeedstatelist = []
		
		for val in indeedlocationlist:
			if "," in val:
				entry = val.split(",")
				indeedcitylist.append(entry[0])
				indeedstatelist.append(entry[1])
			else:
				indeedcitylist.append(" ")
				indeedstatelist.append(" ")
				
		indeedrealdatelist = []
		for val in indeeddatelist:
			val = re.sub("[^0-9]", "", val)
			if (val == ""):
				indeedrealdatelist.append(datetime.now().strftime("%Y-%m-%d"))
			else:
				indeedrealdatelist.append((datetime.now() - timedelta(days = int(val))).strftime("%Y-%m-%d"))
		if len(indeedlinklist) == len(indeedtitlelist) == len(indeedlocationlist) == len(indeedcompanylist) == len(indeedrealdatelist):		
			dfindeed = pd.DataFrame({'Date': indeedrealdatelist,'Job_Title': indeedtitlelist, 'URL' : indeedlinklist,'Company': indeedcompanylist, 'Search_Parameter':searchval.replace("%20"," "), 'City' : indeedcitylist, 'State': indeedstatelist})
			df = pd.concat([df,dfindeed],ignore_index=True).drop_duplicates(subset=['Job_Title','Company','City','State'], keep="last").reset_index(drop=True)
			print("Indeed " +searchval.replace("%20"," ") +" Search Complete.")
		else:
			print("Indeed Page was misread, moving to next page.")
	
		linkedinlinklist = []
		linkedindatelist = []
	
		for a in souplinkedin.find_all('a', href=True):
			if "www.linkedin.com/jobs/view" in a['href']:	
				linkedinlinklist.append(a['href'])

		for time in souplinkedin.find_all('time', datetime =True):
			linkedindatelist.append(time['datetime'])
	
		linkedintitlelist = [element.text for element in souplinkedin.find_all("span", "screen-reader-text")]
		linkedincompanylist = [element.text for element in souplinkedin.find_all("a","result-card__subtitle-link job-result-card__subtitle-link")]
		linkedinlocationlist = [element.text for element in souplinkedin.find_all("span","job-result-card__location")]
		linkedincitylist = []
		linkedinstatelist = []	
	
		for val in linkedinlocationlist:
			if "," in val:
				entry = val.split(",")
				linkedincitylist.append(entry[0])
				linkedinstatelist.append(entry[1])
			else:
				linkedincitylist.append(" ")
				linkedinstatelist.append(" ")

		if len(linkedinlinklist) == len(linkedintitlelist) == len(linkedinlocationlist) == len(linkedincompanylist) == len(linkedindatelist):
			dflinkedin = pd.DataFrame({'Date': linkedindatelist,'Job_Title': linkedintitlelist, 'URL' : linkedinlinklist,'Company': linkedincompanylist, 'Search_Parameter':searchval.replace("%20"," "), 'City' : linkedincitylist, 'State': linkedinstatelist})
			print("LinkedIn " +searchval.replace("%20"," ") +" Search Complete.")
			df = pd.concat([df,dflinkedin],ignore_index=True).drop_duplicates(subset=['Job_Title','Company','City','State'], keep="last").reset_index(drop=True)

		else:
			print("LinkedIn page was misread, moving to next page")


		monsterlinklist = []
		for a in soupmonster.find_all('a', href = True):
			if "https://job-openings" in a['href']:
				monsterlinklist.append(a['href'])
		
		monstertitlelist = [element.text.replace("\n","").replace("\r","")  for element in soupmonster.find_all("h2", "title")]
		monsterdatelist = [element.text for element in soupmonster.find_all("time", datetime= "2017-05-26T12:00")]
		monsterlocationlist = [element.text.replace("\n","").replace("\r","")  for element in soupmonster.find_all("div","location")]
		del monsterlocationlist[0]
		monstercitylist = []
		monsterstatelist = []
	
		for val in monsterlocationlist:
			if "," in val:
				entry = val.split(",")
				monstercitylist.append(entry[0])
				monsterstatelist.append(entry[1])
			else:
				monstercitylist.append(" ")
				monsterstatelist.append(" ")

		monstercompanylist = [element.text.replace("\n","").replace("\r","")  for element in soupmonster.find_all("div","company")]
		monsterrealdatelist = []
		
		for val in monsterdatelist:
			val = re.sub("[^0-9]", "", val)
			if (val == ""):
				monsterrealdatelist.append(datetime.now().strftime("%Y-%m-%d"))
			else:
				monsterrealdatelist.append((datetime.now() - timedelta(days = int(val))).strftime("%Y-%m-%d"))
		if len(monsterlinklist) == len(monstertitlelist) == len(monsterlocationlist) == len(monstercompanylist) == len(monsterrealdatelist):
			dfmonster = pd.DataFrame({'Date': monsterrealdatelist,'Job_Title': monstertitlelist, 'URL' : monsterlinklist,'Company': monstercompanylist, 'Search_Parameter':searchval.replace("%20"," "), 'City' : monstercitylist, 'State': monsterstatelist})
			print("Monster " +searchval.replace("%20"," ") +" Search Complete.")
			df = pd.concat([df,dfmonster],ignore_index=True).drop_duplicates(subset=['Job_Title','Company','City','State'], keep="last").reset_index(drop=True)
		else:
			print("Monster page was misread, moving to next page.")

		simplylinklist = []	
		simplytitlelist = [element.text.replace(u'\xa0', '').replace('\xa0', '') for element in soupsimply.find_all("h2", "jobposting-title")]
		simplydatelist = []
		simplylocationlist = [element.text.replace(u'\xa0', '').replace('\xa0', '') for element in soupsimply.find_all("span","JobPosting-labelWithIcon jobposting-location")]
		simplycitylist = []
		simplystatelist = []
		for val in simplylocationlist:
			if "," in val:
				entry = val.split(",")
				simplycitylist.append(entry[0])
				simplystatelist.append(entry[1])
			else:
				simplycitylist.append(" ")
				simplystatelist.append(" ")
		
		simplycompanylist = [element.text.replace(u'\xa0', '').replace('\xa0', '') for element in soupsimply.find_all("span","JobPosting-labelWithIcon jobposting-company")]	
		
		for a in soupsimply.find_all('a', href = True):
			if "/job/" in a['href']:
				simplylinklist.append("https://www.simplyhired.com"+a['href'])
	
		for time in soupsimply.find_all('time', datetime =True):
			simplydatelist.append(time['datetime'])
		if len(simplylinklist) == len(simplytitlelist) == len(simplylocationlist) == len(simplycompanylist) == len(simplydatelist):
			dfsimply = pd.DataFrame({'Date': simplydatelist,'Job_Title': simplytitlelist, 'URL' : simplylinklist,'Company': simplycompanylist, 'Search_Parameter':searchval.replace("%20"," "), 'City' : simplycitylist, 'State': simplystatelist})
			print("SimplyHired " +searchval.replace("%20"," ") +" Search Complete.")
			df = pd.concat([df,dfsimply],ignore_index=True).drop_duplicates(subset=['Job_Title','Company','City','State'], keep="last").reset_index(drop=True)
		else:
			print("SimplyHired page was misread, moving to next page")
		df.drop_duplicates(subset=['Job_Title','Company','City','State'], keep="last")
		df.to_sql(con=database_connection, name='JobPostings', if_exists='replace')

except ValueError:
	print("Table Does Not Already Exist... Creating Now")
	urllists = []
	for searchval in paramlist:
		df = pd.DataFrame(columns=['Date','Job_Title', 'URL', 'Company', 'Search_Parameter', 'City', 'State'])
		url1 = 'https://www.indeed.com/jobs?q={0}&start=0&fromage=30'.format(searchval)
		x1 = requests.get(url1)
		soupindeed = BeautifulSoup(x1.text, "html.parser")
		url2 = 'https://www.linkedin.com/jobs/search/?geoId=103644278&keywords={0}&location=United%20States&start=0&f_TP=1%2C2%2C3%2C4'.format(searchval)
		x2 = requests.get(url2)
		souplinkedin = BeautifulSoup(x2.text, "html.parser")
		url3 = 'https://www.simplyhired.com/search?q={0}&fdb=30'.format(searchval)
		x3 = requests.get(url3)
		soupsimply = BeautifulSoup(x3.text, "html.parser")
		url4 = 'https://www.monster.com/jobs/search/?q={0}&where=united-states&tm=30'.format(searchval)
		x4 = requests.get(url4)
		soupmonster = BeautifulSoup(x4.text, "html.parser")
		indeedlinklist = []
		for a in soupindeed.find_all('a', href=True):
			if "/pagead/" in a['href'] or "/rc/" in a['href'] or "/company/" in a['href']:	
				indeedlinklist.append("https://www.indeed.com"+ a['href'])
		
		indeedtitlelist = [element.text.replace("\n","").replace("\r","") for element in soupindeed.find_all("a", "jobtitle turnstileLink")]
		indeedcompanylist = [element.text.replace("\n","").replace("\r","") for element in soupindeed.find_all("span","company")]
		indeeddatelist = [element.text for element in soupindeed.find_all("span","date")]
		indeedlocationlist = [element['data-rc-loc'] for element in soupindeed.find_all("div","recJobLoc")]
		indeedcitylist = []
		indeedstatelist = []
		
		for val in indeedlocationlist:
			if "," in val:
				entry = val.split(",")
				indeedcitylist.append(entry[0])
				indeedstatelist.append(entry[1])
			else:
				indeedcitylist.append(" ")
				indeedstatelist.append(" ")
				
		indeedrealdatelist = []
		for val in indeeddatelist:
			val = re.sub("[^0-9]", "", val)
			if (val == ""):
				indeedrealdatelist.append(datetime.now().strftime("%Y-%m-%d"))
			else:
				indeedrealdatelist.append((datetime.now() - timedelta(days = int(val))).strftime("%Y-%m-%d"))
		if len(indeedlinklist) == len(indeedtitlelist) == len(indeedlocationlist) == len(indeedcompanylist) == len(indeedrealdatelist):		
			dfindeed = pd.DataFrame({'Date': indeedrealdatelist,'Job_Title': indeedtitlelist, 'URL' : indeedlinklist,'Company': indeedcompanylist, 'Search_Parameter':searchval.replace("%20"," "), 'City' : indeedcitylist, 'State': indeedstatelist})
			df = pd.concat([df,dfindeed],ignore_index=True).drop_duplicates(subset=['Job_Title','Company','City','State'], keep="last").reset_index(drop=True)
			print("Indeed " +searchval.replace("%20"," ") +" Search Complete.")
		else:
			print("Indeed Page was misread, moving to next page.")
	
		linkedinlinklist = []
		linkedindatelist = []
	
		for a in souplinkedin.find_all('a', href=True):
			if "www.linkedin.com/jobs/view" in a['href']:	
				linkedinlinklist.append(a['href'])

		for time in souplinkedin.find_all('time', datetime =True):
			linkedindatelist.append(time['datetime'])
	
		linkedintitlelist = [element.text for element in souplinkedin.find_all("span", "screen-reader-text")]
		linkedincompanylist = [element.text for element in souplinkedin.find_all("a","result-card__subtitle-link job-result-card__subtitle-link")]
		linkedinlocationlist = [element.text for element in souplinkedin.find_all("span","job-result-card__location")]
		linkedincitylist = []
		linkedinstatelist = []	
	
		for val in linkedinlocationlist:
			if "," in val:
				entry = val.split(",")
				linkedincitylist.append(entry[0])
				linkedinstatelist.append(entry[1])
			else:
				linkedincitylist.append(" ")
				linkedinstatelist.append(" ")

		if len(linkedinlinklist) == len(linkedintitlelist) == len(linkedinlocationlist) == len(linkedincompanylist) == len(linkedindatelist):
			dflinkedin = pd.DataFrame({'Date': linkedindatelist,'Job_Title': linkedintitlelist, 'URL' : linkedinlinklist,'Company': linkedincompanylist, 'Search_Parameter':searchval.replace("%20"," "), 'City' : linkedincitylist, 'State': linkedinstatelist})
			print("LinkedIn " +searchval.replace("%20"," ") +" Search Complete.")
			df = pd.concat([df,dflinkedin],ignore_index=True).drop_duplicates(subset=['Job_Title','Company','City','State'], keep="last").reset_index(drop=True)

		else:
			print("LinkedIn page was misread, moving to next page")


		monsterlinklist = []
		for a in soupmonster.find_all('a', href = True):
			if "https://job-openings" in a['href']:
				monsterlinklist.append(a['href'])
		
		monstertitlelist = [element.text.replace("\n","").replace("\r","")  for element in soupmonster.find_all("h2", "title")]
		monsterdatelist = [element.text for element in soupmonster.find_all("time", datetime= "2017-05-26T12:00")]
		monsterlocationlist = [element.text.replace("\n","").replace("\r","")  for element in soupmonster.find_all("div","location")]
		del monsterlocationlist[0]
		monstercitylist = []
		monsterstatelist = []
	
		for val in monsterlocationlist:
			if "," in val:
				entry = val.split(",")
				monstercitylist.append(entry[0])
				monsterstatelist.append(entry[1])
			else:
				monstercitylist.append(" ")
				monsterstatelist.append(" ")

		monstercompanylist = [element.text.replace("\n","").replace("\r","")  for element in soupmonster.find_all("div","company")]
		monsterrealdatelist = []
		
		for val in monsterdatelist:
			val = re.sub("[^0-9]", "", val)
			if (val == ""):
				monsterrealdatelist.append(datetime.now().strftime("%Y-%m-%d"))
			else:
				monsterrealdatelist.append((datetime.now() - timedelta(days = int(val))).strftime("%Y-%m-%d"))
		if len(monsterlinklist) == len(monstertitlelist) == len(monsterlocationlist) == len(monstercompanylist) == len(monsterrealdatelist):
			dfmonster = pd.DataFrame({'Date': monsterrealdatelist,'Job_Title': monstertitlelist, 'URL' : monsterlinklist,'Company': monstercompanylist, 'Search_Parameter':searchval.replace("%20"," "), 'City' : monstercitylist, 'State': monsterstatelist})
			print("Monster " +searchval.replace("%20"," ") +" Search Complete.")
			df = pd.concat([df,dfmonster],ignore_index=True).drop_duplicates(subset=['Job_Title','Company','City','State'], keep="last").reset_index(drop=True)
		else:
			print("Monster page was misread, moving to next page.")

		simplylinklist = []	
		simplytitlelist = [element.text.replace(u'\xa0', '').replace('\xa0', '') for element in soupsimply.find_all("h2", "jobposting-title")]
		simplydatelist = []
		simplylocationlist = [element.text.replace(u'\xa0', '').replace('\xa0', '') for element in soupsimply.find_all("span","JobPosting-labelWithIcon jobposting-location")]
		simplycitylist = []
		simplystatelist = []
		for val in simplylocationlist:
			if "," in val:
				entry = val.split(",")
				simplycitylist.append(entry[0])
				simplystatelist.append(entry[1])
			else:
				simplycitylist.append(" ")
				simplystatelist.append(" ")
		
		simplycompanylist = [element.text.replace(u'\xa0', '').replace('\xa0', '') for element in soupsimply.find_all("span","JobPosting-labelWithIcon jobposting-company")]	
		
		for a in soupsimply.find_all('a', href = True):
			if "/job/" in a['href']:
				simplylinklist.append("https://www.simplyhired.com"+a['href'])
	
		for time in soupsimply.find_all('time', datetime =True):
			simplydatelist.append(time['datetime'])
		if len(simplylinklist) == len(simplytitlelist) == len(simplylocationlist) == len(simplycompanylist) == len(simplydatelist):
			dfsimply = pd.DataFrame({'Date': simplydatelist,'Job_Title': simplytitlelist, 'URL' : simplylinklist,'Company': simplycompanylist, 'Search_Parameter':searchval.replace("%20"," "), 'City' : simplycitylist, 'State': simplystatelist})
			print("SimplyHired " +searchval.replace("%20"," ") +" Search Complete.")
			df = pd.concat([df,dfsimply],ignore_index=True).drop_duplicates(subset=['Job_Title','Company','City','State'], keep="last").reset_index(drop=True)
		else:
			print("SimplyHired page was misread, moving to next page")
		df.drop_duplicates(subset=['Job_Title','Company','City','State'], keep="last")
		df.to_sql(con=database_connection, name='JobPostings', if_exists='replace')





