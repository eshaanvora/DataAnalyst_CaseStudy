#Eshaan Vora
#Data Analyst Case Study
#11/25/22

# Import packages
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

###########################################
#Set credentials for Database connection
hostName = "localhost"
dbName = "DataAnalyst_CaseStudy"
userName = "root"
password = "Password"

#Create SQL Connection to upload data into MySQL Server
sqlEngine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
    .format(host=hostName, db=dbName, user=userName, pw=password))
if not database_exists(sqlEngine.url):
    create_database(sqlEngine.url)
dbConnection = sqlEngine.connect()
###########################################

fname = "DA_CaseStudy.csv"
survey_df = pd.read_csv(fname)

#Clean RESPONSE Column for Trip Type 
for i in range(0,len(survey_df['RESPONSE'].values)) :
    indexCounter = 0
    phrase = survey_df['RESPONSE'][i]
    if "<b>" in phrase:
        survey_df['RESPONSE'][i] = phrase[phrase.find(">")+1:phrase.rfind("<")]

#Create MySQL table from the loaded dataframe, if it does not already exist
try:
    survey_df.to_sql("Survey", dbConnection, if_exists='fail')
except Exception as E:
    print(E)
else:
    print("Table created successfully")

