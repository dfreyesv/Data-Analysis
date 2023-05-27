import pandas as pd


file_url = "https://covid.ourworldindata.org/data/owid-covid-data.csv"
file_path = "D:\\Documentos\\GitHub\\Data-Analysis\\Covid-19 Global Vaccine Tracker\\Covid19_Dataset.csv"

#Downlaod info into df
raw_data = pd.read_csv(file_url)

#Print column names
#print(list(raw_data.columns))

#Filter out unused columns
data = raw_data[["continent","location","date","people_vaccinated","people_fully_vaccinated","people_vaccinated_per_hundred","total_boosters","new_vaccinations","total_vaccinations","population","gdp_per_capita"]]

#Remove rows with nan values for column "continent" and keep rows for world data
data = data[(~data["continent"].isna()) |(data["location"]=="World")]

#Export csv file to folder path for Tableau to load it
data.to_csv(file_path, index=False)