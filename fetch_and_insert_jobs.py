import requests
import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("API_KEY")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")


def fetch_jsearch_jobs():
    url = "https://jsearch.p.rapidapi.com/search"
    querystring = {"query": "Data Professional in Atlanta, Georgia, USA", "page": "1", "num_pages": "1"}
    headers = {
        "X-RapidAPI-Key": api_key,  # Replace with your actual API key
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()  # Raises an error if the request was unsuccessful
    jobs = response.json().get('data', [])
    return jobs

def insert_jobs_into_db(jobs):
    conn = psycopg2.connect(
        dbname="job_search",
        user=db_user,
        password=db_password,  # Replace with your actual password
        host="localhost"
    )
    cur = conn.cursor()

    for job in jobs:
        job_id = job.get("job_id", "N/A")
        title = job.get("job_title", "N/A")
        description = job.get("job_description", "N/A")
        employment_type = job.get("job_employment_type", "N/A")
        city = job.get("job_city", "N/A")
        state = job.get("job_state", "N/A")
        is_remote = job.get("job_is_remote", False)
        apply_link = job.get("job_apply_link", "N/A")
        company_type = job.get("employer_company_type", "N/A")
        employer_name = job.get("employer_name", "N/A")
        publisher = job.get("job_publisher", "N/A")
        posted_at = job.get("job_posted_at_datetime_utc", None)
        offer_expiration = job.get("job_offer_expiration_datetime_utc", None)
        required_experience_in_months = job.get("job_required_experience", {}).get("required_experience_in_months", None)
        latitude = job.get("job_latitude", None)
        longitude = job.get("job_longitude", None)

        cur.execute(
            sql.SQL("INSERT INTO job_listings (job_id, title, description, employment_type, city, state, is_remote, apply_link, company_type, employer_name, publisher, posted_at, offer_expiration, required_experience_in_months, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"),
            [job_id, title, description, employment_type, city, state, is_remote, apply_link, company_type, employer_name, publisher, posted_at, offer_expiration, required_experience_in_months, latitude, longitude]
        )
    
    conn.commit()
    cur.close()
    conn.close()

# Fetch jobs from JSearch API
jobs = fetch_jsearch_jobs()

# Insert jobs into PostgreSQL database
insert_jobs_into_db(jobs)

print("Data insertion completed.")