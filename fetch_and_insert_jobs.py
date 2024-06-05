import requests
import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv
import re
from datetime import datetime

load_dotenv()

api_key = os.getenv("API_KEY")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

def fetch_jsearch_jobs():
    url = "https://jsearch.p.rapidapi.com/search"
    querystring = {"query": "Data Professional in Atlanta, Georgia, USA", "page": "1", "num_pages": "1"}
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()
    jobs = response.json().get('data', [])
    return jobs

def validate_and_clean_job(job):
    try:
        # Validate and clean job_id
        job_id = job.get("job_id")
        if not isinstance(job_id, str):
            raise ValueError("Invalid job_id")
        
        # Validate and clean title
        title = job.get("job_title")
        if not isinstance(title, str):
            raise ValueError("Invalid job_title")
        
        # Validate and clean description
        description = job.get("job_description")
        if not isinstance(description, str):
            description = "No description available"
        
        # Validate and clean employment_type
        employment_type = job.get("job_employment_type")
        if not isinstance(employment_type, str):
            employment_type = "Unknown"
        
        # Validate and clean city
        city = job.get("job_city")
        if not isinstance(city, str):
            city = "Unknown"
        
        # Validate and clean state
        state = job.get("job_state")
        if not isinstance(state, str):
            state = "Unknown"
        
        # Validate and clean is_remote
        is_remote = job.get("job_is_remote", False)
        if not isinstance(is_remote, bool):
            is_remote = False
        
        # Validate and clean apply_link
        apply_link = job.get("job_apply_link")
        if not isinstance(apply_link, str) or not re.match(r'^https?://', apply_link):
            apply_link = "N/A"
        
        # Validate and clean company_type
        company_type = job.get("employer_company_type")
        if not isinstance(company_type, str):
            company_type = "Unknown"
        
        # Validate and clean employer_name
        employer_name = job.get("employer_name")
        if not isinstance(employer_name, str):
            employer_name = "Unknown"
        
        # Validate and clean publisher
        publisher = job.get("job_publisher")
        if not isinstance(publisher, str):
            publisher = "Unknown"
        
        # Validate and clean posted_at
        posted_at = job.get("job_posted_at_datetime_utc")
        try:
            posted_at = datetime.strptime(posted_at, "%Y-%m-%dT%H:%M:%S.%fZ")
        except:
            posted_at = None
        
        # Validate and clean offer_expiration
        offer_expiration = job.get("job_offer_expiration_datetime_utc")
        try:
            offer_expiration = datetime.strptime(offer_expiration, "%Y-%m-%dT%H:%M:%S.%fZ")
        except:
            offer_expiration = None
        
        # Validate and clean required_experience_in_months
        required_experience_in_months = job.get("job_required_experience", {}).get("required_experience_in_months")
        if not isinstance(required_experience_in_months, int):
            required_experience_in_months = None
        
        # Validate and clean latitude
        latitude = job.get("job_latitude")
        try:
            latitude = float(latitude)
            if latitude < -90 or latitude > 90:
                latitude = None
        except:
            latitude = None
        
        # Validate and clean longitude
        longitude = job.get("job_longitude")
        try:
            longitude = float(longitude)
            if longitude < -180 or longitude > 180:
                longitude = None
        except:
            longitude = None
        
        return {
            "job_id": job_id,
            "title": title,
            "description": description,
            "employment_type": employment_type,
            "city": city,
            "state": state,
            "is_remote": is_remote,
            "apply_link": apply_link,
            "company_type": company_type,
            "employer_name": employer_name,
            "publisher": publisher,
            "posted_at": posted_at,
            "offer_expiration": offer_expiration,
            "required_experience_in_months": required_experience_in_months,
            "latitude": latitude,
            "longitude": longitude
        }
    except Exception as e:
        print(f"Error validating job: {e}")
        return None

def insert_jobs_into_db(jobs):
    conn = psycopg2.connect(
        dbname="job_search",
        user=db_user,
        password=db_password,
        host="localhost"
    )
    cur = conn.cursor()

    for job in jobs:
        cleaned_job = validate_and_clean_job(job)
        if not cleaned_job:
            print("Skipping invalid job.")
            continue
        
        job_id = cleaned_job["job_id"]
        
        # Check if job_id already exists in the database
        cur.execute("SELECT 1 FROM job_listings WHERE job_id = %s", (job_id,))
        if cur.fetchone():
            print(f"Job with ID {job_id} already exists. Skipping insertion.")
            continue

        cur.execute(
            sql.SQL("INSERT INTO job_listings (job_id, title, description, employment_type, city, state, is_remote, apply_link, company_type, employer_name, publisher, posted_at, offer_expiration, required_experience_in_months, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"),
            [
                cleaned_job['job_id'], cleaned_job['title'], cleaned_job['description'], cleaned_job['employment_type'],
                cleaned_job['city'], cleaned_job['state'], cleaned_job['is_remote'], cleaned_job['apply_link'],
                cleaned_job['company_type'], cleaned_job['employer_name'], cleaned_job['publisher'], cleaned_job['posted_at'],
                cleaned_job['offer_expiration'], cleaned_job['required_experience_in_months'], cleaned_job['latitude'], cleaned_job['longitude']
            ]
        )
    
    conn.commit()
    cur.close()
    conn.close()

# Fetch jobs from JSearch API
jobs = fetch_jsearch_jobs()

# Insert jobs into PostgreSQL database
insert_jobs_into_db(jobs)

print("Data insertion completed.")