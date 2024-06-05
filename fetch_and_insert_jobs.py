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
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers, params=querystring)
    response.raise_for_status()
    jobs = response.json().get('data', [])
    return jobs

def validate_job(job):
    required_fields = ["job_id", "job_title", "job_description", "job_employment_type", "job_city", "job_state", "job_is_remote", "job_apply_link", "employer_name", "job_publisher"]
    
    for field in required_fields:
        if field not in job or job[field] is None:
            return False, f"Missing required field: {field}"
    
    # Validate data types and provide default values
    job_id = str(job.get("job_id", "N/A"))
    title = str(job.get("job_title", "N/A"))
    description = str(job.get("job_description", "N/A"))
    employment_type = str(job.get("job_employment_type", "N/A"))
    city = str(job.get("job_city", "N/A"))
    state = str(job.get("job_state", "N/A"))
    is_remote = bool(job.get("job_is_remote", False))
    apply_link = str(job.get("job_apply_link", "N/A"))
    company_type = str(job.get("employer_company_type", "N/A"))
    employer_name = str(job.get("employer_name", "N/A"))
    publisher = str(job.get("job_publisher", "N/A"))
    posted_at = job.get("job_posted_at_datetime_utc", None)
    offer_expiration = job.get("job_offer_expiration_datetime_utc", None)
    required_experience_in_months = job.get("job_required_experience", {}).get("required_experience_in_months", None)
    latitude = job.get("job_latitude", None)
    longitude = job.get("job_longitude", None)
    
    # Check for valid data types
    if not isinstance(job_id, str) or not isinstance(title, str) or not isinstance(description, str):
        return False, "Invalid data types for job_id, title, or description"
    
    return True, {
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

def insert_jobs_into_db(jobs):
    conn = psycopg2.connect(
        dbname="job_search",
        user=db_user,
        password=db_password,
        host="localhost"
    )
    cur = conn.cursor()

    for job in jobs:
        is_valid, result = validate_job(job)
        if not is_valid:
            print(f"Skipping job due to validation error: {result}")
            continue
        
        cur.execute(
            sql.SQL("INSERT INTO job_listings (job_id, title, description, employment_type, city, state, is_remote, apply_link, company_type, employer_name, publisher, posted_at, offer_expiration, required_experience_in_months, latitude, longitude) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"),
            [result['job_id'], result['title'], result['description'], result['employment_type'], result['city'], result['state'], result['is_remote'], result['apply_link'], result['company_type'], result['employer_name'], result['publisher'], result['posted_at'], result['offer_expiration'], result['required_experience_in_months'], result['latitude'], result['longitude']]
        )
    
    conn.commit()
    cur.close()
    conn.close()

# Fetch jobs from JSearch API
jobs = fetch_jsearch_jobs()

# Insert jobs into PostgreSQL database
insert_jobs_into_db(jobs)

print("Data insertion completed.")