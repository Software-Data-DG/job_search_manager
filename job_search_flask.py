from flask import Flask, jsonify
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = "job_search"
db_host = "localhost"

def get_jobs():
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host
        )
        cur = conn.cursor()
        cur.execute("SELECT * FROM job_listings")
        jobs = cur.fetchall()
        cur.close()
        conn.close()
        print("Jobs fetched from the database:", jobs)  # Debugging line
        return jobs
    except Exception as e:
        print(f"Error fetching jobs from the database: {e}")
        return []

@app.route('/')
def home():
    return "Welcome to the Job Search API! Navigate to /jobs to see the job listings."

@app.route('/jobs', methods=['GET'])
def fetch_jobs():
    jobs = get_jobs()
    job_list = []
    for job in jobs:
        job_list.append({
            "job_id": job[1],
            "title": job[2],
            "description": job[3],
            "employment_type": job[4],
            "city": job[5],
            "state": job[6],
            "is_remote": job[7],
            "apply_link": job[8],
            "company_type": job[9],
            "employer_name": job[10],
            "publisher": job[11],
            "posted_at": job[12],
            "offer_expiration": job[13],
            "required_experience_in_months": job[14],
            "latitude": job[15],
            "longitude": job[16],
        })
    print("Jobs to be returned by the endpoint:", job_list)  # Debugging line
    return jsonify(job_list)

if __name__ == '__main__':
    app.run(debug=True)