from flask import Flask, jsonify
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

def get_jobs():
    conn = psycopg2.connect(
        dbname="job_search",
        user=db_user,
        password=db_password,
        host="localhost"
    )
    cur = conn.cursor()
    cur.execute("SELECT * FROM job_listings")
    jobs = cur.fetchall()
    cur.close()
    conn.close()
    return jobs

@app.route('/')
def home():
    return "Welcome to the Job Search API! Navigate to /jobs to see the job listings."

@app.route('/jobs', methods=['GET'])
def fetch_jobs():
    jobs = get_jobs()
    job_list = []
    for job in jobs:
        job_list.append({
            "job_id": job[0],
            "title": job[1],
            "description": job[2],
            "employment_type": job[3],
            "city": job[4],
            "state": job[5],
            "is_remote": job[6],
            "apply_link": job[7],
            "company_type": job[8],
            "employer_name": job[9],
            "publisher": job[10],
            "posted_at": job[11],
            "offer_expiration": job[12],
            "required_experience_in_months": job[13],
            "latitude": job[14],
            "longitude": job[15],
        })
    return jsonify(job_list)

if __name__ == '__main__':
    app.run(debug=True)