from data_loader.s3_downloader import download_file_from_s3, move_s3_file_to_archive
from datetime import datetime
import logging

def run_job(job):
    job_logger = logging.getLogger(job['job_name'])
    job_logger.setLevel(logging.INFO)
    fh = logging.FileHandler(f"logs/{job['job_name']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    job_logger.addHandler(fh)

    try:
        job_logger.info(f"Starting job: {job['job_name']}")
        local_file_path = download_file_from_s3(job['s3_bucket'], job['s3_key'])
        job_logger.info(f"Simulated load complete for {job['target_table']}")
        archive_key = move_s3_file_to_archive(job["s3_bucket"], job["s3_key"])
        job_logger.info(f"Archived to s3://{job['s3_bucket']}/{archive_key}")
        job_logger.info(f"✅ Job completed: {job['job_name']}")
    except Exception as e:
        job_logger.exception(f"❌ Job failed: {job['job_name']} - {str(e)}")
