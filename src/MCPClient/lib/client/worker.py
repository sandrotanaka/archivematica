import logging

from dbconns import auto_close_old_connections

from client.job import Job


logger = logging.getLogger("archivematica.mcp.client.worker")


@auto_close_old_connections()
def run_task(task_name, job_module, jobs):
    """Do actual processing of the jobs given."""
    logger.info("\n\n*** RUNNING TASK: %s***", task_name)
    Job.bulk_set_start_times(jobs)

    try:
        job_module.call(jobs)
    except Exception as err:
        logger.exception("*** TASK FAILED: %s***", task_name)
        Job.bulk_mark_failed(jobs, str(err))
        raise
    else:
        for job in jobs:
            job.log_results()
            job.update_task_status()
