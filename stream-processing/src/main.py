import signal
import threading

import structlog

from src.config import settings
from src.jobs.aggregation_job import AggregationJob
from src.jobs.anomaly_detection_job import AnomalyDetectionJob
from src.jobs.base_job import BaseJob
from src.jobs.data_cleaning_job import DataCleaningJob
from src.jobs.feature_engineering_job import FeatureEngineeringJob
from src.jobs.real_time_join_job import RealTimeJoinJob
from src.jobs.signal_generation_job import SignalGenerationJob
from src.utils.monitoring import start_metrics_server

logger = structlog.get_logger(__name__)

shutdown_event = threading.Event()


def _handle_shutdown(signum: int, frame: object) -> None:
    logger.info("Shutdown signal received, stopping all jobs", signal=signum)
    shutdown_event.set()


def build_jobs() -> list[BaseJob]:
    return [
        DataCleaningJob(),
        AggregationJob(),
        AnomalyDetectionJob(),
        FeatureEngineeringJob(),
        SignalGenerationJob(),
        RealTimeJoinJob(),
    ]


def run_job_thread(job: BaseJob) -> None:
    """Run job.run() with crash-restart loop."""
    while not shutdown_event.is_set():
        try:
            job._running = True
            job.run()
        except Exception as exc:
            logger.error(
                "Job crashed, restarting in 30s",
                job=job.job_name,
                error=str(exc),
            )
            if not shutdown_event.is_set():
                shutdown_event.wait(timeout=30)

    job._running = False
    logger.info("Job thread exiting", job=job.job_name)


def main() -> None:
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    start_metrics_server(port=settings.STREAM_METRICS_PORT)
    logger.info("Stream metrics server started", port=settings.STREAM_METRICS_PORT)

    jobs = build_jobs()
    threads = [
        threading.Thread(
            target=run_job_thread,
            args=(j,),
            name=j.job_name,
            daemon=True,
        )
        for j in jobs
    ]

    for t in threads:
        t.start()
        logger.info("Job thread started", name=t.name)

    shutdown_event.wait()
    logger.info("Waiting for job threads to finish")

    for j in jobs:
        j.shutdown()
    for t in threads:
        t.join(timeout=35)

    logger.info("All stream jobs stopped. Goodbye.")


if __name__ == "__main__":
    main()
