package kafka.demo.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.transaction.Transactional;
import kafka.demo.entity.JobEvent;
import kafka.demo.repo.JobEventRepo;
import kafka.demo.utils.JobStatus;

@Service
public class JobTable {

    private static final Logger log = LoggerFactory.getLogger(JobTable.class);

    @Autowired
    private JobEventRepo jobEventRepo;

    @Transactional
    public void jobInsertion(JobEvent jobEvent) {
        log.info("JobTable -> jobInsertion : job to be insterted ={}", jobEvent);
        jobEventRepo.save(jobEvent);
    }

    @Transactional
    public boolean jobStatusChanger(JobEvent jobEvent, JobStatus jobStatus) {
        log.info("JobTable -> jobStatusChanger : Previous jobStatus ={}", getJobStatus(jobEvent));
        jobEvent.setJobStatus(jobStatus);
        jobEventRepo.save(jobEvent);
        return true;
    }

    public JobStatus getJobStatus(JobEvent jobEvent) {
        JobStatus jobStatus = jobEventRepo.findByJobId(jobEvent.getJobId()).stream().map(JobEvent::getJobStatus)
                .findFirst().orElse(null);
        log.info("JobTable -> getJobStatus : jobStatus ={} for the jobId ={}", jobStatus, jobEvent.getJobId());
        return jobStatus;
    }
}
