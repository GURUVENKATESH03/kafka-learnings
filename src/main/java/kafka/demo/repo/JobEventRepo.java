package kafka.demo.repo;

import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import kafka.demo.entity.JobEvent;
import kafka.demo.utils.JobStatus;

@Repository
public interface JobEventRepo extends JpaRepository<JobEvent, UUID> {

    List<JobEvent> findByJobStatus(JobStatus jobStatus);

    List<JobEvent> findByJobId(UUID jobId);

}
