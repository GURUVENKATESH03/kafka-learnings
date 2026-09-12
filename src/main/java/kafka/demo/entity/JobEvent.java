package kafka.demo.entity;

import java.util.UUID;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import kafka.demo.utils.JobStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "job_events")
public class JobEvent {
    @Id
    private UUID jobId;
    private UUID scheduleId;
    private String[] messages;
    private JobStatus jobStatus;
}