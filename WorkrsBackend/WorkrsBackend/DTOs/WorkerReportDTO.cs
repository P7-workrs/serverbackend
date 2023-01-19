namespace WorkrsBackend.DTOs
{
    public class WorkerReportDTO
    {
        public Guid WorkerId { get; set; }
        public Guid JobId { get; set; }

        public WorkerReportDTO(Guid workerId, Guid jobId)
        {
            WorkerId = workerId;
            JobId = jobId;
        }
    }
}