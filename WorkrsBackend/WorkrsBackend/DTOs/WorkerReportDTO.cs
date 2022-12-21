namespace WorkrsBackend.DTOs
{
    public class WorkerReportDTO
    {
        public string WorkerId { get; set; }
        public Guid JobId { get; set; }

        public WorkerReportDTO(string workerId, Guid jobId)
        {
            WorkerId = workerId;
            JobId = jobId;
        }
    }
}