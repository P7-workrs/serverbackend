namespace WorkrsBackend.DTOs
{
    public class RegisterWorkerResponseDTO
    {
        public Guid WorkerId { get; set; }
        public string ServerName { get; set; }

        public RegisterWorkerResponseDTO(Guid workerId, string serverName)
        {
            WorkerId = workerId;
            ServerName = serverName;
        }
    }
}
