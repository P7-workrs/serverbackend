namespace WorkrsBackend.DTOs
{
    public class RegisterClientResponseDTO
    {
        public Guid ClientId { get; set; }
        public string ServerName { get; set; }
        public string DataServerName { get; set; }

        public RegisterClientResponseDTO(Guid clientId, string serverName, string dataServerName)
        {
            ClientId = clientId;
            ServerName = serverName;
            DataServerName = dataServerName;
        }
    }
}
