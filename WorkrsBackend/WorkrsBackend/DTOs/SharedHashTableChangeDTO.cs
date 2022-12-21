namespace WorkrsBackend.DTOs
{
    public class SharedHashTableChangeDTO
    {
        public ChangeType DataType { get; set; }
        public Guid ChangeId { get; set; }
        public string? Change { get; set; }
    }
}
