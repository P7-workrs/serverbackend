namespace WorkrsBackend.DTOs
{
    public enum ChangeType
    {
        Client,
        Worker,
        Server,
    }

    public class RequestLockDTO
    {
        public ChangeType LockType { get; set; }
        public Guid ChangeId { get; set; }
    }
}
