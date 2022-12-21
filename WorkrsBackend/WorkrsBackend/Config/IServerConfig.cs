namespace WorkrsBackend.Config
{
    public interface IServerConfig
    {
        public string ServerName { get; }
        public string BackupServer { get; }
        public int Mode { get; set; }
    }
}