namespace WorkrsBackend.Config
{
    public class ServerConfigJson
    {
        public string ServerName { get; set; }
        public string BackupServer { get; set; }

        public ServerConfigJson(string serverName, string backupServer)
        {
            ServerName = serverName;
            BackupServer = backupServer;
        }
    }
}
