using System.Text.Json;

namespace WorkrsBackend.Config
{
    public class ServerConfig : IServerConfig
    {
        readonly string _serverName = string.Empty;
        readonly string _backupServer = string.Empty;

        public string ServerName => _serverName;

        public string BackupServer => _backupServer;
        public int Mode { get; set; }

        public ServerConfig()
        {
            using StreamReader r = new("config.json");
            string json = r.ReadToEnd();
            ServerConfigJson? configJson = JsonSerializer.Deserialize<ServerConfigJson>(json);

            if (configJson == null)
                return;

            _serverName = configJson.ServerName;
            _backupServer = configJson.BackupServer;
        }
    }
}
