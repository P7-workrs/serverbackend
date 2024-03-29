﻿using System.Text.Json;

namespace WorkrsBackend.Config
{
    public class ServerConfig : IServerConfig
    {
        readonly string _serverName = string.Empty;
        readonly string _backupServer = string.Empty;
        int _mode = 0;

        public string ServerName => _serverName;

        public string BackupServer => _backupServer;
        public int Mode { get { return _mode; } set { _mode = value; } }

        public ServerConfig()
        {
            using StreamReader r = new("config.json");
            string json = r.ReadToEnd();
            ServerConfigJson? configJson = JsonSerializer.Deserialize<ServerConfigJson>(json);

            if (configJson == null)
                return;

            _serverName = configJson.ServerName;
            _backupServer = configJson.BackupServer;
            _mode = Int32.Parse(configJson.Mode);
        }
    }
}
