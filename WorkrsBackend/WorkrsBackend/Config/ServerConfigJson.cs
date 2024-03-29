﻿namespace WorkrsBackend.Config
{
    public class ServerConfigJson
    {
        public string ServerName { get; set; }
        public string BackupServer { get; set; }
        public string Mode { get; set; }

        public ServerConfigJson(string serverName, string backupServer, string mode)
        {
            ServerName = serverName;
            BackupServer = backupServer;
            Mode = mode;
        }
    }
}
