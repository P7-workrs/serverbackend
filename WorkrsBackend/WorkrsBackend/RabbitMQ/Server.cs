namespace WorkrsBackend.RabbitMQ
{
    public enum ServerMode
    {
        Primary,
        Secondary
    }

    public class Server
    {
        public string Name { get; set; }
        public string PairServer { get; set; }
        public ServerMode Mode { get; set; }
        public Server(string name, string pairServer, ServerMode mode)
        {
            Name = name;
            PairServer = pairServer;
            Mode = mode;
        }
    }
}
