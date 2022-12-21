using RabbitMQ;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace WorkrsBackend.RabbitMQ
{
    public class Client
    {
        public Guid ClientId { get; }
        public string Username { get; set; }
        public string ServerName { get; set; }
        public string DataServer { get; }

        public Client(Guid clientId, string username, string serverName, string dataServer)
        {
            ClientId = clientId;
            Username = username;
            ServerName = serverName;
            DataServer = dataServer;
        }
    }
}