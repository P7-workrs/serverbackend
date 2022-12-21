using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using WorkrsBackend.Config;

namespace WorkrsBackend.RabbitMQ
{
    public class RabbitMQHandler : IRabbitMQHandler
    {
        //public Dictionary<Guid, Client> Clients = new();
        List<Guid> _knownConsumers = new List<Guid>();

        readonly IServerConfig _serverConfig;
        IModel? _channel;

        string _clientRegisterQueueName;
        string _clientConnectQueueName;
        string _workerRegisterQueueName;
        string _workerConnectQueueName;
        string _multicastChangeQueue;
        string _multicastLockRequest;
        string _multicastLockReply;

        public RabbitMQHandler(IServerConfig serverConfig)
        {
            _serverConfig = serverConfig;
            Init();
        }

        void Init()
        {
            //var factory = new ConnectionFactory() { HostName = "localhost" };
            var factory = new ConnectionFactory() { HostName = "rabbit2.rabbitmq" };
            factory.UserName = "rabbit2";
            factory.Password = "rabbit2";
            var connection = factory.CreateConnection();
            _channel = connection.CreateModel();
            _channel.BasicQos(0, 1, false);

            _clientRegisterQueueName = _channel.QueueDeclare("ClientRegisterQueue", exclusive: false);
            _clientConnectQueueName = _channel.QueueDeclare(_serverConfig.ServerName + "_clientConnectQueue", exclusive: false);
            _workerRegisterQueueName = _channel.QueueDeclare("WorkerRegisterQueue", exclusive: false);
            _workerConnectQueueName = _channel.QueueDeclare(_serverConfig.ServerName + "_workerConnectQueue", exclusive: false);
            _multicastChangeQueue = _channel.QueueDeclare(_serverConfig.ServerName + "_multicastChanges", exclusive: false);
            _multicastLockRequest = _channel.QueueDeclare(_serverConfig.ServerName + "_multicastLockReq", exclusive: false);
            _multicastLockReply = _channel.QueueDeclare(_serverConfig.ServerName + "_multicastLockReply", exclusive: false);
            CreateExchanges();
        }

        void CreateExchanges()
        {
            _channel.ExchangeDeclare(exchange: "client", type: "topic");
            _channel.ExchangeDeclare(exchange: "server", type: "topic");
            _channel.ExchangeDeclare(exchange: "multicast", type: "direct");
            _channel.ExchangeDeclare(exchange: "worker", type: "topic");

            _channel.QueueBind(queue: _clientRegisterQueueName,
                                     exchange: "server",
                                     routingKey: "clientRegister");

            _channel.QueueBind(queue: _clientConnectQueueName,
                                     exchange: "server",
                                     routingKey: _serverConfig.ServerName + ".clientConnect");

            _channel.QueueBind(queue: _workerRegisterQueueName,
                                     exchange: "server",
                                     routingKey: "workerRegister");

            _channel.QueueBind(queue: _workerConnectQueueName,
                                     exchange: "server",
                                     routingKey: _serverConfig.ServerName + ".workerConnect");
            _channel.QueueBind(queue: _multicastChangeQueue, exchange: "multicast", routingKey: "changes");
            _channel.QueueBind(queue: _multicastLockRequest, exchange: "multicast", routingKey: "requestLock");
        }

        public string Identity
        {
            get { return _serverConfig.ServerName; }
        }

        public void CreateClientRegisterConsumer(EventHandler<BasicDeliverEventArgs> remoteProcedure)
        {
            var registerConsumer = new EventingBasicConsumer(_channel);

            registerConsumer.Received += remoteProcedure;
            _channel.BasicConsume(queue: _clientRegisterQueueName, autoAck: true, consumer: registerConsumer);
        }

        public void CreateClientConnectConsumer(EventHandler<BasicDeliverEventArgs> remoteProcedure)
        {
            var connectConsumer = new EventingBasicConsumer(_channel);
            connectConsumer.Received += remoteProcedure;

            _channel.BasicConsume(queue: _clientConnectQueueName, autoAck: true, consumer: connectConsumer);
        }

        public void CreateWorkerRegisterConsumer(EventHandler<BasicDeliverEventArgs> remoteProcedure)
        {
            var registerConsumer = new EventingBasicConsumer(_channel);

            registerConsumer.Received += remoteProcedure;
            _channel.BasicConsume(queue: _workerRegisterQueueName, autoAck: true, consumer: registerConsumer);
        }

        public void CreateWorkerConnectConsumer(EventHandler<BasicDeliverEventArgs> remoteProcedure)
        {
            var connectConsumer = new EventingBasicConsumer(_channel);
            connectConsumer.Received += remoteProcedure;

            _channel.BasicConsume(queue: _workerConnectQueueName, autoAck: true, consumer: connectConsumer);
        }

        public void CreateMulticastChanges(EventHandler<BasicDeliverEventArgs> remoteProcedure)
        {
            var constumer = new EventingBasicConsumer(_channel);
            constumer.Received += remoteProcedure;

            _channel.BasicConsume(queue: _multicastChangeQueue, autoAck: true, consumer: constumer);
        }

        public void CreateMulticastLockRequest(EventHandler<BasicDeliverEventArgs> remoteProcedure)
        {
            var constumer = new EventingBasicConsumer(_channel);
            constumer.Received += remoteProcedure;

            _channel.BasicConsume(queue: _multicastLockRequest, autoAck: true, consumer: constumer);
        }

        public void CreateMulticastLockReply(EventHandler<BasicDeliverEventArgs> remoteProcedure)
        {
            var constumer = new EventingBasicConsumer(_channel);
            constumer.Received += remoteProcedure;

            _channel.BasicConsume(queue: _multicastLockReply, autoAck: true, consumer: constumer);
        }

        public void Update()
        {

        }

        public void Connect(Guid id, EventHandler<BasicDeliverEventArgs> remoteProcedure)
        {
            var queueName = _channel.QueueDeclare(queue: $"{_serverConfig.ServerName}.{id}", autoDelete: false, exclusive: false);
            _channel.QueueBind(queue: queueName,
                                    exchange: "server",
                                    routingKey: $"{_serverConfig.ServerName}.{id}");

            var consumer = new EventingBasicConsumer(_channel);
            consumer.Received += remoteProcedure;

            if (_knownConsumers.Contains(id))
            {
                _channel.BasicCancel(id.ToString());
            }
            else
            {
                _knownConsumers.Add(id);
            }

            _channel.BasicConsume(queue: queueName,
                                    autoAck: true,
                                    consumer: consumer,
                                    consumerTag: id.ToString());
        }

        public void Publish(string publishExchange, string publishRoutingKey, IBasicProperties props, string message)
        {
            var messageBytes = Encoding.UTF8.GetBytes(message);
            _channel.BasicPublish(exchange: publishExchange, routingKey: publishRoutingKey, basicProperties: props, body: messageBytes);
        }

        public IBasicProperties GetBasicProperties()
        {
            return _channel.CreateBasicProperties();
        }
    }
}
