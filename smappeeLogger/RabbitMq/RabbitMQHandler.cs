using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using WorkrsBackend.Config;

namespace smappeeLogger;

public class RabbitMQHandler
{
    IModel? _channel;
    string _workerQueue;
    string _clientId = string.Empty;
    string _serverName;
    string _dataServerName;
    string _replyQueueName;
    string _correlationId;
    string _replyConsumerTag;
    string _consumerTag = string.Empty;
    EventHandler<BasicDeliverEventArgs>? _handleMessageReceived = null;


    public RabbitMQHandler()
    {
    }

    public void Init(EventHandler<BasicDeliverEventArgs> handleMessageReceived)
    {
        _handleMessageReceived = handleMessageReceived;
        var factory = new ConnectionFactory() { UserName = "admin", Password = "admin", HostName = "192.168.1.10" };
        var connection = factory.CreateConnection();
        _channel = connection.CreateModel();
        _channel.BasicQos(0, 1, false);

        _workerQueue = _channel.QueueDeclare("workerQueue", exclusive: false);

        _channel.ExchangeDeclare(exchange: "worker", type: "topic");

        _channel.QueueBind(queue: _workerQueue,
                                 exchange: "worker",
                                 routingKey: "#");


        var queueName = _channel.QueueDeclare("ClientRegisterQueue", exclusive: false);

        _channel.ExchangeDeclare(exchange: "server", type: "topic");

        _channel.QueueBind(queue: queueName,
                                 exchange: "server",
                                 routingKey: "clientRegister");
        _correlationId = Guid.NewGuid().ToString();

        _replyQueueName = _channel.QueueDeclare(autoDelete: true, exclusive: true).QueueName;

        var consumer = new EventingBasicConsumer(_channel);

        consumer.Received += (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var response = Encoding.UTF8.GetString(body);

            if (ea.BasicProperties.CorrelationId == _correlationId)
            {
                ResponseDTO? responseDto = JsonSerializer.Deserialize<ResponseDTO>(response);
                _clientId = responseDto?.ClientId ?? string.Empty;
                _serverName = responseDto?.ServerName ?? string.Empty;
                _dataServerName = responseDto?.DataServerName ?? string.Empty;
                Console.WriteLine($"Received response: clientId = {_clientId}, server = {_serverName}, data server = {_dataServerName}");

                var queueName = _channel.QueueDeclare("Client_" + _clientId, exclusive: false);

                _channel.QueueBind(queue: queueName,
                                 exchange: "client",
                                 routingKey: _clientId);

                Connect();
            }
            else
            {
                Console.WriteLine($"Expected correlationId: {_correlationId} but received response with correlationId: {ea.BasicProperties.CorrelationId}");
            }
        };

        _replyConsumerTag = _channel.BasicConsume(
            consumer: consumer,
            queue: _replyQueueName,
            autoAck: true);

        Register("test");
    }

    public void CreateWorkerQueueConsumer(EventHandler<BasicDeliverEventArgs> remoteProcedure)
    {
        var consumer = new EventingBasicConsumer(_channel);

        consumer.Received += remoteProcedure;
        _channel.BasicConsume(queue: _workerQueue, autoAck: true, consumer: consumer);
    }

    public void Register(string Username)
    {
        var props = _channel.CreateBasicProperties();

        props.CorrelationId = _correlationId;
        props.ReplyTo = _replyQueueName;

        Console.WriteLine($"Username: {Username}");
        var username = Username;

        var messageBytes = Encoding.UTF8.GetBytes(username);

        _channel.BasicPublish(exchange: "server", routingKey: "clientRegister", basicProperties: props, body: messageBytes);

        Console.WriteLine("Registration sent to server");
    }

    public void Connect()
    {
        var messageBytes = Encoding.UTF8.GetBytes(_clientId);
        _channel.BasicPublish(exchange: "server", routingKey: $"{_serverName}.clientConnect", body: messageBytes);
        Console.WriteLine($"Connected to server {_serverName}. You can now freely send messages!");
        var consumer = new EventingBasicConsumer(_channel);
        consumer.Received += _handleMessageReceived;

        _consumerTag = _channel.BasicConsume(queue: "Client_" + _clientId,
            true,
            consumer: consumer,
            consumerTag: _clientId);

        _channel.BasicCancel(_replyConsumerTag);
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

    public void SendMessage(string message, IBasicProperties props)
    {
        var messageBytes = Encoding.UTF8.GetBytes(message);
        _channel.BasicPublish(exchange: "server", routingKey: $"{_serverName}.{_clientId}",basicProperties: props, body: messageBytes);
        Console.WriteLine("Message sent!!!!");
    }
}
