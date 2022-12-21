using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text.Json;

namespace TestClient
{
   public class TestClient
   {
      IModel _channel;
      string _clientId;
      string _serverName;
      string _dataServerName;
      string _replyQueueName;
      string _correlationId;
      string _replyConsumerTag;

      public TestClient()
      {
         init();
      }

      void init()
      {
         var factory = new ConnectionFactory() { HostName = "localhost" };
         var connection = factory.CreateConnection();
         _channel = connection.CreateModel();
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
               Console.WriteLine("Received response: clientId = {0}, server = {1}, data server = {2}", _clientId, _serverName, _dataServerName);

               Connect();
            }
            else
            {
               Console.WriteLine("Expected correlationId: {0} but received response with correlationId: {1}", _correlationId, ea.BasicProperties.CorrelationId);
            }
         };

        _replyConsumerTag = _channel.BasicConsume(
            consumer: consumer,
            queue: _replyQueueName,
            autoAck: true);
      }

      public void Register()
      {
         var props = _channel.CreateBasicProperties();
         
         props.CorrelationId = _correlationId;
         props.ReplyTo = _replyQueueName;

         Console.WriteLine("Username: ");
         var username = Console.ReadLine() ?? string.Empty;

         var messageBytes = Encoding.UTF8.GetBytes(username);

         _channel.BasicPublish(exchange: "server", routingKey: "clientRegister", basicProperties: props, body: messageBytes);

         Console.WriteLine("Registration sent to server");
      }

      public void Connect()
      {
         var messageBytes = Encoding.UTF8.GetBytes(_clientId);
         _channel.BasicPublish(exchange: "server", routingKey: $"{_serverName}.clientConnect", body: messageBytes);
         Console.WriteLine("Connected to server {}. You can now freely send messages!");
         _channel.BasicCancel(_replyConsumerTag);
      }

      public void SendMessage(string message)
      {
         var messageBytes = Encoding.UTF8.GetBytes(message);
         _channel.BasicPublish(exchange: "server", routingKey: $"{_serverName}.{_clientId}", body: messageBytes);
         Console.WriteLine("Message sent");
      }
   }
}
