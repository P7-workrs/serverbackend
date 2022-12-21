using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace TestWorker
{
   public class TestWorker
   {
      IModel _channel;
      string _workerId = "49e60bb6-4e43-4e77-a354-099769fbd670";
      string _serverName;
      string _replyQueueName;
      string _correlationId;
      string _replyConsumerTag;

      public TestWorker()
      {
         init();
      }

      void init()
      {
         var factory = new ConnectionFactory() { HostName = "localhost" };
         var connection = factory.CreateConnection();
         _channel = connection.CreateModel();

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
               _workerId = responseDto?.WorkerId ?? string.Empty;
               Console.WriteLine(_workerId);
               _serverName = responseDto?.ServerName ?? string.Empty;
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

         var workerId = "";

         var messageBytes = Encoding.UTF8.GetBytes(workerId);

         _channel.BasicPublish(exchange: "server", routingKey: "workerRegister", basicProperties: props, body: messageBytes);

         Console.WriteLine("Registration sent to server");
      }

      public void Connect()
      {
         var messageBytes = Encoding.UTF8.GetBytes(_workerId);
         _channel.BasicPublish(exchange: "server", routingKey: $"{_serverName}.workerConnect", body: messageBytes);
         Console.WriteLine("Connected to server {}. You can now freely send messages!");
         _channel.BasicCancel(_replyConsumerTag);
      }

      public void SendMessage(string message)
      {
         var messageBytes = Encoding.UTF8.GetBytes(message);
         _channel.BasicPublish(exchange: "server", routingKey: $"{_serverName}.{_workerId}", body: messageBytes);
         Console.WriteLine("Message sent");
      }
   }
}

