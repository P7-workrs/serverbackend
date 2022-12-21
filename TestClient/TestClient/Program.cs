using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using TestClient;

namespace TestClient
{
   internal class Program
   {
      static void Main(string[] args)
      {
         var testClient = new TestClient();
         testClient.Register();
         Thread.Sleep(100);
         while (true)
         {
            testClient.SendMessage(Console.ReadLine() ?? string.Empty);
         }
      }
   }
}