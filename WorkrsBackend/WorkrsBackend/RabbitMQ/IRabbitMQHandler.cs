using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace WorkrsBackend.RabbitMQ
{
    public interface IRabbitMQHandler
    {
        string Identity { get; }

        void Connect(Guid id, EventHandler<BasicDeliverEventArgs> remoteProcedure);
        void CreateClientConnectConsumer(EventHandler<BasicDeliverEventArgs> remoteProcedure);
        void CreateClientRegisterConsumer(EventHandler<BasicDeliverEventArgs> remoteProcedure);
        void CreateMulticastChanges(EventHandler<BasicDeliverEventArgs> remoteProcedure);
        void CreateMulticastLockReply(EventHandler<BasicDeliverEventArgs> remoteProcedure);
        void CreateMulticastLockRequest(EventHandler<BasicDeliverEventArgs> remoteProcedure);
        void CreateWorkerConnectConsumer(EventHandler<BasicDeliverEventArgs> remoteProcedure);
        void CreateWorkerRegisterConsumer(EventHandler<BasicDeliverEventArgs> remoteProcedure);
        IBasicProperties GetBasicProperties();
        void Publish(string publishExchange, string publishRoutingKey, IBasicProperties props, string message);
        void Update();
    }
}