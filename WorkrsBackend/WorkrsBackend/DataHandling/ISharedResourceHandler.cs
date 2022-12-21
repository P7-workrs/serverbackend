using WorkrsBackend.RabbitMQ;

namespace WorkrsBackend.DataHandling
{
    public interface ISharedResourceHandler
    {
        public void AddClientToClientDHT(Client client);
        public void CreateClient(Client client);
        public bool ClientExists(Guid clientId);
        public Client? GetClientById(Guid clientId);
        public bool WorkerExists(Guid clientId);
        public Worker? GetWorkerById(Guid clientId);
        public Client? FindClientByUserName(string username);
        public void UpdateClientDHT(Client client);
        public void UpdateWorkerDHT(Worker worker);
        public Worker? GetAvailableWorker();
        public Dictionary<string, Server> GetPrimaryServers();
        public Server? GetServerInfo(string serverName);
        public void AddWorkerToWorkerDHT(Worker worker);
        public void AddTask(ServiceTask task);
        public void UpdateTask(ServiceTask task);
        public ServiceTask? GetTaskFromId(Guid id);
        public List<ServiceTask> GetTaskForClient(Guid clientId);
        public List<ServiceTask> GetTasksFromStatus(ServiceTaskStatus status);
    }
}
