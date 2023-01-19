namespace WorkrsBackend.RabbitMQ
{
    public class TaskInProgress
    {
        public ServiceTask ServiceTask { get; set; }
        public Worker? Worker { get; set; }

        public TaskInProgress(ServiceTask serviceTask, Worker worker)
        {
            ServiceTask = serviceTask;
            Worker = worker;
        }
    }
}
