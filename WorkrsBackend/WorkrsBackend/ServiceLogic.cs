using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using WorkrsBackend.Config;
using WorkrsBackend.DataHandling;
using WorkrsBackend.DTOs;
using WorkrsBackend.FTP;
using Serilog;
using WorkrsBackend.RabbitMQ;
using SQLitePCL;

namespace WorkrsBackend
{
    public class ServiceLogic : IHostedService
    {
        readonly IRabbitMQHandler _rabbitMQHandler;
        readonly IServerConfig _serverConfig;
        readonly ISharedResourceHandler _dataAccessHandler;
        readonly FTPHandler _ftpHandler;
        Dictionary<Guid, TaskInProgress> _tasks = new();

        public ServiceLogic(IServerConfig serverConfig, ISharedResourceHandler dataAccessHandler, IRabbitMQHandler rabbitMQHandler)
        {
            _serverConfig = serverConfig;
            _dataAccessHandler = dataAccessHandler;
            _rabbitMQHandler = rabbitMQHandler;
            _ftpHandler = new FTPHandler();
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            Init();
            Log.Information("Init completed");
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    Update();
                }
                catch (Exception ex)
                {
                    Log.Error("Update: " + ex.ToString());
                }
                Thread.Sleep(2000);
            }
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        void Init()
        {
            _rabbitMQHandler.CreateClientRegisterConsumer(ClientRegistrationReceived);
            _rabbitMQHandler.CreateClientConnectConsumer(ClientConnectionReceived);
            _rabbitMQHandler.CreateWorkerRegisterConsumer(WorkerRegistrationReceived);
            _rabbitMQHandler.CreateWorkerConnectConsumer(WorkerConnectionReceived);
            _ftpHandler.Init(_serverConfig.ServerName + ".servicehost", _serverConfig.ServerName + "user", "1234");
            //_ftpHandler.Init("localhost", "p1user", "1234");
        }

        bool test = false;
        void Update()
        {
            _rabbitMQHandler.Update();
            if(test)
            {
                _dataAccessHandler.AddClientToClientDHT(new Client(Guid.NewGuid(),"test", "P1", "P1"));
                Client? c =_dataAccessHandler.FindClientByUserName("test");
                if(c != null) 
                {
                    ServiceTask st = new ServiceTask(Guid.NewGuid(), c.ClientId, "myTestTask", ServiceTaskStatus.Created, "p1.source", "p1.backup", "p1.result" );
                    _dataAccessHandler.AddTask(st);
                    st.Name = "test12";
                    _dataAccessHandler.UpdateTask(st);
                }
                test = false;
            }

            HandleStartTasks();
            HandleInProgressTasks();
            HandleCancelTasks();
            Log.Debug("Update alive");
            //var t = _dataAccessHandler.GetTaskFromId(Guid.Parse("1EA20BB4-A25B-4507-928C-E1C5C860B18E"));
            //var t1 = _dataAccessHandler.GetTaskForClient(Guid.Parse("91AD37D3-4057-486E-9005-CE296E7552FB"));


        }

        void HandleStartTasks()
        {
            var jobs = _dataAccessHandler.GetTasksFromStatus(ServiceTaskStatus.Created);
            if(jobs.Count > 0)
                Log.Debug($"HandleStartTasks, jobs to start: {jobs.Count}");
            foreach (var job in jobs)
            {
                StartJob(job);
            }
        }

        void HandleInProgressTasks()
        {
            var jobs = _dataAccessHandler.GetTasksFromStatus(ServiceTaskStatus.InProgress);

        }
        void HandleCancelTasks()
        {
            var jobs = _dataAccessHandler.GetTasksFromStatus(ServiceTaskStatus.Cancel);
            if(jobs.Count > 0)
                Log.Debug($"HandleCancelTasks, jobs to cancel: {jobs.Count}");
            foreach (var job in jobs)
            {
                TaskInProgress tp;
                if (_tasks.TryGetValue(job.Id, out tp))
                {
                    StopJob(tp.Worker.WorkerId, tp.ServiceTask);
                }
                else
                {
                    job.Status = ServiceTaskStatus.Canceled;
                    _dataAccessHandler.UpdateTask(job);
                }
            }

        }

        void ClientRegistrationReceived(object? model, BasicDeliverEventArgs ea)
        {
            try
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var props = ea.BasicProperties;

                var client = _dataAccessHandler.FindClientByUserName(message);
                if (client == null)
                {
                    Log.Debug("ClientRegistrationReceived: unknown user");
                    return;
                }

                var replyProps = _rabbitMQHandler.GetBasicProperties();
                replyProps.CorrelationId = props.CorrelationId;

                string response = JsonSerializer.Serialize(new RegisterClientResponseDTO(client.ClientId, _serverConfig.ServerName, _serverConfig.ServerName));
                _rabbitMQHandler.Publish("", props.ReplyTo, replyProps, response);
                Log.Debug("ClientRegistrationReceived: " + response);
            }
            catch(Exception ex)
            {
                Log.Error("ClientRegistrationReceived: " + ex.ToString());
            } 
        }

        void ClientConnectionReceived(object? model, BasicDeliverEventArgs ea)
        {
            try
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var clientId = Guid.Parse(message);
                AddClient(clientId);
                Log.Debug("ClientConnectionReceived: " + clientId);
            }
            catch (Exception ex)
            {
                Log.Error("ClientConnectionReceived: " + ex.ToString());
            }
        }

        void WorkerRegistrationReceived(object? model, BasicDeliverEventArgs ea)
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            var props = ea.BasicProperties;

            var workerId = Guid.NewGuid();

            var replyProps = _rabbitMQHandler.GetBasicProperties();
            replyProps.CorrelationId = props.CorrelationId;

            string response = JsonSerializer.Serialize(new RegisterWorkerResponseDTO(workerId, _serverConfig.ServerName));

            _rabbitMQHandler.Publish("", props.ReplyTo, replyProps, response);
            Log.Debug($"WorkerRegistrationReceived: {workerId}");
        }

        void WorkerConnectionReceived(object? model, BasicDeliverEventArgs ea)
        {
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            var workerId = Guid.Parse(message);
            AddWorker(workerId);
            Log.Debug($"WorkerConnectionReceived: {workerId}");
        }

        void AddClient(Guid clientId)
        {
            Client? c = _dataAccessHandler.GetClientById(clientId);
            if (c == null) return;
            c.ServerName = c.DataServer;
            _dataAccessHandler.UpdateClientDHT(c);
            _rabbitMQHandler.Connect(c.ClientId, HandleClientRequest);
        }

        void HandleClientRequest(object? model, BasicDeliverEventArgs ea)
        {
            Task.Run(() => {
                try
                {
                    Client? c = _dataAccessHandler.GetClientById(Guid.Parse(ea.ConsumerTag));
                    if (c != null)
                    {
                        if(ea.BasicProperties.Headers != null)
                        {
                            var props = _rabbitMQHandler.GetBasicProperties();
                            props.Headers = new Dictionary<string, object>();
                            props.Headers.Add("type", "");
                            string msgType = Encoding.UTF8.GetString((byte[])ea.BasicProperties.Headers["type"]);
                            switch (msgType)
                            {
                                case "startNewTask":
                                    {
                                        var t = new ServiceTask(
                                        Guid.NewGuid(),
                                        c.ClientId,
                                        Encoding.UTF8.GetString(ea.Body.ToArray()),
                                        ServiceTaskStatus.Created);
                                        string source = $"{c.ClientId}/{t.Id}/source/";
                                        string backup = $"{c.ClientId}/{t.Id}/backup/";
                                        string result = $"{c.ClientId}/{t.Id}/result/";
                                        _ftpHandler.CreateDirectory(source);
                                        _ftpHandler.CreateDirectory(backup);
                                        _ftpHandler.CreateDirectory(result);

                                        t.SourcePath = $"{_ftpHandler.HostName}:{_ftpHandler.Username}:{_ftpHandler.Password}:{source}{t.Name}.py";
                                        t.BackupPath = $"{_ftpHandler.HostName}:{_ftpHandler.Username}:{_ftpHandler.Password}:{backup}";
                                        t.ResultPath = $"{_ftpHandler.HostName}:{_ftpHandler.Username}:{_ftpHandler.Password}:{result}";
                                        _dataAccessHandler.AddTask(t);
                                        props.Headers["type"] = "startNewTask";
                                        string s = JsonSerializer.Serialize(t);
                                        Log.Debug($"HandleClientRequest_startNewTask, client:{c.ClientId}, Task id: {t.Id}");
                                        _rabbitMQHandler.Publish("client", $"{c.ClientId}", props, s);
                                    }
                                    break;
                                case "taskUploadCompleted":
                                    {
                                        var t = _dataAccessHandler.GetTaskFromId(Guid.Parse(Encoding.UTF8.GetString(ea.Body.ToArray())));
                                        t.Status = ServiceTaskStatus.Starting;
                                        _dataAccessHandler.UpdateTask(t);
                                        Log.Debug($"HandleClientRequest_taskUploadCompleted, client:{c.ClientId}, Task id: {t.Id}");
                                    }
                                    break;
                                case "getServiceTasks":
                                    {
                                        var t = _dataAccessHandler.GetTaskForClient(c.ClientId);
                                        props.Headers["type"] = "getServiceTasks";
                                        string s = JsonSerializer.Serialize(t);
                                        Log.Debug($"HandleClientRequest_getServiceTasks, client:{c.ClientId}");
                                        _rabbitMQHandler.Publish("client", $"{c.ClientId}", props, s);
                                    }
                                    break;
                                case "cancelServiceTask":
                                    {

                                        var t = _dataAccessHandler.GetTaskFromId(Guid.Parse(Encoding.UTF8.GetString(ea.Body.ToArray())));
                                        t.Status = ServiceTaskStatus.Cancel;
                                        _dataAccessHandler.UpdateTask(t);
                                        Log.Debug($"HandleClientRequest_cancelServiceTask, client:{c.ClientId}, Task:{t.Id}");
                                    }
                                    break;
                                default:
                                    {
                                        Log.Debug($"Unkown message tag from client: {c.ClientId}");
                                        break;
                                    }
                            }
                        }
                        Log.Debug("HandleClientRequest, Headers was set to NULL");
                    }
                    else
                        Log.Debug("HandleClientRequest, Unknown client");
                }
                catch (Exception ex)

                {
                    Log.Error("HandleClientRequest: " + ex.ToString());
                }
            });
        }


        void HandleWorkerRequest(object? model, BasicDeliverEventArgs ea)
        {
            Task.Run(() =>
            {
                Worker? w = _dataAccessHandler.GetWorkerById(Guid.Parse(ea.ConsumerTag));
                if (w != null)
                {
                    if (ea.BasicProperties.Headers != null)
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        var props = _rabbitMQHandler.GetBasicProperties();
                        props.Headers = new Dictionary<string, object>();
                        props.Headers.Add("type", "");
                        string msgType = Encoding.UTF8.GetString((byte[])ea.BasicProperties.Headers["type"]);
                        switch (msgType)
                        {
                            case "startJob":
                                {
                                    Guid guid = Guid.Parse(message);
                                    if (_tasks.ContainsKey(guid))
                                    {
                                        TaskInProgress tp = _tasks[guid];
                                        var t = _dataAccessHandler.GetTaskFromId(tp.ServiceTask.Id);
                                        t.Status = ServiceTaskStatus.InProgress;
                                        _dataAccessHandler.UpdateTask(t);
                                        _tasks[guid].ServiceTask = t;
                                        Log.Debug($"Worker: {w.WorkerId} started task: {t.Id}");
                                    }
                                    else
                                    {
                                        props.Headers["type"] = "stopJob";
                                        Log.Debug($"HandleWorkerRequest_jobStarted, {w.WorkerId} forced to stopped");
                                        _rabbitMQHandler.Publish("worker", w.WorkerId.ToString(), props, "");
                                    }
                                }
                                break;
                            case "report":
                                {
                                    WorkerReportDTO report = JsonSerializer.Deserialize<WorkerReportDTO>(message);
                                    Log.Debug($"HandleWorkerRequest_report, Worker: {report.WorkerId}");
                                    if (w.JobId != report.JobId)
                                    {
                                        Log.Debug($"HandleWorkerRequest_report, {report.WorkerId}, incorrect task!");
                                        StopJob(w.WorkerId, _dataAccessHandler.GetTaskFromId(report.JobId));
                                    }
                                }
                                break;
                            case "jobDone":
                                {
                                    var t = _dataAccessHandler.GetTaskFromId(Guid.Parse(Encoding.UTF8.GetString(ea.Body.ToArray())));
                                    t.Status = ServiceTaskStatus.Completed;
                                    _dataAccessHandler.UpdateTask(t);
                                    Log.Debug($"HandleWorkerRequest_jobDone, task: {t.Id}");
                                }
                                break;
                            case "stopJob":
                                {
                                    TaskInProgress? tp;
                                    if (_tasks.TryGetValue(Guid.Parse(Encoding.UTF8.GetString(ea.Body.ToArray())), out tp))
                                    {
                                        var t = _dataAccessHandler.GetTaskFromId(tp.ServiceTask.Id);
                                        t.Status = ServiceTaskStatus.Canceled;
                                        _dataAccessHandler.UpdateTask(t);
                                        Worker worker = _dataAccessHandler.GetWorkerById(tp.Worker.WorkerId);
                                        worker.Status = WorkerStatus.Available;
                                        _dataAccessHandler.UpdateWorkerDHT(worker);
                                        _tasks.Remove(t.Id);
                                        Log.Debug($"HandleWorkerRequest_stopJob, task: {t.Id}");
                                    }
                                }
                                break;
                            default:
                                Log.Debug($"Unkown msg type: {msgType}");
                                break;
                        }
                    }
                    else
                        Log.Debug("HandleWorkerRequest headers was set to NULL");
                }
                else
                    Log.Debug("HandleWorkerRequest, Unknown worker");
            });
        }

        void StopJob(Guid workerId, ServiceTask job)
        {
            var props = _rabbitMQHandler.GetBasicProperties();
            props.Headers = new Dictionary<string, object>();
            props.Headers.Add("type", "stopJob");

            var message = JsonSerializer.Serialize(job);
            _rabbitMQHandler.Publish("worker", workerId.ToString(), props, message);
            Log.Debug($"StopJob sent to worker; {workerId}, task: {job.Id}");
        }

        void RecoverJob(ServiceTask job)
        {
            var worker = _dataAccessHandler.GetAvailableWorker();
            worker.Status = WorkerStatus.Busy;
            worker.JobId = job.Id;
            _dataAccessHandler.UpdateWorkerDHT(worker);

            var props = _rabbitMQHandler.GetBasicProperties();
            props.Headers = new Dictionary<string, object>();
            props.Headers.Add("type", "recoverJob");

            var message = JsonSerializer.Serialize(job);
            _rabbitMQHandler.Publish("worker", worker.WorkerId.ToString(), props, message);
            Log.Debug($"RecoverJob sent to worker; {worker.WorkerId}, task: {job.Id}");
        }

        void StartJob(ServiceTask job)
        {
            if(!_tasks.ContainsKey(job.Id))
            {
                var worker = _dataAccessHandler.GetAvailableWorker();
                if (worker == null)
                    return;

                worker.Status = WorkerStatus.Busy;
                worker.JobId = job.Id;
                _dataAccessHandler.UpdateWorkerDHT(worker);
                _tasks.Add(job.Id, new TaskInProgress(job, worker));
                var props = _rabbitMQHandler.GetBasicProperties();
                props.Headers = new Dictionary<string, object>();
                props.Headers.Add("type", "startJob");
                var message = JsonSerializer.Serialize(job);
                _rabbitMQHandler.Publish("worker", worker.WorkerId.ToString(), props, message);
                Log.Debug($"StartJob sent to worker; {worker.WorkerId}, task: {job.Id}");
            }
        }

        void AddWorker(Guid workerId)
        {
            if (!_dataAccessHandler.WorkerExists(workerId))
            {
                _dataAccessHandler.AddWorkerToWorkerDHT(new Worker(workerId, WorkerStatus.Available, _serverConfig.ServerName));
            }
            else
            {
                Worker? w = _dataAccessHandler.GetWorkerById(workerId);
                w.ServerName = _serverConfig.ServerName;
                _dataAccessHandler.UpdateWorkerDHT(w);
            }
            _rabbitMQHandler.Connect(workerId, HandleWorkerRequest);
        }
    }
}