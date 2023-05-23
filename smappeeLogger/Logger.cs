using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Serilog;
using System.Diagnostics;
using RabbitMQ.Client.Events;
using System.Text.Json;
using FluentFTP;

namespace smappeeLogger;

public class Logger
{
    //Number of times to repeat each experiment
    readonly int repeat = 3;


    Serilog.ILogger _logger;
    Smappee _smappee;

    RabbitMQHandler _rabbitmqHandler;
    List<Experiment> experimentsList = new();
    List<Experiment>.Enumerator experiments;
    Stopwatch _sw = new Stopwatch();
    byte[] ScriptBytes;
    string Jobname;
    bool print = false;
    object _myObj = new();

    public Logger(Serilog.ILogger logger)
    {
        _logger = logger;
        _smappee = new Smappee("COM4", 61, _logger);
        _rabbitmqHandler = new RabbitMQHandler();
        _rabbitmqHandler.Init(HandleMessageReceived);
        _rabbitmqHandler.CreateWorkerQueueConsumer(HandleWorkerListen);

        //add experiments to list
        experimentsList.Add(new Experiment(0.0001, 0, 100, 300));
        experimentsList.Add(new Experiment(0.0001, 15, 100, 300));

        experimentsList.Add(new Experiment(0.0005, 0, 200, 300));
        experimentsList.Add(new Experiment(0.0005, 1, 200, 300));
        experimentsList.Add(new Experiment(0.0005, 15, 200, 300));

        experimentsList.Add(new Experiment(0.003, 0, 300, 300));
        experimentsList.Add(new Experiment(0.003, 5, 300, 300));
        experimentsList.Add(new Experiment(0.003, 15, 300, 300));

        experimentsList.Add(new Experiment(0.0131, 0, 400, 300));
        experimentsList.Add(new Experiment(0.0131, 11, 400, 300));
        experimentsList.Add(new Experiment(0.0131, 15, 400, 300));

        experiments = experimentsList.GetEnumerator();
        experiments.MoveNext();

        Console.WriteLine("Interval: " + experimentsList[0].Interval);

        LoadScript();
        Thread.Sleep(2000);
    }

    public bool Update()
    {
        if (print)
        {
            log(_smappee.Readings, false, "running");
        }
        if (experiments.Current == null)
            return false;

        if (experiments.Current.Status == Status.running)
            return true;

        if (experiments.Current.Status == Status.waiting)
        {
            experiments.Current.Status = Status.running;
            StartExperiment();
        }
        else
        {
            experiments.MoveNext();
        }
        return true;
    }

    public void LoadScript()
    {
        
        var path = Path.GetFullPath("./script/script.py");
        ScriptBytes = File.ReadAllBytes(path);
    }

    public void StartExperiment()
    {
        experiments.Current.Count = experiments.Current.Count + 1;
        Jobname = $"Experiment_mu{experiments.Current.Mu}_interval{experiments.Current.Interval}_seed_{experiments.Current.GetSeed()}_{experiments.Current.Count}";
        Jobname = Jobname.Replace(',', '_');
        experiments.Current.Status = experiments.Current.Count > repeat ? Status.done : Status.running;
        if (experiments.Current.Status == Status.running)
        {
            Upload();
        }
    }

    //Handles messages that are send by the server while pretending to be client
    public void HandleMessageReceived(object? model, BasicDeliverEventArgs ea)
    {
        var body = ea.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        var headers = ea.BasicProperties.Headers;
        if (headers != null)
        {
            string msgType = Encoding.UTF8.GetString((byte[])headers["type"]);
            switch (msgType)
            {
                case "startNewTask":
                    HandleRecievedTask(model, ea);
                    break;

                default:
                    break;
            }
        }
    }

    //Handles messages that are intercepted on the worker exchange
    public void HandleWorkerListen(object? model, BasicDeliverEventArgs ea)
    {
        var body = ea.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        var headers = ea.BasicProperties.Headers;

        if (headers != null)
        {
            string msgType = Encoding.UTF8.GetString((byte[])headers["type"]);
            var time = DateTime.Now;

            switch (msgType)
            {
                case "startJob":
                    var file = File.Create($"{Jobname}.csv");
                    file.Close();
                    _sw.Restart();
                    log(_smappee.Readings, true, "start");
                    _logger.Information($"{Jobname} start: {time}");
                    print = true;
                    break;

                case "recoverJob":
                    //Log that job was recovered
                    log(_smappee.Readings, false, "recover");
                    _logger.Information($"{Jobname} recover: {time}");
                    break;

                case "jobDone":
                    _sw.Stop();
                    //Stop logging
                    var stop = DateTime.Now;
                    print = false;
                    _logger.Information($"{Jobname} stop: {time}");
                    log(_smappee.Readings, false, "done");
                    StartExperiment();
                    break;

                default:
                    break;
            }
        }
    }

    void log(SmappeeDto s, bool start, string state)
    {
        lock (_myObj)
        {
            //var line = $"{DateTime.Now.TimeOfDay};{state};{s.Powers[0]};{s.Powers[1]};{s.Powers[2]};{s.Powers[3]};{s.Energy[0]};{s.Energy[1]};{s.Energy[2]};{s.Energy[3]}";
            //var line = $"{_sw.Elapsed};{state};{s.Powers[0]};{s.Powers[1]};{s.Powers[2]};{s.Powers[3]};{s.Energy[0]};{s.Energy[1]};{s.Energy[2]};{s.Energy[3]}";
            var line = $"{_sw.Elapsed};{state};{SumPower(s)};{s.Powers[0]};{s.Powers[1]};{s.Powers[2]};{s.Powers[3]};{SumEnergy(s)};{s.Energy[0]};{s.Energy[1]};{s.Energy[2]};{s.Energy[3]}";


            using (StreamWriter file = new($"{Jobname}.csv", append: true))
            {
                if (start)
                {
                    _sw.Start();
                    //var lineHeaders = "timestamp;state;powerTotal;powerServer;powerWorker1;powerWorker2;energyTotal;energyServer;energyWorker1;energyWorker2";
                    var lineHeaders = "timestamp;state;powerTotal;powerServer1;powerServer2;powerWorker1;powerWorker2;energyTotal;energyServer1;energyServer2;energyWorker1;energyWorker2";
                    file.WriteLine(lineHeaders);
                }

                file.WriteLine(line);
            }
        }


    }

    float SumPower(SmappeeDto s)
    {
        return s.Powers.Sum();
    }

    double SumEnergy(SmappeeDto s)
    {

        return s.Energy.Sum();
    }

    public async Task UploadToServer(byte[] bytes, string location)
    {
        string[] subStr = location.Split(":");
        var ftpClient = new FtpClient(subStr[0], subStr[1], subStr[2]);

        ftpClient.Connect();
        ftpClient.UploadBytes(bytes, subStr[3]);
        ftpClient.Disconnect();
    }

    void SendJobs(string jobName)
    {
        var props = _rabbitmqHandler.GetBasicProperties();
        props.Headers = new Dictionary<string, object>();
        props.Headers.Add("type", "startNewTask");
        props.Headers.Add("mu", experiments.Current.Mu.ToString().Replace(',', '.'));
        props.Headers.Add("interval", experiments.Current.Interval.ToString());
        props.Headers.Add("seed", experiments.Current.GetSeed().ToString());

        _rabbitmqHandler.SendMessage(jobName, props);
    }

    async void HandleRecievedTask(object? model, BasicDeliverEventArgs ea)
    {
        var task = JsonSerializer.Deserialize<ServiceTask>(Encoding.UTF8.GetString(ea.Body.ToArray()));

        await UploadToServer(ScriptBytes, task.SourcePath);
    }

    void Upload()
    {
        SendJobs(Jobname);
    }
}
