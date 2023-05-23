using Microsoft.AspNetCore.Mvc.TagHelpers.Cache;
using Microsoft.Data.Sqlite;
using System.Data;
using System.Runtime.ConstrainedExecution;
using WorkrsBackend.RabbitMQ;
using static System.Net.Mime.MediaTypeNames;

namespace WorkrsBackend.DataHandling
{
    public class DataAccessHandler : IDataAccessHandler
    {
        const string sharedConnectionStringName = "Data Source=servicehostSharded.db";
        const string localConnectionStringName = "Data Source=servicehostLocal.db";
        string sharedConnectionString = new SqliteConnectionStringBuilder(sharedConnectionStringName)
        {
            Mode = SqliteOpenMode.ReadWriteCreate,
        }.ToString();

        string localConnectionString = new SqliteConnectionStringBuilder(localConnectionStringName)
        {
            Mode = SqliteOpenMode.ReadWriteCreate,
        }.ToString();

        SqliteConnection sharedDatabase;
        SqliteConnection localDatabase;

        public DataAccessHandler()
        {
            CreateShardedDatabase();
            CreateLocalDatabase();
        }

        void CreateLocalDatabase()
        {
            localDatabase = new SqliteConnection(localConnectionString);
            localDatabase.Open();
            var command = localDatabase.CreateCommand();

            command.CommandText =
          @"
                CREATE TABLE IF NOT EXISTS serviceTask(
                    tasksId TEXT PRIMARY KEY NOT NULL, 
                    clientId TEXT NOT NULL,
                    name TEXT NOT NULL,
                    status INTEGER NOT NULL,
                    sourcePath TEXT NOT NULL,
                    backupPath TEXT NOT NULL,
                    resultPath TEXT NOT NULL
                );
            ";
            command.ExecuteNonQuery();

        }

        void CreateShardedDatabase()
        {
            sharedDatabase = new SqliteConnection(sharedConnectionString);

            sharedDatabase.Open();

            var command = sharedDatabase.CreateCommand();

            command.CommandText =
            @"
                CREATE TABLE IF NOT EXISTS clientdht(
                    userId TEXT PRIMARY KEY NOT NULL,
                    username TEXT NOT NULL,
                    servername TEXT NOT NULL,
                    dataserver TEXT NOT NULL
                );
            ";
            command.ExecuteNonQuery();

            command.CommandText =
            @"
                CREATE TABLE IF NOT EXISTS workerdht(
                    workerId TEXT PRIMARY KEY NOT NULL,
                    status INTEGER NOT NULL,
                    serverName TEXT NOT NULL,
                    jobId TEXT NOT NULL
                );
            ";
            command.ExecuteNonQuery();

            command.CommandText =
            @"
                CREATE TABLE IF NOT EXISTS serverdht(
                    name TEXT PRIMARY KEY NOT NULL,
                    pairServer TEXT NOT NULL,
                    mode INTEGER NOT NULL
                );
            ";
            command.ExecuteNonQuery();
        }

        public Client? FindClientByUserName(string username)
        {
            Client? retval = null;

            var command = sharedDatabase.CreateCommand();
            command.CommandText =
            @"
                    SELECT *
                    FROM clientdht
                    WHERE username = $username;
                ";
            command.Parameters.AddWithValue("$username", username);

            using (var reader = command.ExecuteReader())
            {
                if(reader.Read())
                {
                    retval = new Client(reader.GetGuid(0), reader.GetString(1), reader.GetString(2), reader.GetString(3));
                }
            }

            return retval;
        }

        public Dictionary<Guid, Client> GetClientDHT()
        {
            Dictionary<Guid, Client> retval = new();

            var command = sharedDatabase.CreateCommand();
            command.CommandText =
            @"
                    SELECT *
                    FROM clientdht
                ";
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    retval.Add(reader.GetGuid(0), new Client(reader.GetGuid(0), reader.GetString(1), reader.GetString(2), reader.GetString(3)));
                }
            }

            return retval;
        }

        public void UpdateClientDHT(Client client)
        {
            var command = sharedDatabase.CreateCommand();
            command.CommandText =
            @"
                    UPDATE clientdht
                    SET servername = $servername, username = $username, dataserver = $dataserver
                    WHERE userId = $clientId
                ";

            command.Parameters.AddWithValue("$clientId", client.ClientId);
            command.Parameters.AddWithValue("$username", client.Username);
            command.Parameters.AddWithValue("$servername", client.ServerName);
            command.Parameters.AddWithValue("$dataserver", client.DataServer);

            command.ExecuteNonQuery();
        }

        public void AddClientToClientDHT(Client client)
        {
            var command = sharedDatabase.CreateCommand();
            command.CommandText =
            @"
                    INSERT INTO clientdht (userId, username, servername, dataserver)
                    VALUES ($clientId, $username, $servername, $dataserver);
                ";

            command.Parameters.AddWithValue("$clientId", client.ClientId);
            command.Parameters.AddWithValue("$username", client.Username);
            command.Parameters.AddWithValue("$servername", client.ServerName);
            command.Parameters.AddWithValue("$dataserver", client.DataServer);

            command.ExecuteNonQuery();
        }

        public void DeleteClientFromClientDHT(Guid id)
        {
            var command = sharedDatabase.CreateCommand();
            command.CommandText =
            @"
                    DELETE from clientdht
                    WHERE userId = $clientId;
                ";

            command.Parameters.AddWithValue("$clientId", id);
            command.ExecuteNonQuery();
        }

        public Dictionary<Guid, Worker> GetWorkerDHT()
        {
            Dictionary<Guid, Worker> retval = new();

            var command = sharedDatabase.CreateCommand();
            command.CommandText =
            @"
                    SELECT *
                    FROM workerdht
                ";
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    retval.Add(reader.GetGuid(0), new Worker(reader.GetGuid(0), (WorkerStatus)reader.GetInt32(1), reader.GetString(2), Guid.Parse(reader.GetString(3))));
                }
            }

            return retval;
        }

        public void UpdateWorkerDHT(Worker worker)
        {
            var command = sharedDatabase.CreateCommand();
            command.CommandText =
            @"
                    UPDATE workerdht
                    SET status = $status, serverName = $serverName, jobId = $jobId
                    WHERE workerId = $workerId
                ";

            command.Parameters.AddWithValue("$workerId", worker.WorkerId);
            command.Parameters.AddWithValue("$status", (int)worker.Status);
            command.Parameters.AddWithValue("$serverName", worker.ServerName);
            command.Parameters.AddWithValue("$jobId", worker.JobId);

            command.ExecuteNonQuery();
        }

        public void AddWorkerToWorkerDHT(Worker worker)
        {
            var command = sharedDatabase.CreateCommand();
            command.CommandText =
            @"
                    INSERT INTO workerdht (workerId, status, serverName, jobId)
                    VALUES ($workerId, $status, $serverName, $jobId);
                ";

            command.Parameters.AddWithValue("$workerId", worker.WorkerId);
            command.Parameters.AddWithValue("$status", (int)worker.Status);
            command.Parameters.AddWithValue("$serverName", worker.ServerName);
            command.Parameters.AddWithValue("$jobId", worker.JobId);

            command.ExecuteNonQuery();
        }

        public Dictionary<string, Server> GetPrimaryServers()
        {
            Dictionary<string, Server> retval = new();

            var command = sharedDatabase.CreateCommand();
            command.CommandText =
            @"
                    SELECT *
                    FROM serverdht
                ";
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    retval.Add(reader.GetString(0), new Server(reader.GetString(0), reader.GetString(1), (ServerMode)reader.GetInt32(1)));
                }
            }

            return retval;
        }

        public Server? GetServerInfo(string serverName)
        {
            Server? server = null;
            var command = sharedDatabase.CreateCommand();
            command.CommandText =
            @"
                    SELECT *
                    FROM serverdht
                    WHERE name = $name
                ";
            command.Parameters.AddWithValue("$name", serverName);
            using (var reader = command.ExecuteReader())
            {
                if(reader.Read())
                {
                    server = new Server(reader.GetString(0), reader.GetString(1), (ServerMode)reader.GetInt32(2));
                }
            }
            return server;
        }

        public void AddServerToServerDHT(Server server)
        {
            var command = sharedDatabase.CreateCommand();
            command.CommandText =
            @"
                    INSERT INTO serverdht (name, pairServer, mode)
                    VALUES ($name, $pairServer, $mode);
                ";

            command.Parameters.AddWithValue("$name", server.Name);
            command.Parameters.AddWithValue("$pairServer", server.PairServer);
            command.Parameters.AddWithValue("$mode", server.Mode);

            command.ExecuteNonQuery();
        }

        public void UpdateServerDHT(Server server)
        {
            var command = sharedDatabase.CreateCommand();
            command.CommandText =
            @"
                    UPDATE serverdht
                    SET name = $name, pairServer = $pairServer, mode = $mode
                    WHERE name = $name
                ";

            command.Parameters.AddWithValue("$name", server.Name);
            command.Parameters.AddWithValue("$pairServer", server.PairServer);
            command.Parameters.AddWithValue("$mode", server.Mode);

            command.ExecuteNonQuery();
        }

        public void AddTask(ServiceTask task)
        {
            var command = localDatabase.CreateCommand();
            command.CommandText =
            @"
                    INSERT INTO serviceTask 
                    (tasksId, name, clientId, status, sourcePath, backupPath, resultPath)
                    VALUES 
                    ($tasksId, $name, $clientId, $status, $sourcePath, $backupPath, $resultPath);
                ";
            command.Parameters.AddWithValue("$tasksId", task.Id);
            command.Parameters.AddWithValue("$name", task.Name);
            command.Parameters.AddWithValue("$clientId", task.ClientId);
            command.Parameters.AddWithValue("$status", task.Status);
            command.Parameters.AddWithValue("$sourcePath", task.SourcePath);
            command.Parameters.AddWithValue("$backupPath", task.BackupPath);
            command.Parameters.AddWithValue("$resultPath", task.ResultPath);

            command.ExecuteNonQuery();
        }

        public void UpdateTask(ServiceTask task) 
        {
            var command = localDatabase.CreateCommand();
            command.CommandText =
            @"
                    UPDATE serviceTask
                    SET 
                    name = $name, 
                    clientId = $clientId, 
                    status = $status,
                    sourcePath = $sourcePath,
                    backupPath = $backupPath,
                    resultPath = $resultPath
                    WHERE tasksId = $tasksId
                ";

            command.Parameters.AddWithValue("$name", task.Name);
            command.Parameters.AddWithValue("$clientId", task.ClientId);
            command.Parameters.AddWithValue("$status", task.Status);
            command.Parameters.AddWithValue("$sourcePath", task.SourcePath);
            command.Parameters.AddWithValue("$backupPath", task.BackupPath);
            command.Parameters.AddWithValue("$resultPath", task.ResultPath);
            command.Parameters.AddWithValue("$tasksId", task.Id);

            command.ExecuteNonQuery();
        }

        public ServiceTask? GetTaskFromId(Guid taskId)
        {
            ServiceTask? retVal = null;
            var command = localDatabase.CreateCommand();

            command.CommandText =
            @"
                    SELECT *
                    FROM serviceTask
                    WHERE tasksId = $tasksId
                ";
            command.Parameters.AddWithValue("$tasksId", taskId);
            using (var reader = command.ExecuteReader())
            {
                if (reader.Read())
                {
                    retVal = new ServiceTask(
                        reader.GetGuid(0), 
                        reader.GetGuid(1), 
                        reader.GetString(2),
                        (ServiceTaskStatus)reader.GetInt32(3),
                        reader.GetString(4),
                        reader.GetString(5),
                        reader.GetString(6));
                }
            }

            return retVal;
        }

        public List<ServiceTask> GetTaskForClient(Guid clientId)
        {
            List<ServiceTask> retVal = new List<ServiceTask>();
            var command = localDatabase.CreateCommand();

            command.CommandText =
            @"
                    SELECT *
                    FROM serviceTask
                    WHERE clientId = $clientId
                ";
            command.Parameters.AddWithValue("$clientId", clientId);
            using (var reader = command.ExecuteReader())
            {
                while(reader.Read())
                {
                    retVal.Add(new ServiceTask(
                        reader.GetGuid(0),
                        reader.GetGuid(1),
                        reader.GetString(2),
                        (ServiceTaskStatus)reader.GetInt32(3),
                        reader.GetString(4),
                        reader.GetString(5),
                        reader.GetString(6)));
                }                    
            }

            return retVal;
        }

        public List<ServiceTask> GetTasksFromStatus(ServiceTaskStatus status)
        {
            List<ServiceTask> retval = new();

            var command = localDatabase.CreateCommand();
            command.CommandText =
            @"
                    SELECT *
                    FROM serviceTask
                    WHERE status = $status
                ";

            command.Parameters.AddWithValue("$status", (int)status);

            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                   retval.Add(new ServiceTask(
                   reader.GetGuid(0),
                   reader.GetGuid(1),
                   reader.GetString(2),
                   (ServiceTaskStatus)reader.GetInt32(3),
                   reader.GetString(4),
                   reader.GetString(5),
                   reader.GetString(6)));
                }
            }
            return retval;
        }

    }
}
