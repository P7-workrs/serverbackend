using FluentFTP;
using System.Text;


namespace WorkrsBackend.FTP
{
    public class FTPHandler
    {
        FtpClient client;

        public string HostName { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }

        public void Init(string hostName, string username, string password)
        {
            HostName = hostName;
            Username = username;
            Password = password;
            client = new FtpClient(hostName, username, password);
        }

        public void CreateDirectory(string path)
        {
            client.CreateDirectory(path);
        }

        public void UploadFile(string filename, string content)
        {
            byte[] byteArray = Encoding.UTF8.GetBytes(content);
            Stream s = new MemoryStream(byteArray);
            Connect();
            client.UploadStream(s, filename, FtpRemoteExists.Overwrite, true);
            Disconnect();
        }

        bool Connect()
        {
            client.Connect();
            if (client.IsConnected)
                return true;
            return false;
        }

        void Disconnect()
        {
            if(client.IsConnected)
                client.Disconnect();
        }

    }
}