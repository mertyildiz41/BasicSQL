using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace BasicSQL.Core
{
    public class SqlTcpServer
    {
        private readonly int _port;
        private readonly Func<string, string> _queryHandler;
        private TcpListener _listener;
        private bool _isRunning;

        public SqlTcpServer(int port, Func<string, string> queryHandler)
        {
            _port = port;
            _queryHandler = queryHandler;
        }

        public void Start()
        {
            _listener = new TcpListener(IPAddress.Any, _port);
            _listener.Start();
            _isRunning = true;
            Task.Run(AcceptClientsAsync);
        }

        public void Stop()
        {
            _isRunning = false;
            _listener?.Stop();
        }

        private async Task AcceptClientsAsync()
        {
            while (_isRunning)
            {
                try
                {
                    var client = await _listener.AcceptTcpClientAsync();
                    _ = Task.Run(() => HandleClientAsync(client));
                }
                catch { }
            }
        }

        private async Task HandleClientAsync(TcpClient client)
        {
            using (client)
            using (var stream = client.GetStream())
            using (var reader = new StreamReader(stream, Encoding.UTF8))
            using (var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true })
            {
                string line;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    string result = _queryHandler(line);
                    await writer.WriteLineAsync(result);
                }
            }
        }
    }
}
