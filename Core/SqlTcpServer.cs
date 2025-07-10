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
        private readonly AuthenticationManager _authManager;
        private TcpListener _listener;
        private bool _isRunning;

        public SqlTcpServer(int port, Func<string, string> queryHandler, AuthenticationManager authManager)
        {
            _port = port;
            _queryHandler = queryHandler;
            _authManager = authManager;
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
                catch (SocketException) when (!_isRunning) {
                    // Listener was stopped, ignore the exception
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error accepting client: {ex.Message}");
                }
            }
        }

        private async Task HandleClientAsync(TcpClient client)
        {
            using (client)
            using (var stream = client.GetStream())
            using (var reader = new StreamReader(stream, Encoding.UTF8))
            using (var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true })
            {
                try
                {
                    var authLine = await reader.ReadLineAsync();
                    if (authLine == null)
                    {
                        return; // Client disconnected
                    }
                    
                    var parts = authLine.Split(' ');
                    if (parts.Length != 3 || parts[0] != "AUTH")
                    {
                        await writer.WriteLineAsync("AUTH_FAIL Invalid command");
                        return;
                    }

                    var username = parts[1];
                    var password = parts[2];

                    if (_authManager.Authenticate(username, password))
                    {
                        await writer.WriteLineAsync("AUTH_SUCCESS");
                        var userRole = _authManager.GetUserRole(username);

                        // Handle queries after successful authentication
                        string line;
                        while ((line = await reader.ReadLineAsync()) != null)
                        {
                            if (!HasPermission(userRole, line))
                            {
                                await writer.WriteLineAsync("ERROR: Permission denied.");
                                continue;
                            }
                            string result = _queryHandler(line);
                            await writer.WriteLineAsync(result);
                        }
                    }
                    else
                    {
                        await writer.WriteLineAsync("AUTH_FAIL Invalid credentials");
                        return;
                    }
                }
                catch (IOException ex) when (ex.InnerException is SocketException)
                {
                    // Client disconnected
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error handling client: {ex.Message}");
                }
            }
        }

        private bool HasPermission(string role, string sql)
        {
            if (string.IsNullOrWhiteSpace(sql)) return true;

            var command = sql.Trim().Split(' ')[0].ToUpper();

            if (role == "admin")
            {
                return true; // Admin can do anything
            }

            if (role == "user")
            {
                switch (command)
                {
                    case "SELECT":
                    case "SHOW":
                    case "DESCRIBE":
                        return true;
                    default:
                        return false; // Deny all other commands
                }
            }

            return false; // Default deny
        }
    }
}
