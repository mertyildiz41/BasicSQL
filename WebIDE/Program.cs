using System.Net.Sockets;
using System.Text;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

// Use static files and default file (index.html)
app.UseDefaultFiles();
app.UseStaticFiles();

// API endpoint for running SQL queries
app.MapPost("/api/query", async (HttpContext ctx) =>
{
    using var reader = new StreamReader(ctx.Request.Body);
    var body = await reader.ReadToEndAsync();
    var json = System.Text.Json.JsonDocument.Parse(body).RootElement;
    var sql = json.GetProperty("sql").GetString();
    var username = json.GetProperty("username").GetString();
    var password = json.GetProperty("password").GetString(); // No hashing here

    if (string.IsNullOrWhiteSpace(sql)) return Results.BadRequest("No SQL provided");
    if (string.IsNullOrWhiteSpace(username)) return Results.BadRequest("No username provided");
    if (string.IsNullOrWhiteSpace(password)) return Results.BadRequest("No password provided");

    // Connect to BasicSQL TCP server
    try
    {
        using var client = new TcpClient("localhost", 4162);
        using var stream = client.GetStream();
        using var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };
        using var tcpReader = new StreamReader(stream, Encoding.UTF8);

        // Wait for AUTH_REQUIRED
        var initialResponse = await tcpReader.ReadLineAsync();
        if (initialResponse != "AUTH_REQUIRED")
        {
            return Results.Text($"ERROR: Unexpected server response: {initialResponse}");
        }

        // Authenticate
        writer.WriteLine($"AUTH {username} {password}");
        var authResponse = await tcpReader.ReadLineAsync();
        if (authResponse != "AUTH_SUCCESS")
        {
            return Results.Text($"ERROR: {authResponse}");
        }

        writer.WriteLine(sql);
        string response = string.Empty;
        while (true)
        {
            var line = await tcpReader.ReadLineAsync();
            if (line == null || line == "") break; // End of response
            if (response.Length > 0)
            {
                response += "\n"; // Add newline between responses
            }
            response += line;
        }
        return Results.Text(response);
    }
    catch (Exception ex)
    {
        return Results.Text($"ERROR: {ex.Message}");
    }
});

// API endpoint for getting table list
app.MapPost("/api/tables", async (HttpContext ctx) =>
{
    using var reader = new StreamReader(ctx.Request.Body);
    var body = await reader.ReadToEndAsync();
    var json = System.Text.Json.JsonDocument.Parse(body).RootElement;
    var username = json.GetProperty("username").GetString();
    var password = json.GetProperty("password").GetString(); // No hashing here

    if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
    {
        return Results.Unauthorized();
    }

    try
    {
        using var client = new TcpClient("localhost", 4162);
        using var stream = client.GetStream();
        using var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };
        using var tcpReader = new StreamReader(stream, Encoding.UTF8);

        // Wait for AUTH_REQUIRED
        var initialResponse = await tcpReader.ReadLineAsync();
        if (initialResponse != "AUTH_REQUIRED")
        {
            return Results.Problem($"Unexpected server response: {initialResponse}");
        }

        writer.WriteLine($"AUTH {username} {password}");
        var authResponse = await tcpReader.ReadLineAsync();
        Console.WriteLine($"Auth response: {authResponse}");
        if (authResponse != "AUTH_SUCCESS")
        {
            return Results.Unauthorized();
        }

        writer.WriteLine("SHOW TABLES");
        var tablesResponse = await tcpReader.ReadLineAsync();
        if (tablesResponse != null && tablesResponse.StartsWith("Tables: "))
        {
            var tables = tablesResponse.Substring("Tables: ".Length).Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries).Select(t => t.Trim()).ToList();
            return Results.Ok(tables);
        }
        return Results.Ok(new List<string>());
    }
    catch (Exception ex)
    {
        return Results.Problem(ex.Message);
    }
});

// API endpoint for getting database list
app.MapPost("/api/databases", async (HttpContext ctx) =>
{
    using var reader = new StreamReader(ctx.Request.Body);
    var body = await reader.ReadToEndAsync();
    var json = System.Text.Json.JsonDocument.Parse(body).RootElement;
    var username = json.GetProperty("username").GetString();
    var password = json.GetProperty("password").GetString();

    if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
    {
        return Results.Unauthorized();
    }

    try
    {
        using var client = new TcpClient("localhost", 4162);
        using var stream = client.GetStream();
        using var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };
        using var tcpReader = new StreamReader(stream, Encoding.UTF8);

        // Wait for AUTH_REQUIRED
        var initialResponse = await tcpReader.ReadLineAsync();
        if (initialResponse != "AUTH_REQUIRED")
        {
            return Results.Problem($"Unexpected server response: {initialResponse}");
        }

        // Authenticate
        writer.WriteLine($"AUTH {username} {password}");
        var authResponse = await tcpReader.ReadLineAsync();
        if (authResponse != "AUTH_SUCCESS")
        {
            return Results.Unauthorized();
        }

        writer.WriteLine("SHOW DATABASES");
        var databasesResponse = await tcpReader.ReadLineAsync();
        if (databasesResponse != null && databasesResponse.StartsWith("Databases: "))
        {
            var databases = databasesResponse.Substring("Databases: ".Length).Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(d => d.Trim()).Where(d => !string.Equals(d, "default", StringComparison.OrdinalIgnoreCase)).ToList();
            return Results.Ok(databases);
        }
        return Results.Ok(new List<string>());
    }
    catch (Exception ex)
    {
        return Results.Problem(ex.Message);
    }
});

// API endpoint for switching database
app.MapPost("/api/use-database", async (HttpContext ctx) =>
{
    using var reader = new StreamReader(ctx.Request.Body);
    var body = await reader.ReadToEndAsync();
    var json = System.Text.Json.JsonDocument.Parse(body).RootElement;
    var username = json.GetProperty("username").GetString();
    var password = json.GetProperty("password").GetString();
    var database = json.GetProperty("database").GetString();

    if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password) || string.IsNullOrWhiteSpace(database))
    {
        return Results.BadRequest("Missing required parameters");
    }

    try
    {
        using var client = new TcpClient("localhost", 4162);
        using var stream = client.GetStream();
        using var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };
        using var tcpReader = new StreamReader(stream, Encoding.UTF8);

        // Wait for AUTH_REQUIRED
        var initialResponse = await tcpReader.ReadLineAsync();
        if (initialResponse != "AUTH_REQUIRED")
        {
            return Results.Text($"ERROR: Unexpected server response: {initialResponse}");
        }

        // Authenticate
        writer.WriteLine($"AUTH {username} {password}");
        var authResponse = await tcpReader.ReadLineAsync();
        if (authResponse != "AUTH_SUCCESS")
        {
            return Results.Text($"ERROR: {authResponse}");
        }

        writer.WriteLine($"USE {database}");
        var useResponse = await tcpReader.ReadLineAsync();
        return Results.Text(useResponse ?? "No response");
    }
    catch (Exception ex)
    {
        return Results.Text($"ERROR: {ex.Message}");
    }
});

app.Run("http://localhost:5173");
