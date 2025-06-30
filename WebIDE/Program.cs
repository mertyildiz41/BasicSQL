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
    var sql = System.Text.Json.JsonDocument.Parse(body).RootElement.GetProperty("sql").GetString();
    if (string.IsNullOrWhiteSpace(sql)) return Results.BadRequest("No SQL provided");

    // Connect to BasicSQL TCP server
    try
    {
        using var client = new TcpClient("localhost", 4162);
        using var stream = client.GetStream();
        using var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true };
        using var tcpReader = new StreamReader(stream, Encoding.UTF8);
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

app.Run("http://localhost:5173");
