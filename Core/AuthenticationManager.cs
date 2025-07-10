using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using BasicSQL.Models;

namespace BasicSQL.Core
{
    public class AuthenticationManager
    {
        private readonly BinarySqlEngine _engine;
        private const string UserTableName = "_users";

        public AuthenticationManager(BinarySqlEngine engine)
        {
            _engine = engine;
            InitializeUserTable();
        }

        public BinarySqlEngine GetEngine()
        {
            return _engine;
        }

        private void InitializeUserTable()
        {
            // Create the user table only if it doesn't exist.
            var createUserTableSql = $"CREATE TABLE IF NOT EXISTS {UserTableName} (username TEXT PRIMARY KEY, password_hash TEXT NOT NULL, role TEXT NOT NULL)";
            var creationResult = _engine.Execute(createUserTableSql);
            if (!creationResult.Success)
            {
                throw new Exception($"Could not create user table: {creationResult.Message}");
            }

            // Check if any users exist and create a default admin if none are found
            var checkUserSql = $"SELECT COUNT FROM {UserTableName}";
            var userCountResult = _engine.Execute(checkUserSql);

            if (userCountResult.Success && userCountResult.Rows.Any() && Convert.ToInt64(userCountResult.Rows.First()["COUNT"]) == 0)
            {
                // No users exist, create a default admin
                CreateUser("admin", "admin", "admin");
                Console.WriteLine("No users found. Created default admin user with username 'admin' and password 'admin'.");
            }
        }

        public bool CreateUser(string username, string password, string role = "user")
        {
            if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
            {
                return false;
            }

            var passwordHash = HashPassword(password);
            var sql = $"INSERT INTO {UserTableName} (username, password_hash, role) VALUES ('{username}', '{passwordHash}', '{role}')";
            var result = _engine.Execute(sql);
            return result.Success;
        }

        public bool Authenticate(string username, string password)
        {
            var sql = $"SELECT password_hash FROM {UserTableName} WHERE username = '{username}'";
            var result = _engine.Execute(sql);

            Console.WriteLine($"Authentication query executed: {result}");

            if (result.Success && result.Rows.Any())
            {
                var storedHash = result.Rows.First()["password_hash"].ToString();
                var providedHash = HashPassword(password);
                Console.WriteLine($"Stored hash: {storedHash}, Provided hash: {providedHash}");
                return storedHash == providedHash;
            }

            return false;
        }

        public string GetUserRole(string username)
        {
            var sql = $"SELECT role FROM {UserTableName} WHERE username = '{username}'";
            var result = _engine.Execute(sql);

            if (result.Success && result.Rows.Any())
            {
                return result.Rows.First()["role"].ToString();
            }

            return null;
        }

        private string HashPassword(string password)
        {
            using (var sha256 = SHA256.Create())
            {
                var bytes = Encoding.UTF8.GetBytes(password);
                var hash = sha256.ComputeHash(bytes);
                return System.BitConverter.ToString(hash).Replace("-", "").ToLower();
            }
        }
    }
}
