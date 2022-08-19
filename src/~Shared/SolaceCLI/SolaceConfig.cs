using System;
using System.Collections.Generic;
using System.CommandLine;
using System.Linq;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace Tutorial.Common
{
    public class SolaceConfig
    {
        internal static readonly JsonSerializerOptions SerializerOptions = new JsonSerializerOptions() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

        public string Host { get; set; }
        public string Vpn { get; set; }
        public string UserName { get; set; }

        [JsonIgnore]
        public string Password { get; set; }

        public string PasswordBase64
        {
            get => Convert.ToBase64String(Encoding.UTF8.GetBytes(Password));
            set => Password = Encoding.UTF8.GetString(Convert.FromBase64String(value));
        }
    }
}
