using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Tutorial
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                Console.WriteLine($"{nameof(BasicReplier)} Tutorial");
                var opts = ParseArgs(args);

                var host = GetOrPromptOption(opts, "h", "Solace Host");
                var vpnname = GetOrPromptOption(opts, "v", "VPN Name");
                var username = GetOrPromptOption(opts, "u", "Username");
                var password = GetOrPromptOption(opts, "p", "Password", true);
                
                var basicReplier = new BasicReplier();
                basicReplier.Run(host, vpnname, username, password);
            }
            catch(ArgumentException)
            {
                Console.WriteLine($"Usage:\n{nameof(BasicReplier)} [-h host] [-v vpn] [-u username] [-p password]");
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Exception thrown: {ex.Message}");
            }

        }

        private static IDictionary<string,string> ParseArgs(string[] args)
        {
            var argString = $" {string.Join(" ", args)}";
            var pattern = @" -(?<name>[hvup]) (?<value>[^\-\s][^\s]+)";

            if(args.Length > 0 && !Regex.Match(argString, $"^({pattern})*$").Success)
            {
                throw new ArgumentException();
            }

            return Regex.Matches(argString, pattern)
                .Cast<Match>()
                .ToDictionary(m => m.Groups["name"].Value, m => m.Groups["value"].Value);
        }

        private static string GetOrPromptOption(IDictionary<string, string> opts, string argName, string prompt, bool mask = false)
        {
            if (!opts.TryGetValue(argName, out var value))
            {
                Console.Write($"{prompt}: ");
                value = ReadLine(mask);
            }
            
            if(string.IsNullOrEmpty(value))
            {
                throw new InvalidOperationException($"{prompt} must be non-empty.");
            }

            return value;
        }

        private static string ReadLine(bool mask)
        {
            if(!mask)
            {
                return Console.ReadLine();
            }

            var result = string.Empty;
            ConsoleKeyInfo c;
            while ((c = Console.ReadKey(true)).Key != ConsoleKey.Enter)
            {
                if (c.Key == ConsoleKey.Backspace && result.Length > 0)
                {
                    result = result.Remove(result.Length - 1, 1);
                    Console.Write("\b \b");
                }
                else
                {
                    result += c.KeyChar;
                    Console.Write("*");
                }
            }
            Console.WriteLine();
            return result;
        }
    }
}