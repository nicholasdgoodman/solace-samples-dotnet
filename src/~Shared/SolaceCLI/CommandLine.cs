using System;
using System.Collections.Generic;
using System.CommandLine;
using System.IO;
using System.Text;
using System.Text.Json;

namespace Tutorial.Common
{
    public static class CommandLine
    {
        private static readonly string ConfigPath = Path.Combine(Path.GetDirectoryName(typeof(SolaceConfig).Assembly.Location), $"{nameof(SolaceConfig)}.json");
        public static bool TryLoadConfig(string[] args, out SolaceConfig config)
        {
            var rootCommand = new RootCommand();

            var initOption = new Option<bool>("init", "Re-initialize the saved Solace configuration.");
            var hostOption = new Option<string>("-h", "Solace Host");
            var vpnOption = new Option<string>("-v", "VPN Name");
            var userNameOption = new Option<string>("-u", "Username");
            var passwordOption = new Option<string>("-p", "Password");

            rootCommand.Add(initOption);
            rootCommand.Add(hostOption);
            rootCommand.Add(vpnOption);
            rootCommand.Add(userNameOption);
            rootCommand.Add(passwordOption);

            SolaceConfig loadedConfig = default;

            rootCommand.SetHandler((init, host, vpn, userName, password) =>
            {
                if (!TryLoadConfig(out loadedConfig) || init)
                {
                    loadedConfig = new SolaceConfig()
                    {
                        Host = host ?? PromptOption(hostOption.Description),
                        Vpn = vpn ?? PromptOption(vpnOption.Description),
                        UserName = userName ?? PromptOption(userNameOption.Description),
                        Password = password ?? PromptOption(passwordOption.Description, true)
                    };

                    SaveConfig(loadedConfig);
                }
            }, initOption, hostOption, vpnOption, userNameOption, passwordOption);

            rootCommand.Invoke(args);

            config = loadedConfig;
            return (config != null);
        }
        
        public static void WriteLine(string value)
        {
            var timestamp = DateTime.Now.ToString("HH:mm:ss.fff");
            var managedThreadId = string.Format("{0:X4}", System.Threading.Thread.CurrentThread.ManagedThreadId);
            Console.WriteLine($"{timestamp} [{managedThreadId}]: {value}");
        }
        
        private static bool TryLoadConfig(out SolaceConfig config)
        {
            if (File.Exists(ConfigPath))
            {
                var json = File.ReadAllText(ConfigPath);
                config = JsonSerializer.Deserialize<SolaceConfig>(json, options: SolaceConfig.SerializerOptions);
                return true;
            }
            else
            {
                config = default;
                return false;
            }
        }

        private static void SaveConfig(SolaceConfig config)
        {
            File.WriteAllText(ConfigPath, JsonSerializer.Serialize(config, options: SolaceConfig.SerializerOptions));
        }

        private static string PromptOption(string prompt, bool mask = false)
        {
            Console.Write($"{prompt}: ");
            return ReadLine(mask);
        }

        private static string ReadLine(bool mask)
        {
            if (!mask)
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
