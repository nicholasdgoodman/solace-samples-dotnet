#region Copyright & License
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#endregion

using System;
using System.Text;
using System.Threading;
using System.Collections.Generic;

using SolaceSystems.Solclient.Messaging;
using Tutorial.Common;
using System.Threading.Tasks;

/// <summary>
/// Solace Systems Messaging API tutorial: ConfirmedPublish
/// </summary>

namespace Tutorial
{
    /// <summary>
    /// Demonstrates how to use Solace Systems Messaging API for sending a confirmed guaranteed delivery message
    /// </summary>
    class ConfirmedPublish
    {
        static readonly int DefaultConnectRetries = 3;
        static readonly CountdownEvent CountdownEvent = new CountdownEvent(TotalMessages);
        static int TotalMessages;
        
        static void Main(string[] args)
        {
            if (CommandLine.TryLoadConfig(args, out var config))
            {
                ThreadPool.GetMinThreads(out var workerThreads, out var completionPortThreads);
                ThreadPool.SetMaxThreads(workerThreads, completionPortThreads);

                TotalMessages = workerThreads * 2;

                CommandLine.WriteLine($"Running Sample (max worker threads: {workerThreads})");
                Task.Run(() => Run(config.Host, config.Vpn, config.UserName, config.Password)).Wait();
                CommandLine.WriteLine("Sample Complete");
            }
        }

        static async Task Run(string host, string vpnname, string username, string password)
        {
            try
            {
                // Initialize Solace Systems Messaging API with logging to console at Warning level
                var props = new ContextFactoryProperties() { SolClientLogLevel = SolLogLevel.Warning };
                props.LogToConsoleError();
                ContextFactory.Instance.Init(props);
                
                // Define context and session properties
                var contextProperties = new ContextProperties();
                var sessionProperties = new SessionProperties()
                {
                    Host = host,
                    VPNName = vpnname,
                    UserName = username,
                    Password = password,
                    ConnectRetries = DefaultConnectRetries,
                    BlockWhileConnecting = false,
                    ConnectBlocking = false,
                    SendBlocking = false,
                    SubscribeBlocking = false,
                };

                // Create context and session instances
                using (var context = ContextFactory.Instance.CreateContext(contextProperties, null))
                using (var client = context.CreateClient(sessionProperties))
                { 
                    // Connect to the Solace messaging router
                    CommandLine.WriteLine($"Connecting as {username}@{vpnname} on {host}...");
                    var connectResult = await client.ConnectAsync();

                    if (connectResult == ReturnCode.SOLCLIENT_OK)
                    {
                        CommandLine.WriteLine("Session successfully connected.");

                        var queueName = "Q-tutorial";

                        var sendResults = new List<Task>();

                        // Create the queue
                        using (var queue = ContextFactory.Instance.CreateQueue(queueName))
                        {
                            // Set queue permissions to "consume" and access-type to "exclusive"
                            var endpointProps = new EndpointProperties()
                            {
                                Permission = EndpointProperties.EndpointPermission.Consume,
                                AccessType = EndpointProperties.EndpointAccessType.Exclusive
                            };

                            CommandLine.WriteLine(value: $"Attempting to provision the queue '{queueName}'...");
                            await client.ProvisionAsync(queue, endpointProps,
                                ProvisionFlag.IgnoreErrorIfEndpointAlreadyExists);
                            CommandLine.WriteLine($"Queue '{queueName}' has been created and provisioned.");

                            // Create the message
                            using (var message = ContextFactory.Instance.CreateMessage())
                            {
                                // Message's destination is the queue and the message is persistent
                                message.Destination = queue;
                                message.DeliveryMode = MessageDeliveryMode.Persistent;

                                // Send it to the mapped topic a few times with different content
                                for (var i = 0; i < TotalMessages; i++)
                                {
                                    var messageId = i;
                                    // Create the message content as a binary attachment
                                    message.BinaryAttachment = Encoding.UTF8.GetBytes(
                                        $"Confirmed Publish Tutorial! Message ID: {messageId}");

                                    // Send the message to the queue on the Solace messaging router
                                    CommandLine.WriteLine($"Sending message {messageId} to queue {queueName}...");
                                    var sendTask = client.SendAsync(message);

                                    sendResults.Add(sendTask.ContinueWith(s =>
                                    {
                                        var sendResult = s.Result;
                                        if (sendResult == ReturnCode.SOLCLIENT_OK)
                                        {
                                            CommandLine.WriteLine($"Successfully sent message {messageId}");
                                        }
                                        else
                                        {
                                            CommandLine.WriteLine($"Sending failed, return code: {sendResult}");
                                        }
                                    }));
                                }

                            }
                        }

                        CommandLine.WriteLine($"{TotalMessages} messages sent. Processing replies.");

                        // block the current thread until a confirmation received
                        Task.WaitAll(sendResults.ToArray());
                        
                    }
                    else
                    {
                        CommandLine.WriteLine($"Error connecting, return code: {connectResult}");
                    }
                }
            }
            finally
            {
                // Dispose Solace Systems Messaging API
                CommandLine.WriteLine("Cleaning up...");
                ContextFactory.Instance.Cleanup();
            }
            CommandLine.WriteLine("Finished.");
        }
    }

}
