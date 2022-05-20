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
using SolaceSystems.Solclient.Messaging;

/// <summary>
/// Solace Systems Messaging API tutorial: TopicPublisher
/// </summary>

namespace Tutorial
{
    /// <summary>
    /// Demonstrates how to use Solace Systems Messaging API for publishing a message
    /// </summary>
    class TopicPublisher
    {
        const int DefaultConnectRetries = 3;

        /// <summary>
        /// Runs the subscription demo on the given host and VPN
        /// </summary>
        public void Run(string host, string vpnname, string username, string password)
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
                };
                
                // Create context and session instances
                using (var context = ContextFactory.Instance.CreateContext(contextProperties, null))
                using (var session = context.CreateSession(sessionProperties, null, null))
                {
                    // Connect to the Solace messaging router
                    Console.WriteLine($"Connecting as {username}@{vpnname} on {host}...");
                    var connectResult = session.Connect();

                    if (connectResult == ReturnCode.SOLCLIENT_OK)
                    {
                        Console.WriteLine("Session successfully connected.");

                        // Create a topic and subscribe to it
                        using (var message = ContextFactory.Instance.CreateMessage())
                        using (var topic = ContextFactory.Instance.CreateTopic("tutorial/topic"))
                        {
                            message.Destination = topic;
                            message.BinaryAttachment = Encoding.UTF8.GetBytes("Sample Message");

                            // Publish the message to the topic on the Solace messaging router
                            Console.WriteLine("Publishing message...");
                            var sendResult = session.Send(message);
                            if (sendResult == ReturnCode.SOLCLIENT_OK)
                            {
                                Console.WriteLine("Done.");
                            }
                            else
                            {
                                Console.WriteLine($"Publishing failed, return code: {sendResult}");
                            }
                        }
                    }
                    else
                    {
                        Console.WriteLine($"Error connecting, return code: {connectResult}");
                    }
                }
            }
            finally
            {
                // Dispose Solace Systems Messaging API
                ContextFactory.Instance.Cleanup();
            }
            Console.WriteLine("Finished.");
        }
    }
}
