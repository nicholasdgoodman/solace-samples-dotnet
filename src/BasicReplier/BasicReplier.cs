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

using SolaceSystems.Solclient.Messaging;
using Tutorial.Common;

/// <summary>
/// Solace Systems Messaging API tutorial: BasicReplier
/// </summary>

namespace Tutorial
{
    /// <summary>
    /// Demonstrates how to use Solace Systems Messaging API for receiving a request and sending a reply 
    /// </summary>
    class BasicReplier
    {
        static readonly int DefaultConnectRetries = 3;
        static readonly AutoResetEvent MessageReceivedEvent = new AutoResetEvent(false);

        static void Main(string[] args)
        {
            if (CommandLine.TryLoadConfig(args, out var config))
            {
                Run(config.Host, config.Vpn, config.UserName, config.Password);
            }
        }

        static void Run(string host, string vpnname, string username, string password)
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
                using (var session = context.CreateSession(sessionProperties, HandleRequestMessage, null))
                {
                    // Connect to the Solace messaging router
                    Console.WriteLine($"Connecting as {username}@{vpnname} on {host}...");
                    var connectResult = session.Connect();

                    if (connectResult == ReturnCode.SOLCLIENT_OK)
                    {
                        Console.WriteLine("Session successfully connected.");

                        // Create a topic and subscribe to it
                        using (var topic = ContextFactory.Instance.CreateTopic("tutorial/requests"))
                        {
                            session.Subscribe(topic, true);

                            Console.WriteLine("Waiting for a request to come in...");
                            MessageReceivedEvent.WaitOne();
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

        /// <summary>
        /// This event handler is invoked by Solace Systems Messaging API when a message arrives
        /// </summary>
        /// <param name="source"></param>
        /// <param name="args"></param>
        static void HandleRequestMessage(object source, MessageEventArgs args)
        { 
            Console.WriteLine("Received request.");
            var session = source as ISession;

            // Received a request message
            using (IMessage requestMessage = args.Message)
            {
                // Expecting the request content as a binary attachment
                Console.WriteLine($"Request content: {Encoding.UTF8.GetString(requestMessage.BinaryAttachment)}");
                // Create reply message
                using (IMessage replyMessage = ContextFactory.Instance.CreateMessage())
                {
                    // Set the reply content as a binary attachment 
                    replyMessage.BinaryAttachment = Encoding.UTF8.GetBytes("Sample Reply");
                    Console.WriteLine("Sending reply...");
                    var sendReplyResult = session.SendReply(requestMessage, replyMessage);
                    if (sendReplyResult == ReturnCode.SOLCLIENT_OK)
                    {
                        Console.WriteLine("Sent.");
                    }
                    else
                    {
                        Console.WriteLine($"Reply failed, return code: {sendReplyResult}");
                    }
                    // finish the program
                    MessageReceivedEvent.Set();
                }
            }
        }
    }
}
