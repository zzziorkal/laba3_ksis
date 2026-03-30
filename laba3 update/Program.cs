using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;

namespace P2PChat
{
    class Program
    {
        static string myName;
        static int myPort;
        static int udpPort = 9999;
        static TcpListener server;
        static Dictionary<string, TcpClient> connections = new Dictionary<string, TcpClient>();
        static UdpClient udpClient;
        static CancellationTokenSource cts = new CancellationTokenSource();
        static object locker = new object();

        static void Main(string[] args)
        {
            Console.Write("Imya: ");
            myName = Console.ReadLine();

            Console.Write("Vash IP : ");
            string myIp = Console.ReadLine();

            Console.Write("TCP port: ");
            myPort = int.Parse(Console.ReadLine());

            // ZAPUSK UDP SLUSHATELYA
            udpClient = new UdpClient();
            udpClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            udpClient.Client.Bind(new IPEndPoint(IPAddress.Parse(myIp), udpPort));

            // ZAPUSK TCP SERVERA
            server = new TcpListener(IPAddress.Parse(myIp), myPort);
            server.Start();

            // FONOVYE ZADAChI
            Task.Run(() => AcceptClients());
            Task.Run(() => ListenForBroadcast());
            Task.Run(() => SendPeriodicBroadcast());

            Console.WriteLine($"\n{myName} zapushen na {myIp}:{myPort}");
            Console.WriteLine($"UDP broadcast na portu {udpPort}");
            Console.WriteLine("Pishi soobsheniya:\n");

            // OSNOVNOY CYKL
            while (true)
            {
                string text = Console.ReadLine();
                if (text == "/quit") break;

                if (!string.IsNullOrWhiteSpace(text))
                {
                    SendToAll(text);
                    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Ya: {text}");
                }
            }

            cts.Cancel();
            server.Stop();
            udpClient.Close();
            foreach (var c in connections.Values) c.Close();
        }

        static void SendToAll(string text)
        {
            string msg = $"{myName}|{text}|{DateTime.Now:HH:mm:ss}";
            byte[] data = Encoding.UTF8.GetBytes(msg);
            byte[] len = BitConverter.GetBytes(data.Length);

            List<TcpClient> clientsCopy;
            lock (locker) { clientsCopy = connections.Values.ToList(); }

            foreach (var client in clientsCopy)
            {
                try
                {
                    if (client.Connected)
                    {
                        var stream = client.GetStream();
                        stream.Write(len, 0, 4);
                        stream.Write(data, 0, data.Length);
                        stream.Flush();
                    }
                }
                catch { }
            }
        }

        static async Task AcceptClients()
        {
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    var client = await server.AcceptTcpClientAsync();
                    string ip = ((IPEndPoint)client.Client.RemoteEndPoint).Address.ToString();

                    lock (locker)
                    {
                        if (!connections.ContainsKey(ip))
                        {
                            connections[ip] = client;
                            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] * + {ip}");
                        }
                    }

                    Task.Run(() => HandleClient(client, ip));
                }
                catch { break; }
            }
        }

        static async Task HandleClient(TcpClient client, string ip)
        {
            var stream = client.GetStream();

            try
            {
                while (client.Connected)
                {
                    byte[] lenBuf = new byte[4];
                    int read = 0;
                    while (read < 4)
                    {
                        int r = await stream.ReadAsync(lenBuf, read, 4 - read);
                        if (r == 0) return;
                        read += r;
                    }

                    int msgLen = BitConverter.ToInt32(lenBuf, 0);
                    byte[] msgBuf = new byte[msgLen];
                    read = 0;
                    while (read < msgLen)
                    {
                        int r = await stream.ReadAsync(msgBuf, read, msgLen - read);
                        if (r == 0) return;
                        read += r;
                    }

                    string msg = Encoding.UTF8.GetString(msgBuf);
                    string[] parts = msg.Split('|');
                    if (parts.Length >= 2)
                    {
                        string name = parts[0];
                        string text = parts[1];
                        string time = parts.Length > 2 ? parts[2] : DateTime.Now.ToString("HH:mm:ss");
                        Console.WriteLine($"[{time}] {name}: {text}");
                    }
                }
            }
            catch { }
            finally
            {
                lock (locker) { connections.Remove(ip); }
                client.Close();
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] * - {ip}");
            }
        }

        static async Task ListenForBroadcast()
        {
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    var result = await udpClient.ReceiveAsync();
                    string data = Encoding.UTF8.GetString(result.Buffer);
                    string[] parts = data.Split('|');

                    if (parts.Length >= 3)
                    {
                        string senderIp = parts[0];
                        int senderPort = int.Parse(parts[1]);
                        string senderName = parts[2];

                        if (senderIp == ((IPEndPoint)udpClient.Client.LocalEndPoint).Address.ToString())
                            continue;

                        lock (locker)
                        {
                            if (connections.ContainsKey(senderIp)) continue;
                        }

                        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] * Nayden {senderName} ({senderIp}:{senderPort})");

                        try
                        {
                            TcpClient client = new TcpClient();
                            await client.ConnectAsync(IPAddress.Parse(senderIp), senderPort);

                            lock (locker)
                            {
                                if (!connections.ContainsKey(senderIp))
                                {
                                    connections[senderIp] = client;
                                    Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] * + {senderName} ({senderIp})");
                                }
                            }

                            Task.Run(() => HandleClient(client, senderIp));
                        }
                        catch { }
                    }
                }
                catch { break; }
            }
        }

        static async Task SendPeriodicBroadcast()
        {
            var broadcastEndpoint = new IPEndPoint(IPAddress.Broadcast, udpPort);

            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    string broadcastMsg = $"{((IPEndPoint)udpClient.Client.LocalEndPoint).Address}|{myPort}|{myName}";
                    byte[] data = Encoding.UTF8.GetBytes(broadcastMsg);
                    await udpClient.SendAsync(data, data.Length, broadcastEndpoint);
                }
                catch { }

                await Task.Delay(30000);
            }
        }
    }
}