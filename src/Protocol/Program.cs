using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Protocol;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        if (args.Length != 2 || !ushort.TryParse(args[0], out var port) || port == 0)
        {
            Console.Error.WriteLine("Usage: Protocol <port 1-65535> <device name>");
            return 1;
        }

        var name = args[1];
        var deviceId = Guid.NewGuid();

        try
        {
            Payloads.EncodePeerInfo(deviceId, name);

            using var transport = new UdpTransport(port);
            using var cts = new CancellationTokenSource();

            ConsoleCancelEventHandler cancelHandler = (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            Console.CancelKeyPress += cancelHandler;
            try
            {
                Console.WriteLine($"Name: {name}");
                Console.WriteLine($"Device ID: {deviceId}");
                Console.WriteLine($"UDP port: {transport.LocalPort}");
                Console.WriteLine("Press Ctrl+C to stop.");

                var discover = new Packet(
                    MessageType.Discover,
                    transport.NextSeq(),
                    Payloads.EncodePeerInfo(deviceId, name)
                );

                await transport.SendAsync(
                    discover,
                    new IPEndPoint(IPAddress.Loopback, ProtocolConstants.DefaultPort)
                );

                while (!cts.IsCancellationRequested)
                {
                    var (packet, from) = await transport.ReceiveAsync(cts.Token);

                    if (packet.Type is not (MessageType.Discover or MessageType.PeerInfo))
                        continue;

                    var peerInfo = Payloads.DecodePeerInfo(packet.Payload);
                    if (peerInfo is null)
                        continue;

                    if (peerInfo.Value.DeviceId == deviceId)
                        continue;

                    Console.WriteLine($"{packet.Type}: {peerInfo.Value.Name} at {from}");

                    if (packet.Type != MessageType.Discover)
                        continue;

                    var response = new Packet(
                        MessageType.PeerInfo,
                        transport.NextSeq(),
                        Payloads.EncodePeerInfo(deviceId, name)
                    );

                    await transport.SendAsync(response, from);
                }
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested) { }
            finally
            {
                Console.CancelKeyPress -= cancelHandler;
            }

            return 0;
        }
        catch (ArgumentException ex)
        {
            Console.Error.WriteLine($"Invalid device name: {ex.Message}");
            return 1;
        }
        catch (SocketException ex)
        {
            Console.Error.WriteLine($"UDP error on port {port}: {ex.Message}");
            return 1;
        }
    }
}
