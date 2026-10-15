using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Protocol;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        if (args.Length < 2 || !ushort.TryParse(args[0], out var port) || port == 0)
        {
            Console.Error.WriteLine(
                "Usage: Protocol <port 1-65535> <device name> [--loss N] [--peer IPv4:port]"
            );
            return 1;
        }

        try
        {
            await using var service = new PeerService(args[1], port);
            service.Transfers.FileOffered += transfer =>
                Console.WriteLine(
                    $"File offer: {transfer.FileName}, {transfer.FileSize} bytes, "
                        + $"from {transfer.PeerDeviceId}, file ID: {transfer.FileId}"
                );
            for (var index = 2; index < args.Length; index += 2)
            {
                if (index + 1 >= args.Length)
                    throw new ArgumentException("Each option requires a value");
                switch (args[index])
                {
                    case "--loss":
                        service.LossRate = int.Parse(args[index + 1]);
                        break;
                    case "--peer":
                        service.AddManualEndpoint(IPEndPoint.Parse(args[index + 1]));
                        break;
                    default:
                        throw new ArgumentException($"Unknown option: {args[index]}");
                }
            }

            var displayed = new ConcurrentDictionary<Guid, Peer>();
            service.PeerChanged += peer =>
            {
                if (
                    displayed.TryGetValue(peer.DeviceId, out var previous)
                    && previous.Name == peer.Name
                    && previous.Endpoint.Equals(peer.Endpoint)
                    && previous.Availability == peer.Availability
                )
                    return;
                displayed[peer.DeviceId] = peer;
                Console.WriteLine($"{peer.Name} at {peer.Endpoint}: {peer.Availability}");
            };

            service.NetworkError += (endpoint, error) =>
                Console.Error.WriteLine($"UDP error for {endpoint}: {error.Message}");

            using var cts = new CancellationTokenSource();
            ConsoleCancelEventHandler cancelHandler = (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };
            Console.CancelKeyPress += cancelHandler;
            try
            {
                Console.WriteLine($"Name: {args[1]}");
                Console.WriteLine($"Device ID: {service.DeviceId}");
                Console.WriteLine($"UDP port: {service.LocalPort}");
                Console.WriteLine(
                    "Commands: peers, peer IPv4:port, offer <deviceId> <filePath>, accept <senderDeviceId> <fileId>, reject <senderDeviceId> <fileId>, loss N, quit. Ctrl+C to stop."
                );

                var running = service.RunAsync(cts.Token);
                // Native console reads can block until Enter; they must not hold up network shutdown.
                var input = Task.Run(() => ReadCommandsAsync(service, cts.Token));
                try
                {
                    var completed = await Task.WhenAny(running, input);
                    await completed;
                }
                finally
                {
                    cts.Cancel();
                    try
                    {
                        await running;
                        if (input.IsCompleted)
                            await input;
                    }
                    catch (OperationCanceledException) when (cts.IsCancellationRequested) { }
                }
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested) { }
            finally
            {
                Console.CancelKeyPress -= cancelHandler;
            }
            return 0;
        }
        catch (Exception ex) when (ex is ArgumentException or FormatException or OverflowException)
        {
            Console.Error.WriteLine($"Invalid arguments: {ex.Message}");
            return 1;
        }
        catch (SocketException ex)
        {
            Console.Error.WriteLine($"UDP error on port {port}: {ex.Message}");
            return 1;
        }
    }

    private static async Task ReadCommandsAsync(PeerService service, CancellationToken ct)
    {
        while (true)
        {
            var line = await Console.In.ReadLineAsync(ct);
            ct.ThrowIfCancellationRequested();
            if (line is null || line.Trim() == "quit")
            {
                return;
            }

            var parts = line.Trim().Split(" ", 2, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 0)
                continue;
            try
            {
                switch (parts[0])
                {
                    case "peers" when parts.Length == 1:
                        foreach (var peer in service.GetPeers())
                            Console.WriteLine(
                                $"{peer.DeviceId}: {peer.Name} at {peer.Endpoint}: {peer.Availability}"
                            );
                        break;
                    case "peer" when parts.Length == 2:
                        service.AddManualEndpoint(IPEndPoint.Parse(parts[1]));
                        break;
                    case "offer" when parts.Length == 2:
                        var arguments = parts[1].Split(
                            ' ',
                            2,
                            StringSplitOptions.RemoveEmptyEntries
                        );
                        if (arguments.Length != 2)
                            throw new ArgumentException("Usage: offer <deviceId> <filePath>");

                        var peerDeviceId = Guid.Parse(arguments[0]);
                        var transfer = await service.OfferFileAsync(
                            peerDeviceId,
                            arguments[1],
                            ct
                        );
                        Console.WriteLine(
                            $"Offer delivered, file ID: {transfer.FileId}. Waiting for the recipient's decision."
                        );
                        break;
                    case "accept" when parts.Length == 2:
                    case "reject" when parts.Length == 2:
                        var answerArguments = parts[1].Split(
                            ' ',
                            StringSplitOptions.RemoveEmptyEntries
                        );
                        if (answerArguments.Length != 2)
                            throw new ArgumentException(
                                $"Usage: {parts[0]} <senderDeviceId> <fileId>"
                            );

                        var senderDeviceId = Guid.Parse(answerArguments[0]);
                        var fileId = Guid.Parse(answerArguments[1]);
                        var answeredTransfer = await service.AnswerFileOfferAsync(
                            senderDeviceId,
                            fileId,
                            accepted: parts[0] == "accept",
                            ct
                        );
                        Console.WriteLine(
                            $"Answer delivered, file ID: {answeredTransfer.FileId}, state: {answeredTransfer.State}"
                        );
                        break;
                    case "loss" when parts.Length == 2:
                        service.LossRate = int.Parse(parts[1]);
                        Console.WriteLine($"Loss rate: {service.LossRate}%");
                        break;
                    default:
                        Console.WriteLine(
                            "Commands: peers, peer IPv4:port, offer <deviceId> <filePath>, accept <senderDeviceId> <fileId>, reject <senderDeviceId> <fileId>, loss N, quit"
                        );
                        break;
                }
            }
            catch (Exception ex)
                when (ex is ArgumentException or FormatException or OverflowException or InvalidOperationException)
            {
                Console.Error.WriteLine($"Invalid command: {ex.Message}");
            }
            catch (Exception ex)
                when (ex is IOException or UnauthorizedAccessException or TimeoutException or SocketException)
            {
                Console.Error.WriteLine($"Command failed: {ex.Message}");
            }
        }
    }
}
