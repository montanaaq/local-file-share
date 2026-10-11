using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Protocol;

public sealed class UdpTransport : IDisposable
{
    private readonly Socket _socket;
    private uint _nextSeq;
    private readonly Channel<(Packet Packet, IPEndPoint From)> _incoming = Channel.CreateUnbounded<(
        Packet Packet,
        IPEndPoint From
    )>();
    private readonly ConcurrentDictionary<
        (Guid DeviceId, IPEndPoint Endpoint, uint Seq),
        TaskCompletionSource<bool>
    > _pendingAcks = new();
    private readonly CancellationTokenSource _receiveCts = new();
    private readonly Task _receiveTask;
    private int _disposed;

    public UdpTransport(int localPort = 0)
    {
        _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        try
        {
            _socket.ExclusiveAddressUse = true;
            _socket.EnableBroadcast = true;
            _socket.Bind(new IPEndPoint(IPAddress.Any, localPort));
        }
        catch
        {
            _socket.Dispose();
            _receiveCts.Dispose();
            throw;
        }
        _receiveTask = ReceiveLoopAsync(_receiveCts.Token);
    }

    public int LocalPort => ((IPEndPoint)_socket.LocalEndPoint!).Port;

    public uint NextSeq() => _nextSeq++;

    public async Task SendAsync(Packet packet, IPEndPoint remote)
    {
        var bytes = packet.Serialize();

        await _socket.SendToAsync(bytes.AsMemory(), SocketFlags.None, remote);
    }

    private async Task<(Packet? Packet, IPEndPoint From)> ReceiveFromSocketAsync(
        CancellationToken ct
    )
    {
        byte[] buf = new byte[ushort.MaxValue];

        var result = await _socket
            .ReceiveFromAsync(
                buf.AsMemory(),
                SocketFlags.None,
                new IPEndPoint(IPAddress.Any, 0),
                ct
            )
            .ConfigureAwait(false);

        var packet = Packet.Parse(buf.AsSpan(0, result.ReceivedBytes));
        var from = (IPEndPoint)result.RemoteEndPoint;

        return (packet, from);
    }

    private async Task ReceiveLoopAsync(CancellationToken ct)
    {
        Exception? error = null;
        try
        {
            while (true)
            {
                var (packet, from) = await ReceiveFromSocketAsync(ct).ConfigureAwait(false);
                if (packet is null)
                    continue;

                if (packet.Type == MessageType.Ack)
                {
                    var ack = Payloads.DecodeAck(packet.Payload);

                    if (ack is null)
                        continue;

                    var key = (ack.Value.DeviceId, from, ack.Value.AckedSeq);

                    if (_pendingAcks.TryGetValue(key, out var completion))
                        completion.TrySetResult(true);

                    continue;
                }

                await _incoming.Writer.WriteAsync((packet, from), ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex)
        {
            error = ex;
        }
        finally
        {
            _incoming.Writer.TryComplete(error);
        }
    }

    public async Task<(Packet Packet, IPEndPoint From)> ReceiveAsync(CancellationToken ct)
    {
        return await _incoming.Reader.ReadAsync(ct);
    }

    public async Task SendReliableAsync(
        Packet packet,
        IPEndPoint remote,
        Guid remoteDeviceId,
        CancellationToken ct
    )
    {
        ct.ThrowIfCancellationRequested();

        var key = (remoteDeviceId, remote, packet.Seq);
        var completion = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );

        if (!_pendingAcks.TryAdd(key, completion))
            throw new InvalidOperationException($"Already waiting for ACK for seq {packet.Seq}");

        try
        {
            for (var attempt = 0; attempt < ProtocolConstants.MaxSendAttempts; attempt++)
            {
                ct.ThrowIfCancellationRequested();

                await SendAsync(packet, remote);

                if (await WaitForAckAsync(completion.Task, ct).ConfigureAwait(false))
                    return;
            }

            throw new TimeoutException($"No ACK for seq {packet.Seq}");
        }
        finally
        {
            _pendingAcks.TryRemove(key, out _);
        }
    }

    private async Task<bool> WaitForAckAsync(Task<bool> ackTask, CancellationToken ct)
    {
        try
        {
            return await ackTask.WaitAsync(ProtocolConstants.AckTimeout, ct).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        try
        {
            _receiveCts.Cancel();
            _receiveTask.GetAwaiter().GetResult();
        }
        finally
        {
            _socket.Dispose();
            _receiveCts.Dispose();
        }
    }
}
