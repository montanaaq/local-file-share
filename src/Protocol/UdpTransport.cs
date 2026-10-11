using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Protocol;

public sealed class UdpTransport(int localPort = 0) : IDisposable
{
    private readonly UdpClient _client = new(localPort);
    private uint _nextSeq;
    public int LocalPort => ((IPEndPoint)_client.Client.LocalEndPoint!).Port;

    public uint NextSeq() => _nextSeq++;

    public async Task SendAsync(Packet packet, IPEndPoint remote)
    {
        await _client.SendAsync(packet.Serialize(), remote);
    }

    public async Task<(Packet? Packet, IPEndPoint From)> ReceiveAsync(CancellationToken ct)
    {
        var result = await _client.ReceiveAsync(ct);
        return (Packet.Parse(result.Buffer), result.RemoteEndPoint);
    }

    // shortcut: ACK waits consume other messages; add a shared receive loop before app integration.
    public async Task SendReliableAsync(
        Packet packet,
        IPEndPoint remote,
        Guid remoteDeviceId,
        CancellationToken ct
    )
    {
        for (var attempt = 0; attempt < ProtocolConstants.MaxSendAttempts; attempt++)
        {
            await SendAsync(packet, remote);
            if (await WaitForAckAsync(packet.Seq, remote, remoteDeviceId, ct))
                return;
        }

        throw new TimeoutException($"No ACK for seq {packet.Seq}");
    }

    private async Task<bool> WaitForAckAsync(
        uint seq,
        IPEndPoint remote,
        Guid remoteDeviceId,
        CancellationToken ct
    )
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(ProtocolConstants.AckTimeout);

        try
        {
            while (true)
            {
                var (packet, from) = await ReceiveAsync(timeout.Token);
                if (
                    packet is not null
                    && from.Equals(remote)
                    && IsAckFor(packet, seq, remoteDeviceId)
                )
                    return true;
            }
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return false;
        }
    }

    private static bool IsAckFor(Packet packet, uint seq, Guid remoteDeviceId)
    {
        if (packet.Type != MessageType.Ack)
            return false;

        var ack = Payloads.DecodeAck(packet.Payload);
        return ack is not null && ack.Value.DeviceId == remoteDeviceId && ack.Value.AckedSeq == seq;
    }

    public void Dispose() => _client.Dispose();
}
