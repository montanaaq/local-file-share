using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Protocol.Extensions;
using Protocol.Messages;

namespace Protocol.Transport;

internal interface IMessageTransport
{
    uint NextSeq();
    Task SendReliableAsync(
        Packet packet,
        IPEndPoint remote,
        Guid remoteDeviceId,
        CancellationToken ct
    );
    Task SendAckAsync(uint ackedSeq, IPEndPoint remote, Guid deviceId);
}
