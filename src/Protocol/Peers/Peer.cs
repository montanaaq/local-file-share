using System;
using System.Net;

namespace Protocol.Peers;

public sealed record Peer(
    Guid DeviceId,
    string Name,
    IPAddress Address,
    int Port,
    DateTimeOffset LastSeen,
    PeerAvailability Availability
)
{
    public IPEndPoint Endpoint => new(Address, Port);
}
