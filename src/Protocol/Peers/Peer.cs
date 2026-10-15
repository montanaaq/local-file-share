using System;
using System.Net;

namespace Protocol;

public enum PeerAvailability
{
    Available,
    Unreachable,
    Offline,
}

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
