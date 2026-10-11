using System;

namespace Protocol;

public class ProtocolConstants
{
    public const ushort DefaultPort = 5000;
    public const int ChunkSize = 1024;
    public const long MaxFileSize = 20 * 1024 * 1024;

    public static readonly TimeSpan AckTimeout = TimeSpan.FromMilliseconds(200);
    public const int MaxSendAttempts = 5;

    public static readonly TimeSpan Heartbeat = TimeSpan.FromSeconds(1);
    public static readonly TimeSpan PeerTimeout = TimeSpan.FromSeconds(5);
}
