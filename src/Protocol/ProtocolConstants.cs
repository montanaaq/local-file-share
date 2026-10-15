using System;

namespace Protocol;

public static class ProtocolConstants
{
    public const ushort DefaultPort = 5000;
    public const int ChunkSize = 1024;
    public const int ClipboardTextSize = 1000;
    public const long MaxFileSize = 20 * 1024 * 1024;
    public static readonly TimeSpan AckTimeout = TimeSpan.FromMilliseconds(200);
    public const int MaxSendAttempts = 5;
    public const int IncomingQueueCapacity = 256;
    public static readonly TimeSpan Heartbeat = TimeSpan.FromSeconds(1);
    public static readonly TimeSpan PeerTimeout = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan DiscoverInterval = TimeSpan.FromSeconds(2);
    public static readonly TimeSpan ConnectionTimeout = TimeSpan.FromSeconds(3);
    public static readonly TimeSpan FileAnswerTimeout = TimeSpan.FromSeconds(60);
    public const int ProcessedMessagesCapacity = 256;
}
