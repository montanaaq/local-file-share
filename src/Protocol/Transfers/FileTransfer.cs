using System;

namespace Protocol;

public class FileTransfer
{
    public Guid FileId { get; set; }
    public Guid PeerDeviceId { get; set; }
    public string FileName { get; set; } = string.Empty;
    public ulong FileSize { get; set; }
    public required byte[] Sha256 { get; set; }
    public TransferDirection Direction { get; set; }
    public TransferState State { get; set; }
    public uint NextIndex { get; set; }
}
