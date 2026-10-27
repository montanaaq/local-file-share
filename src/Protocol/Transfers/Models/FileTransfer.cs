using System;

namespace Protocol;

/// <summary>
/// Снимок передачи. Свойства изменяет только библиотека; SHA-256 копируется при создании снимка.
/// </summary>
public class FileTransfer
{
    public Guid FileId { get; internal set; }
    public Guid PeerDeviceId { get; internal set; }
    public string FileName { get; internal set; } = string.Empty;
    public ulong FileSize { get; internal set; }
    public byte[] Sha256 { get; internal set; } = [];
    public TransferDirection Direction { get; internal set; }
    public TransferState State { get; internal set; }
    public uint NextIndex { get; internal set; }
    public string? SavedPath { get; internal set; }
    public string? Error { get; internal set; }
    public TimeSpan? PauseRemaining { get; internal set; }
    public int PauseCount { get; internal set; }
    public int ResumeCount { get; internal set; }
    public TransferKey Key => new(PeerDeviceId, FileId);
    public uint ChunkCount =>
        (uint)((FileSize + ProtocolConstants.ChunkSize - 1) / ProtocolConstants.ChunkSize);
    public ulong TransferredBytes =>
        Math.Min(FileSize, (ulong)NextIndex * ProtocolConstants.ChunkSize);

    internal FileTransfer Snapshot() =>
        new()
        {
            FileId = FileId,
            PeerDeviceId = PeerDeviceId,
            FileName = FileName,
            FileSize = FileSize,
            Sha256 = (byte[])Sha256.Clone(),
            Direction = Direction,
            State = State,
            NextIndex = NextIndex,
            SavedPath = SavedPath,
            Error = Error,
            PauseRemaining = PauseRemaining,
            PauseCount = PauseCount,
            ResumeCount = ResumeCount,
        };
}
