using System;

namespace Protocol.Transfers.Models;

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

    /// <summary>Техническое описание ошибки для диагностики.</summary>
    public string? Error { get; internal set; }

    /// <summary>Причина ошибки для отображения сообщения на языке интерфейса.</summary>
    public TransferError ErrorCode { get; internal set; }

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
            ErrorCode = ErrorCode,
            PauseRemaining = PauseRemaining,
            PauseCount = PauseCount,
            ResumeCount = ResumeCount,
        };
}
