using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Protocol;

public sealed class FileTransferService
{
    private readonly Lock _stateLock = new();

    private readonly Dictionary<(Guid PeerDeviceId, Guid FileId), FileTransfer> _transfers = [];

    public event Action<FileTransfer>? FileOffered;

    public void HandleFileOffer(ReadOnlySpan<byte> payload)
    {
        var offer = Payloads.DecodeFileOffer(payload);
        if (offer == null)
            return;

        var key = (offer.Value.DeviceId, offer.Value.FileId);
        FileTransfer fileTransfer;

        lock (_stateLock)
        {
            if (_transfers.ContainsKey(key))
                return;

            fileTransfer = new FileTransfer()
            {
                FileId = offer.Value.FileId,
                PeerDeviceId = offer.Value.DeviceId,
                FileName = offer.Value.Name,
                FileSize = offer.Value.Size,
                Sha256 = offer.Value.Sha256,
                Direction = TransferDirection.Incoming,
                State = TransferState.WaitingForAnswer,
            };

            _transfers.Add(key, fileTransfer);
        }

        FileOffered?.Invoke(fileTransfer);
    }

    public FileTransfer CreateOutgoingTransfer(
        Guid peerDeviceId,
        string fileName,
        ulong fileSize,
        byte[] sha256
    )
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            fileSize,
            (ulong)ProtocolConstants.MaxFileSize
        );

        ArgumentNullException.ThrowIfNull(sha256);
        if (sha256.Length != PayloadsConstants.Sha256Size)
            throw new ArgumentException("SHA-256 must contain 32 bytes", nameof(sha256));

        if (
            fileName == string.Empty
            || Encoding.UTF8.GetByteCount(fileName) > PayloadsConstants.MaxNameBytes
        )
            throw new ArgumentException("File Name required or not too long");

        FileTransfer fileTransfer;

        lock (_stateLock)
        {
            fileTransfer = new()
            {
                FileId = Guid.NewGuid(),
                PeerDeviceId = peerDeviceId,
                FileName = fileName,
                FileSize = fileSize,
                Sha256 = sha256,
                Direction = TransferDirection.Outgoing,
                State = TransferState.WaitingForAnswer,
            };
            var key = (fileTransfer.PeerDeviceId, fileTransfer.FileId);
            _transfers.Add(key, fileTransfer);
        }

        return fileTransfer;
    }

    public async Task<FileTransfer> PrepareOutgoingTransferAsync(
        Guid peerDeviceId,
        string filePath,
        CancellationToken ct
    )
    {
        await using var fileStream = new FileStream(
            filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read
        );

        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            fileStream.Length,
            ProtocolConstants.MaxFileSize
        );

        var fileName = Path.GetFileName(filePath);
        var sha256 = await SHA256.HashDataAsync(fileStream, ct);

        return CreateOutgoingTransfer(peerDeviceId, fileName, (ulong)fileStream.Length, sha256);
    }

    public void SetState(Guid peerDeviceId, Guid fileId, TransferState state)
    {
        lock (_stateLock)
        {
            var key = (peerDeviceId, fileId);
            _transfers[key].State = state;
        }
    }

    public FileTransfer SetIncomingDecision(Guid peerDeviceId, Guid fileId, bool accepted)
    {
        lock (_stateLock)
        {
            var key = (peerDeviceId, fileId);
            if (!_transfers.TryGetValue(key, out var transfer))
                throw new ArgumentException("Transfer not exists");

            if (transfer.Direction != TransferDirection.Incoming)
                throw new InvalidOperationException("Transfer must be incoming");

            if (transfer.State != TransferState.WaitingForAnswer)
                throw new InvalidOperationException("Transfer is not waiting for a decision");

            transfer.State = accepted ? TransferState.Transferring : TransferState.Rejected;

            return transfer;
        }
    }

    public bool HandleFileAnswer(ReadOnlySpan<byte> payload)
    {
        var answer = Payloads.DecodeFileAnswer(payload);
        if (answer is null)
            return false;

        lock (_stateLock)
        {
            var key = (answer.Value.DeviceId, answer.Value.FileId);
            if (!_transfers.TryGetValue(key, out var transfer))
                return false;

            if (transfer.Direction != TransferDirection.Outgoing)
                return false;

            if (transfer.State != TransferState.WaitingForAnswer)
                return false;

            if (!answer.Value.Accepted)
            {
                transfer.State = TransferState.Rejected;
                return true;
            }

            if (answer.Value.NextIndex != 0)
                return false;

            transfer.NextIndex = answer.Value.NextIndex;
            transfer.State = TransferState.Transferring;
        }
        return true;
    }
}
