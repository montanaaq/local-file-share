using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Protocol.Extensions;
using Protocol.Messages;
using Protocol.Transfers.Models;

namespace Protocol.Transfers;

internal sealed class OutgoingFileExchange
{
    private readonly Lock _stateLock = new();
    private readonly HashSet<TransferKey> _activeOutgoing = [];
    private readonly Dictionary<TransferKey, long> _lastProbes = [];

    private readonly FileTransferService _transfers;
    private readonly Guid _deviceId;

    private readonly Func<MessageType, Guid, byte[], CancellationToken, Task> _sendReliable;
    private readonly Action<Guid, SocketException> _reportNetworkError;

    private readonly FileBackgroundTaskHandler _backgroundTasks;
    private readonly TimeProvider _timeProvider;
    private readonly CancellationToken _stoppingToken;

    public OutgoingFileExchange(
        FileTransferService transfers,
        Guid deviceId,
        Func<MessageType, Guid, byte[], CancellationToken, Task> sendReliable,
        Action<Guid, SocketException> reportNetworkError,
        FileBackgroundTaskHandler backgroundTasks,
        TimeProvider timeProvider,
        CancellationToken stoppingToken
    )
    {
        _transfers = transfers;
        _deviceId = deviceId;

        _sendReliable = sendReliable;
        _reportNetworkError = reportNetworkError;

        _backgroundTasks = backgroundTasks;
        _timeProvider = timeProvider;
        _stoppingToken = stoppingToken;
    }

    /// <summary>
    /// Для peerDeviceId подготавливает файл по filePath, отправляет предложение и ожидает отдельного решения.
    /// callerToken отменяет вызов; отключение обмена сохраняет передачу в паузе.
    /// </summary>
    public async Task<FileTransfer> OfferFileAsync(
        Guid peerDeviceId,
        string filePath,
        CancellationToken callerToken,
        CancellationToken exchangeToken
    )
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(
            callerToken,
            exchangeToken,
            _stoppingToken
        );

        var ct = cts.Token;
        ct.ThrowIfCancellationRequested();

        var transfer = await _transfers.PrepareOutgoingTransferAsync(peerDeviceId, filePath, ct);
        var key = new TransferKey(peerDeviceId, transfer.FileId);

        lock (_stateLock)
        {
            _activeOutgoing.Add(key);
        }

        try
        {
            try
            {
                await SendOfferAsync(transfer, ct).ConfigureAwait(false);

                bool accepted = await _transfers.WaitForFileAnswerAsync(key, ct);

                if (!accepted)
                {
                    return _transfers.GetTransfer(key);
                }
            }
            catch (OperationCanceledException) when (IsExchangeInterruption(callerToken))
            {
                _transfers.SetState(key, TransferState.Paused);
                return _transfers.GetTransfer(key);
            }
            catch (Exception exception)
            {
                _transfers.SetState(
                    key,
                    exception is OperationCanceledException
                        ? TransferState.Cancelled
                        : TransferState.Failed,
                    exception
                );
                throw;
            }

            await SendFileContentsAsync(key, ct, callerToken).ConfigureAwait(false);
            return _transfers.GetTransfer(key);
        }
        finally
        {
            lock (_stateLock)
            {
                _activeOutgoing.Remove(key);
            }
        }
    }

    /// <summary>
    /// Для передачи key отправляет оставшиеся чанки, FILE_DONE и ожидает FILE_RESULT.
    /// ct прерывает сетевую операцию; callerToken позволяет отличить отмену вызова от отключения обмена.
    /// </summary>
    private async Task SendFileContentsAsync(
        TransferKey key,
        CancellationToken ct,
        CancellationToken callerToken
    )
    {
        try
        {
            await SendChunksAsync(key, ct).ConfigureAwait(false);
            await _sendReliable(
                    MessageType.FileDone,
                    key.PeerDeviceId,
                    Payloads.EncodeFileDone(_deviceId, key.FileId),
                    ct
                )
                .ConfigureAwait(false);
            await _transfers.WaitForFileResultAsync(key, ct).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            _transfers.SetState(key, TransferState.Paused);
        }
        catch (SocketException exception) when (exception.IsUnreachable())
        {
            _transfers.SetState(key, TransferState.Paused);

            _reportNetworkError(key.PeerDeviceId, exception);
        }
        catch (OperationCanceledException) when (IsExchangeInterruption(callerToken))
        {
            _transfers.SetState(key, TransferState.Paused);
        }
        catch (InvalidOperationException)
            when (_transfers.GetTransfer(key).State
                    is TransferState.Paused
                        or TransferState.Cancelled
                        or TransferState.Failed
                        or TransferState.Completed
            ) { }
        catch (Exception exception)
        {
            _transfers.SetState(
                key,
                exception is OperationCanceledException
                    ? TransferState.Cancelled
                    : TransferState.Failed,
                exception
            );
            throw;
        }
    }

    /// <summary>
    /// Читает исходный файл передачи key с первого недостающего чанка и продвигает индекс только после ACK.
    /// ct отменяет чтение или сетевое ожидание.
    /// </summary>
    private async Task SendChunksAsync(TransferKey key, CancellationToken ct)
    {
        await using var stream = await _transfers
            .OpenOutgoingFileAsync(key, ct)
            .ConfigureAwait(false);

        var transfer = _transfers.GetTransfer(key);
        uint index = transfer.NextIndex;
        long offset = (long)index * ProtocolConstants.ChunkSize;
        stream.Position = offset;

        byte[] buffer = new byte[ProtocolConstants.ChunkSize];

        while (offset < (long)transfer.FileSize)
        {
            ct.ThrowIfCancellationRequested();

            int length = (int)Math.Min(buffer.Length, (long)transfer.FileSize - offset);

            await stream.ReadExactlyAsync(buffer.AsMemory(0, length), ct).ConfigureAwait(false);

            byte[] payload = Payloads.EncodeChunk(
                _deviceId,
                key.FileId,
                index,
                buffer.AsSpan(0, length)
            );

            await _sendReliable(MessageType.Chunk, key.PeerDeviceId, payload, ct)
                .ConfigureAwait(false);

            _transfers.ConfirmOutgoingChunk(key, index);
            index++;
            offset += length;
        }
    }

    private Task SendOfferAsync(FileTransfer transfer, CancellationToken ct)
    {
        byte[] payload = Payloads.EncodeFileOffer(
            _deviceId,
            transfer.FileId,
            transfer.FileSize,
            transfer.Sha256,
            transfer.FileName
        );

        return _sendReliable(MessageType.FileOffer, transfer.PeerDeviceId, payload, ct);
    }

    /// <summary>
    /// Возобновляет исходящую передачу key по nextIndex получателя, не продлевая исходный срок паузы.
    /// exchangeToken отменяет попытку при выключении обмена; maintenanceToken — при остановке обслуживания.
    /// </summary>
    private async Task ResumeOutgoingAsync(
        TransferKey key,
        CancellationToken exchangeToken,
        CancellationToken maintenanceToken
    )
    {
        try
        {
            if (exchangeToken.IsCancellationRequested)
            {
                return;
            }

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(
                maintenanceToken,
                exchangeToken,
                _stoppingToken
            );

            if (!_transfers.PrepareResume(key))
            {
                return;
            }

            var transfer = _transfers.GetTransfer(key);

            try
            {
                await SendOfferAsync(transfer, cts.Token).ConfigureAwait(false);

                bool accepted = await _transfers
                    .WaitForFileAnswerAsync(key, cts.Token, resuming: true)
                    .ConfigureAwait(false);

                if (!accepted)
                {
                    return;
                }

                await SendFileContentsAsync(key, cts.Token, maintenanceToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                _transfers.SetState(key, TransferState.Paused);
            }
            catch (OperationCanceledException) when (!_stoppingToken.IsCancellationRequested)
            {
                _transfers.SetState(key, TransferState.Paused);
            }
            catch (SocketException exception) when (exception.IsUnreachable())
            {
                _transfers.SetState(key, TransferState.Paused);
                _reportNetworkError(key.PeerDeviceId, exception);
            }
            catch (Exception exception)
                when (exception is IOException or UnauthorizedAccessException)
            {
                _transfers.SetState(key, TransferState.Failed, exception);
            }
        }
        catch (OperationCanceledException) when (_stoppingToken.IsCancellationRequested) { }
        finally
        {
            lock (_stateLock)
            {
                _activeOutgoing.Remove(key);
            }
        }
    }

    /// <summary>
    /// Запускает пробы приостановленных исходящих передач, исключая уже работающие операции.
    /// Владелец вызывает метод только при включённом обмене и до начала закрытия.
    /// </summary>
    /// <param name="exchangeToken">Токен текущего периода включённого обмена.</param>
    /// <param name="ct">Токен остановки обслуживания.</param>
    public void ResumePausedTransfers(CancellationToken exchangeToken, CancellationToken ct)
    {
        foreach (var transfer in _transfers.GetTransfers())
        {
            if (
                transfer.Direction != TransferDirection.Outgoing
                || transfer.State != TransferState.Paused
            )
            {
                continue;
            }

            var key = transfer.Key;

            lock (_stateLock)
            {
                if (_activeOutgoing.Contains(key))
                {
                    continue;
                }

                long now = _timeProvider.GetTimestamp();

                if (!_lastProbes.TryGetValue(key, out long lastProbe))
                {
                    _lastProbes[key] = now;
                    continue;
                }

                if (_timeProvider.GetElapsedTime(lastProbe, now) < ProtocolConstants.ProbeInterval)
                {
                    continue;
                }

                _lastProbes[key] = now;
                _activeOutgoing.Add(key);
                _backgroundTasks.Start(() => ResumeOutgoingAsync(key, exchangeToken, ct));
            }
        }
    }

    private bool IsExchangeInterruption(CancellationToken callerToken) =>
        !_stoppingToken.IsCancellationRequested && !callerToken.IsCancellationRequested;
}
