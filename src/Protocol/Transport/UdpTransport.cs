using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Protocol;

public sealed class UdpTransport : IDisposable, IMessageTransport
{
    private readonly Socket _socket;
    private uint _nextSeq;

    private readonly Channel<(Packet Packet, IPEndPoint From, int Generation)> _incoming =
        Channel.CreateBounded<(Packet Packet, IPEndPoint From, int Generation)>(
            ProtocolConstants.IncomingQueueCapacity
        );

    private readonly ConcurrentDictionary<
        (Guid DeviceId, IPEndPoint Endpoint, uint Seq),
        TaskCompletionSource<bool>
    > _pendingAcks = new();

    private readonly CancellationTokenSource _receiveCts = new();
    private readonly Task _receiveTask;
    private int _disposed;
    private readonly Lock _ackLock = new();
    private Exception? _receiveError;
    private int _lossRate;
    private int _exchangeEnabled = 1;
    private int _exchangeGeneration;
    private long _sentPackets;
    private long _receivedPackets;
    private long _acknowledgmentsSent;
    private long _acknowledgmentsReceived;
    private long _retries;
    private long _droppedOutgoingPackets;
    private long _droppedIncomingPackets;
    private long _invalidPackets;
    private long _queueOverflows;
    private long _errors;

    /// <summary>
    /// Открывает IPv4 UDP-сокет с broadcast и запускает единый цикл приёма.
    /// </summary>
    /// <param name="localPort">Локальный порт; 0 выбирает свободный порт. Для приложения передавайте DefaultPort явно.</param>
    /// <param name="initialSeq">Следующий seq, сохранённый после закрытия предыдущей сетевой сессии.</param>
    public UdpTransport(int localPort = 0, uint initialSeq = 0)
    {
        _nextSeq = initialSeq;
        _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        try
        {
            _socket.ExclusiveAddressUse = true;
            _socket.EnableBroadcast = true;
            _socket.Bind(new IPEndPoint(IPAddress.Any, localPort));
        }
        catch
        {
            _socket.Dispose();
            _receiveCts.Dispose();
            throw;
        }
        _receiveTask = ReceiveLoopAsync(_receiveCts.Token);
    }

    public int LocalPort => ((IPEndPoint)_socket.LocalEndPoint!).Port;

    public bool IsExchangeEnabled
    {
        get => Volatile.Read(ref _exchangeEnabled) != 0;
        set
        {
            lock (_ackLock)
            {
                ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

                int enabled = value ? 1 : 0;
                if (_exchangeEnabled == enabled)
                {
                    return;
                }

                Interlocked.Increment(ref _exchangeGeneration);
                Volatile.Write(ref _exchangeEnabled, enabled);
            }
        }
    }

    public TransportStatistics GetStatistics() =>
        new(
            Interlocked.Read(ref _sentPackets),
            Interlocked.Read(ref _receivedPackets),
            Interlocked.Read(ref _acknowledgmentsSent),
            Interlocked.Read(ref _acknowledgmentsReceived),
            Interlocked.Read(ref _retries),
            Interlocked.Read(ref _droppedOutgoingPackets),
            Interlocked.Read(ref _droppedIncomingPackets),
            Interlocked.Read(ref _invalidPackets),
            Interlocked.Read(ref _queueOverflows),
            Interlocked.Read(ref _errors)
        );

    public uint NextSeq() => unchecked(Interlocked.Increment(ref _nextSeq) - 1);

    public uint NextSequence => Volatile.Read(ref _nextSeq);

    public int LossRate
    {
        get => Volatile.Read(ref _lossRate);
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(value, 100);

            Volatile.Write(ref _lossRate, value);
        }
    }

    /// <summary>
    /// Отправляет пакет без ожидания ACK с учётом LossRate и состояния обмена.
    /// </summary>
    /// <param name="packet">Пакет с уже назначенным seq; при повторе используйте тот же пакет.</param>
    /// <param name="remote">IPv4-адрес и UDP-порт получателя.</param>
    /// <param name="ct">Токен отмены операции.</param>
    public async Task SendAsync(Packet packet, IPEndPoint remote, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(packet);

        ArgumentNullException.ThrowIfNull(remote);

        ct.ThrowIfCancellationRequested();
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

        byte[] bytes = packet.Serialize();

        if (!IsExchangeEnabled || ShouldDropPacket())
        {
            Interlocked.Increment(ref _droppedOutgoingPackets);
            return;
        }

        try
        {
            await _socket
                .SendToAsync(bytes.AsMemory(), SocketFlags.None, remote, ct)
                .ConfigureAwait(false);
            Interlocked.Increment(ref _sentPackets);
            if (packet.Type == MessageType.Ack)
            {
                Interlocked.Increment(ref _acknowledgmentsSent);
            }
        }
        catch (SocketException)
        {
            Interlocked.Increment(ref _errors);
            throw;
        }
    }

    public Task SendAckAsync(uint ackedSeq, IPEndPoint remote, Guid deviceId)
    {
        byte[] payload = Payloads.EncodeAck(deviceId, ackedSeq);
        var packet = new Packet(MessageType.Ack, NextSeq(), payload);
        return SendAsync(packet, remote);
    }

    private async Task<(Packet? Packet, IPEndPoint From)> ReceiveFromSocketAsync(
        Memory<byte> buffer,
        CancellationToken ct
    )
    {
        var result = await _socket
            .ReceiveFromAsync(buffer, SocketFlags.None, new IPEndPoint(IPAddress.Any, 0), ct)
            .ConfigureAwait(false);

        Interlocked.Increment(ref _receivedPackets);

        var packet = Packet.Parse(buffer.Span[..result.ReceivedBytes]);
        var from = (IPEndPoint)result.RemoteEndPoint;

        return (packet, from);
    }

    private async Task ReceiveLoopAsync(CancellationToken ct)
    {
        byte[] buffer = new byte[ushort.MaxValue];
        Exception? error = null;
        try
        {
            while (true)
            {
                // Discard receives spanning a toggle as well as already queued packets.
                int generation = Volatile.Read(ref _exchangeGeneration);
                (Packet? Packet, IPEndPoint From) incoming;
                try
                {
                    incoming = await ReceiveFromSocketAsync(buffer, ct).ConfigureAwait(false);
                }
                catch (SocketException exception)
                    when (exception.SocketErrorCode
                            is SocketError.ConnectionReset
                                or SocketError.NetworkReset
                                or SocketError.ConnectionRefused
                    )
                {
                    // An ICMP error for an earlier datagram does not close a UDP listener.
                    Interlocked.Increment(ref _errors);
                    continue;
                }

                var (packet, from) = incoming;
                if (
                    !IsExchangeEnabled
                    || generation != Volatile.Read(ref _exchangeGeneration)
                    || ShouldDropPacket()
                )
                {
                    Interlocked.Increment(ref _droppedIncomingPackets);
                    continue;
                }

                if (packet is null)
                {
                    Interlocked.Increment(ref _invalidPackets);
                    continue;
                }

                if (packet.Type == MessageType.Ack)
                {
                    var ack = Payloads.DecodeAck(packet.Payload);

                    if (ack is null)
                    {
                        Interlocked.Increment(ref _invalidPackets);
                        continue;
                    }

                    var key = (ack.Value.DeviceId, from, ack.Value.AckedSeq);

                    if (
                        _pendingAcks.TryGetValue(key, out var completion)
                        && !completion.Task.IsCompleted
                    )
                    {
                        Interlocked.Increment(ref _acknowledgmentsReceived);
                        completion.TrySetResult(true);
                    }

                    continue;
                }

                if (!Payloads.IsValid(packet.Type, packet.Payload))
                {
                    Interlocked.Increment(ref _invalidPackets);
                    continue;
                }

                if (!_incoming.Writer.TryWrite((packet, from, generation)))
                {
                    Interlocked.Increment(ref _queueOverflows);
                    continue;
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception exception)
        {
            Interlocked.Increment(ref _errors);
            error = exception;
        }
        finally
        {
            _incoming.Writer.TryComplete(error);

            var stopError = error ?? new ObjectDisposedException(nameof(UdpTransport));

            lock (_ackLock)
            {
                _receiveError = stopError;
            }

            foreach (var completion in _pendingAcks.Values)
            {
                completion.TrySetException(stopError);
            }
        }
    }

    /// <summary>
    /// Возвращает следующее проверенное сообщение из очереди, пропуская пакеты до отключения обмена.
    /// ACK обрабатываются внутри транспорта и в эту очередь не попадают.
    /// </summary>
    /// <param name="ct">Токен отмены операции.</param>
    /// <returns>Сообщение и адрес его отправителя. Очередь должен читать один потребитель.</returns>
    public async Task<(Packet Packet, IPEndPoint From)> ReceiveAsync(CancellationToken ct)
    {
        while (true)
        {
            var incoming = await _incoming.Reader.ReadAsync(ct).ConfigureAwait(false);
            if (IsExchangeEnabled && incoming.Generation == Volatile.Read(ref _exchangeGeneration))
            {
                return (incoming.Packet, incoming.From);
            }

            Interlocked.Increment(ref _droppedIncomingPackets);
        }
    }

    /// <summary>
    /// Повторяет отправку одного пакета до совпадающего ACK или исчерпания попыток.
    /// HEARTBEAT отправляется один раз; тайм-аут приводит к TimeoutException.
    /// </summary>
    /// <param name="packet">Пакет с назначенным seq, сохраняемым при повторах.</param>
    /// <param name="remote">Адрес и порт получателя, с которыми сверяется источник ACK.</param>
    /// <param name="remoteDeviceId">Ожидаемый deviceId отправителя ACK.</param>
    /// <param name="ct">Токен отмены операции.</param>
    public async Task SendReliableAsync(
        Packet packet,
        IPEndPoint remote,
        Guid remoteDeviceId,
        CancellationToken ct
    )
    {
        ArgumentNullException.ThrowIfNull(packet);

        ArgumentNullException.ThrowIfNull(remote);

        ct.ThrowIfCancellationRequested();

        var key = (remoteDeviceId, remote, packet.Seq);
        var completion = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );

        lock (_ackLock)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

            if (_receiveError is not null)
            {
                throw new InvalidOperationException("Receive loop has stopped", _receiveError);
            }

            if (!_pendingAcks.TryAdd(key, completion))
            {
                throw new InvalidOperationException(
                    $"Already waiting for ACK for seq {packet.Seq}"
                );
            }
        }
        try
        {
            int maxSendAttempts =
                packet.Type == MessageType.Heartbeat ? 1 : ProtocolConstants.MaxSendAttempts;

            for (int attempt = 0; attempt < maxSendAttempts; attempt++)
            {
                ct.ThrowIfCancellationRequested();

                if (attempt > 0)
                {
                    Interlocked.Increment(ref _retries);
                }

                await SendAsync(packet, remote, ct).ConfigureAwait(false);

                if (await WaitForAckAsync(completion.Task, ct).ConfigureAwait(false))
                {
                    return;
                }
            }

            throw new TimeoutException($"No ACK received for seq {packet.Seq}");
        }
        finally
        {
            _pendingAcks.TryRemove(key, out _);
        }
    }

    private async Task<bool> WaitForAckAsync(Task<bool> ackTask, CancellationToken ct)
    {
        try
        {
            return await ackTask.WaitAsync(ProtocolConstants.AckTimeout, ct).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        try
        {
            _receiveCts.Cancel();
            _receiveTask.GetAwaiter().GetResult();
        }
        finally
        {
            _socket.Dispose();
            _receiveCts.Dispose();
        }
    }

    private bool ShouldDropPacket()
    {
        return Random.Shared.Next(100) < LossRate;
    }
}
