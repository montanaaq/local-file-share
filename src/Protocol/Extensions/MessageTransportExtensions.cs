using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Protocol;

internal static class MessageTransportExtensions
{
    /// <summary>
    /// Создаёт новый пакет с общим seq транспорта и ожидает ACK; повторы выполняет сам транспорт.
    /// </summary>
    /// <param name="transport">Транспорт, назначающий seq и проверяющий ACK.</param>
    /// <param name="type">Тип отправляемого сообщения.</param>
    /// <param name="payload">Готовое тело сообщения с deviceId отправителя.</param>
    /// <param name="remote">Адрес и порт получателя.</param>
    /// <param name="remoteDeviceId">Ожидаемый deviceId отправителя ACK.</param>
    /// <param name="ct">Токен отмены отправки и ожидания ACK.</param>
    internal static Task SendReliableAsync(
        this IMessageTransport transport,
        MessageType type,
        byte[] payload,
        IPEndPoint remote,
        Guid remoteDeviceId,
        CancellationToken ct
    ) =>
        transport.SendReliableAsync(
            new Packet(type, transport.NextSeq(), payload),
            remote,
            remoteDeviceId,
            ct
        );
}
