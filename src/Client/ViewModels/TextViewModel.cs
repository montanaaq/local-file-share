using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Protocol;
using Protocol.Peers;

namespace Client.ViewModels;

public partial class TextViewModel : ObservableObject
{
    private PeerService? _peerService;
    private CancellationToken _sessionToken;

    public Func<Task<string?>>? ReadClipboardTextAsync { get; set; }
    public Func<string, Task>? WriteClipboardTextAsync { get; set; }

    [ObservableProperty]
    [NotifyCanExecuteChangedFor(nameof(SendTextCommand))]
    public partial PeerItem? Recipient { get; set; }

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(TextByteCount))]
    [NotifyPropertyChangedFor(nameof(TextValidationMessage))]
    [NotifyCanExecuteChangedFor(nameof(SendTextCommand))]
    public partial string TextToSend { get; set; } = string.Empty;

    [ObservableProperty]
    public partial string TextMessage { get; set; } = string.Empty;

    public int TextByteCount => Encoding.UTF8.GetByteCount(TextToSend);

    public string TextValidationMessage =>
        TextByteCount > ProtocolConstants.ClipboardTextSize
            ? "Текст превышает ограничение 1000 байт в UTF-8."
            : string.Empty;

    public ObservableCollection<ReceivedText> ReceivedTexts { get; } = [];

    /// <summary>
    /// Меняет сессию отправки текста и подписку на входящие сообщения.
    /// </summary>
    /// <param name="service">Текущая сессия; null отключает получение текста.</param>
    /// <param name="token">Токен времени жизни сессии для отправки.</param>
    public void SetSession(PeerService? service, CancellationToken token = default)
    {
        _peerService?.TextReceived -= OnTextReceived;

        _peerService = service;
        _sessionToken = token;

        _peerService?.TextReceived += OnTextReceived;

        SendTextCommand.NotifyCanExecuteChanged();
    }

    [RelayCommand]
    private async Task PasteTextAsync()
    {
        if (ReadClipboardTextAsync is null)
        {
            TextMessage = "Буфер обмена недоступен.";
            return;
        }

        try
        {
            string? text = await ReadClipboardTextAsync();

            if (string.IsNullOrEmpty(text))
            {
                TextMessage = "В буфере обмена нет текста.";
                return;
            }

            TextToSend = text;
            TextMessage = "Текст вставлен. Проверьте его перед отправкой.";
        }
        catch (Exception exception)
            when (exception
                    is IOException
                        or ExternalException
                        or NotSupportedException
                        or InvalidOperationException
            )
        {
            TextMessage = $"Не удалось прочитать буфер обмена: {exception.Message}";
        }
    }

    private bool CanSendText() =>
        _peerService is not null
        && !_sessionToken.IsCancellationRequested
        && _peerService.IsExchangeEnabled
        && _peerService.LossRate < 100
        && Recipient is not null
        && Recipient.Snapshot.Availability == PeerAvailability.Available
        && TextByteCount is > 0 and <= ProtocolConstants.ClipboardTextSize;

    /// <summary>
    /// Отправляет показанный в поле текст выбранному устройству и ожидает ACK.
    /// Получатель и токен фиксируются до начала ожидания.
    /// </summary>
    [RelayCommand(CanExecute = nameof(CanSendText))]
    private async Task SendTextAsync()
    {
        var service = _peerService;
        var recipient = Recipient;
        var token = _sessionToken;
        string text = TextToSend;

        if (service is null || recipient is null || !CanSendText())
        {
            return;
        }

        try
        {
            TextMessage = $"Отправляем текст устройству «{recipient.Name}»…";
            await service.SendTextAsync(recipient.DeviceId, text, token);
            TextMessage = $"Текст доставлен устройству «{recipient.Name}».";
        }
        catch (OperationCanceledException exception)
            when (token.IsCancellationRequested
                || exception.CancellationToken.IsCancellationRequested
            )
        {
            TextMessage = "Отправка текста остановлена.";
        }
        catch (Exception exception)
            when (exception is SocketException or TimeoutException or EncoderFallbackException)
        {
            TextMessage = $"Не удалось отправить текст: {exception.Message}";
        }
    }

    private void OnTextReceived(Guid senderDeviceId, string text)
    {
        var service = _peerService;

        if (service is null)
        {
            return;
        }

        string name =
            service.GetPeers().FirstOrDefault(peer => peer.DeviceId == senderDeviceId)?.Name
            ?? "Неизвестное устройство";

        Dispatcher.UIThread.Post(() =>
        {
            if (ReferenceEquals(service, _peerService))
            {
                ReceivedTexts.Add(new ReceivedText(name, text));
            }
        });
    }

    [RelayCommand]
    private async Task CopyReceivedTextAsync(ReceivedText? message)
    {
        if (message is null)
        {
            return;
        }

        if (WriteClipboardTextAsync is null)
        {
            TextMessage = "Буфер обмена недоступен.";
            return;
        }

        try
        {
            await WriteClipboardTextAsync(message.Text);
            TextMessage = "Полученный текст скопирован.";
        }
        catch (Exception exception)
            when (exception
                    is IOException
                        or ExternalException
                        or NotSupportedException
                        or InvalidOperationException
            )
        {
            TextMessage = $"Не удалось скопировать текст: {exception.Message}";
        }
    }
}
