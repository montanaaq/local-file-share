using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Protocol;
using Protocol.Peers;
using Protocol.Transfers.Models;

namespace Client.ViewModels;

public partial class TransfersViewModel : ObservableObject
{
    private PeerService? _peerService;
    private CancellationToken _sessionToken;

    private IDisposable? _pauseRefresh;

    public Func<Task<string?>>? PickFileAsync { get; set; }

    [ObservableProperty]
    [NotifyCanExecuteChangedFor(nameof(SendFileCommand))]
    public partial PeerItem? Recipient { get; set; }

    [ObservableProperty]
    [NotifyCanExecuteChangedFor(nameof(SendFileCommand))]
    [NotifyPropertyChangedFor(nameof(SelectedFileName), nameof(HasSelectedFile))]
    public partial string? SelectedFilePath { get; set; }

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(SelectedFileSizeText))]
    public partial ulong SelectedFileSize { get; set; }

    [ObservableProperty]
    public partial string NetworkMessage { get; set; } = string.Empty;

    public ObservableCollection<TransferItem> Transfers { get; } = [];

    public bool HasSelectedFile => SelectedFilePath is not null;

    public string SelectedFileName => Path.GetFileName(SelectedFilePath) ?? string.Empty;
    public string SelectedFileSizeText => SelectedFileSize.ToFileSize();

    public string FileSizeHint =>
        $"Выберите один файл до {ProtocolConstants.MaxFileSize / 1024 / 1024} МиБ";

    /// <summary>
    /// Меняет сессию файлового UI, согласуя подписки и таймер обновления паузы.
    /// </summary>
    /// <param name="service">Текущая сессия; null отключает подписки и таймер.</param>
    /// <param name="token">Токен времени жизни сессии для действий пользователя.</param>
    public void SetSession(PeerService? service, CancellationToken token = default)
    {
        _pauseRefresh?.Dispose();
        _pauseRefresh = null;

        if (_peerService is not null)
        {
            _peerService.Transfers.TransferChanged -= OnTransferChanged;
        }

        _peerService = service;
        _sessionToken = token;

        if (service is not null)
        {
            Transfers.Clear();
            ClearSelectedFile();

            service.Transfers.TransferChanged += OnTransferChanged;

            _pauseRefresh = DispatcherTimer.Run(
                RefreshPausedTransfers,
                TimeSpan.FromSeconds(1),
                DispatcherPriority.Background
            );
        }

        SendFileCommand.NotifyCanExecuteChanged();
    }

    [RelayCommand]
    private void ClearSelectedFile()
    {
        SelectedFilePath = null;
        SelectedFileSize = 0;
    }

    [RelayCommand]
    private void CancelPausedTransfer(TransferItem? item)
    {
        var service = _peerService;

        if (service is null || item is null)
        {
            return;
        }

        var current = service.Transfers.GetTransfer(item.Key);

        if (current.State != TransferState.Paused)
        {
            OnTransferChanged(current);
            NetworkMessage = "Передача уже вышла из паузы.";
            return;
        }

        try
        {
            var cancelled = service.CancelPausedTransfer(item.Key);
            OnTransferChanged(cancelled);

            NetworkMessage = $"Передача «{cancelled.FileName}» отменена.";
        }
        catch (InvalidOperationException)
        {
            OnTransferChanged(service.Transfers.GetTransfer(item.Key));
            NetworkMessage = "Состояние передачи изменилось. Повторите действие при необходимости.";
        }
    }

    [RelayCommand]
    private async Task ChooseFileAsync()
    {
        var service = _peerService;

        if (service is null || Recipient is null)
        {
            NetworkMessage = "Сначала запустите обмен и выберите получателя";
            return;
        }

        if (PickFileAsync is null)
        {
            NetworkMessage = "Выбор файла недоступен";
            return;
        }

        try
        {
            string? path = await PickFileAsync();

            if (
                path is not null
                && ReferenceEquals(service, _peerService)
                && !_sessionToken.IsCancellationRequested
            )
            {
                SelectFile(path);
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            NetworkMessage = $"Не удалось открыть файл: {ex.ToUserMessage()}";
        }
    }

    /// <summary>
    /// Проверяет локальный файл из диалога или перетаскивания и выбирает его для отправки.
    /// </summary>
    /// <param name="path">Локальный путь к файлу.</param>
    public bool SelectFile(string path)
    {
        try
        {
            var file = new FileInfo(path);

            if (!ValidateFile(file))
            {
                return false;
            }

            SelectedFilePath = file.FullName;
            SelectedFileSize = (ulong)file.Length;
            NetworkMessage = $"Выбран файл «{file.Name}». Нажмите «Отправить файл».";
            return true;
        }
        catch (Exception exception)
            when (exception
                    is IOException
                        or UnauthorizedAccessException
                        or ArgumentException
                        or NotSupportedException
            )
        {
            NetworkMessage = $"Не удалось выбрать файл: {exception.ToUserMessage()}";
            return false;
        }
    }

    private bool ValidateFile(FileInfo file)
    {
        if (!file.Exists)
        {
            NetworkMessage = "Выберите существующий файл, а не папку.";
            return false;
        }

        if (file.Length > ProtocolConstants.MaxFileSize)
        {
            NetworkMessage =
                $"Размер файла не должен превышать {ProtocolConstants.MaxFileSize / 1024 / 1024} МБ.";
            return false;
        }

        return true;
    }

    /// <summary>
    /// Предлагает выбранный файл получателю и ожидает результат отправки.
    /// </summary>
    [RelayCommand(CanExecute = nameof(CanSendFile))]
    private async Task SendFileAsync()
    {
        var service = _peerService;
        var recipient = Recipient;
        var token = _sessionToken;
        string? path = SelectedFilePath;

        if (service is null || recipient is null || path is null || !CanSendFile())
        {
            return;
        }

        try
        {
            var fileInfo = new FileInfo(path);

            if (!ValidateFile(fileInfo))
            {
                return;
            }

            NetworkMessage = $"Предлагаем файл «{fileInfo.Name}» устройству «{recipient.Name}».";

            var transfer = await service.OfferFileAsync(recipient.DeviceId, path, token);

            NetworkMessage = transfer.State switch
            {
                TransferState.Completed => "Файл успешно передан и сохранён получателем.",
                TransferState.Rejected => "Получатель отказался от файла.",
                TransferState.Paused => "Передача приостановлена. Ожидается восстановление связи.",
                TransferState.Failed => transfer.ErrorCode.ToUserMessage(),
                TransferState.Cancelled => "Передача отменена.",
                _ => "Операция отправки завершилась. Состояние передачи изменилось.",
            };
        }
        catch (OperationCanceledException exception)
            when (token.IsCancellationRequested
                || exception.CancellationToken.IsCancellationRequested
            )
        {
            NetworkMessage = token.IsCancellationRequested
                ? "Отправка прервана из-за остановки сессии."
                : "Подготовка файла прервана из-за выключения обмена. Повторите отправку после включения.";
        }
        catch (Exception ex)
            when (ex
                    is IOException
                        or UnauthorizedAccessException
                        or SocketException
                        or TimeoutException
            )
        {
            NetworkMessage = $"Не удалось отправить файл: {ex.ToUserMessage()}";
        }
    }

    private void OnTransferChanged(FileTransfer transfer)
    {
        var service = _peerService;

        Dispatcher.UIThread.Post(() =>
        {
            if (service is null || !ReferenceEquals(service, _peerService))
            {
                return;
            }

            var item = new TransferItem(transfer);

            for (int index = 0; index < Transfers.Count; index++)
            {
                if (Transfers[index].Key == item.Key)
                {
                    Transfers[index] = item;
                    return;
                }
            }

            Transfers.Add(item);
        });
    }

    private bool RefreshPausedTransfers()
    {
        var service = _peerService;

        if (service is null || _sessionToken.IsCancellationRequested)
        {
            return false;
        }

        for (int index = 0; index < Transfers.Count; index++)
        {
            var item = Transfers[index];

            if (!item.IsPaused)
            {
                continue;
            }

            var snapshot = service.Transfers.GetTransfer(item.Key);
            Transfers[index] = new TransferItem(snapshot);
        }

        return true;
    }

    private bool CanSendFile()
    {
        return _peerService is not null
            && _peerService.IsExchangeEnabled
            && _peerService.LossRate < 100
            && !_sessionToken.IsCancellationRequested
            && Recipient is not null
            && Recipient.Snapshot.Availability == PeerAvailability.Available
            && !string.IsNullOrWhiteSpace(SelectedFilePath);
    }
}
