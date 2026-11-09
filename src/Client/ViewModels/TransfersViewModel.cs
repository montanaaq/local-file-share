using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Protocol;

namespace Client.ViewModels;

public partial class TransfersViewModel : ViewModelBase
{
    private PeerService? _peerService;
    private CancellationToken _sessionToken;
    private IDisposable? _pauseRefresh;

    public Func<Task<string?>>? PickReceiveDirectoryAsync { get; set; }
    public Func<Task<string?>>? PickFileAsync { get; set; }

    [ObservableProperty]
    [NotifyCanExecuteChangedFor(nameof(SendFileCommand))]
    public partial PeerItem? Recipient { get; set; }

    [ObservableProperty]
    [NotifyCanExecuteChangedFor(nameof(SendFileCommand))]
    public partial string? SelectedFilePath { get; set; }

    [ObservableProperty]
    public partial string NetworkMessage { get; set; } = string.Empty;

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(HasIncomingFile))]
    [NotifyCanExecuteChangedFor(nameof(AnswerIncomingFileCommand))]
    public partial IncomingFileOffer? SelectedIncomingFile { get; set; }

    public ObservableCollection<IncomingFileOffer> IncomingFiles { get; } = [];
    public ObservableCollection<TransferItem> Transfers { get; } = [];
    public bool HasIncomingFile => SelectedIncomingFile is not null;
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
            _peerService.Transfers.FileOffered -= OnFileOffered;
            _peerService.Transfers.TransferChanged -= OnIncomingTransferChanged;
            _peerService.Transfers.TransferChanged -= OnTransferChanged;
        }

        _peerService = service;
        _sessionToken = token;
        IncomingFiles.Clear();
        SelectedIncomingFile = null;
        if (service is not null)
        {
            Transfers.Clear();
            SelectedFilePath = null;
            service.Transfers.FileOffered += OnFileOffered;
            service.Transfers.TransferChanged += OnIncomingTransferChanged;
            service.Transfers.TransferChanged += OnTransferChanged;
            _pauseRefresh = DispatcherTimer.Run(
                RefreshPausedTransfers,
                TimeSpan.FromSeconds(1),
                DispatcherPriority.Background
            );
        }

        SendFileCommand.NotifyCanExecuteChanged();
        AnswerIncomingFileCommand.NotifyCanExecuteChanged();
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

    private bool CanAnswerIncomingFile(bool accepted) =>
        _peerService is not null
        && _peerService.IsExchangeEnabled
        && !_sessionToken.IsCancellationRequested
        && SelectedIncomingFile is not null;

    /// <summary>
    /// Передаёт решение по выбранному входящему файлу; при принятии запрашивает папку у окна.
    /// </summary>
    /// <param name="accepted">Принимает файл при true, отклоняет при false.</param>
    [RelayCommand(CanExecute = nameof(CanAnswerIncomingFile))]
    private async Task AnswerIncomingFileAsync(bool accepted)
    {
        var service = _peerService;
        var transfer = SelectedIncomingFile;
        var token = _sessionToken;
        if (service is null || transfer is null || token.IsCancellationRequested)
        {
            return;
        }

        try
        {
            string directory = string.Empty;
            if (accepted)
            {
                if (PickReceiveDirectoryAsync is null)
                {
                    NetworkMessage = "Выбор папки недоступен.";
                    return;
                }

                string? selectedDirectory = await PickReceiveDirectoryAsync();
                if (selectedDirectory is null)
                {
                    return;
                }

                directory = selectedDirectory;
            }

            token.ThrowIfCancellationRequested();
            if (!ReferenceEquals(service, _peerService))
            {
                return;
            }

            if (!service.IsExchangeEnabled)
            {
                NetworkMessage = "Включите обмен, прежде чем принять или отклонить файл.";
                return;
            }

            await service.AnswerFileOfferAsync(transfer.Key, accepted, directory, token);
            IncomingFiles.Remove(transfer);
            if (SelectedIncomingFile?.Key == transfer.Key)
            {
                SelectedIncomingFile = IncomingFiles.Count > 0 ? IncomingFiles[0] : null;
            }

            NetworkMessage = accepted
                ? $"Файл «{transfer.FileName}» принят. Папка: {directory}"
                : $"Файл «{transfer.FileName}» отклонён.";
        }
        catch (OperationCanceledException exception)
            when (token.IsCancellationRequested
                || exception.CancellationToken.IsCancellationRequested
            )
        {
            NetworkMessage = "Сессия остановлена, решение по файлу прервано.";
        }
        catch (Exception exception)
            when (exception
                    is IOException
                        or UnauthorizedAccessException
                        or SocketException
                        or TimeoutException
            )
        {
            NetworkMessage = $"Не удалось передать решение по файлу: {exception.Message}";
        }
    }

    private void OnIncomingTransferChanged(FileTransfer transfer)
    {
        if (
            transfer.Direction != TransferDirection.Incoming
            || transfer.State == TransferState.WaitingForAnswer
        )
        {
            return;
        }

        var service = _peerService;
        Dispatcher.UIThread.Post(() =>
        {
            if (!ReferenceEquals(service, _peerService))
            {
                return;
            }

            for (int index = 0; index < IncomingFiles.Count; index++)
            {
                if (IncomingFiles[index].Key == transfer.Key)
                {
                    IncomingFiles.RemoveAt(index);
                    if (SelectedIncomingFile?.Key == transfer.Key)
                    {
                        SelectedIncomingFile = IncomingFiles.Count > 0 ? IncomingFiles[0] : null;
                    }
                    break;
                }
            }
        });
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
            NetworkMessage = $"Не удалось открыть файл: {ex.Message}";
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

            SelectedFilePath = file.FullName;
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
            NetworkMessage = $"Не удалось выбрать файл: {exception.Message}";
            return false;
        }
    }

    /// <summary>
    /// Предлагает выбранный файл получателю и ожидает результат отправки.
    /// </summary>
    [RelayCommand(CanExecute = nameof(CanSendFile))]
    private async Task SendFileAsync()
    {
        if (!CanSendFile())
        {
            return;
        }
        try
        {
            var fileInfo = new FileInfo(SelectedFilePath!);

            if (!fileInfo.Exists)
            {
                NetworkMessage = "Выбранный файл больше не существует";
                return;
            }

            if (fileInfo.Length > ProtocolConstants.MaxFileSize)
            {
                NetworkMessage =
                    $"Размер файла не должен превышать {ProtocolConstants.MaxFileSize / 1024 / 1024} МБ";
                return;
            }

            NetworkMessage = $"Предлагаем файл «{fileInfo.Name}» устройству «{Recipient!.Name}».";

            var transfer = await _peerService!.OfferFileAsync(
                Recipient.DeviceId,
                SelectedFilePath!,
                _sessionToken
            );

            NetworkMessage = transfer.State switch
            {
                TransferState.Completed => "Файл успешно передан и сохранён получателем.",
                TransferState.Rejected => "Получатель отказался от файла.",
                TransferState.Paused => "Передача приостановлена. Ожидается восстановление связи.",
                TransferState.Failed => $"Ошибка передачи: {transfer.Error}",
                TransferState.Cancelled => "Передача отменена.",
                _ => "Операция отправки завершилась. Состояние передачи изменилось.",
            };
        }
        catch (OperationCanceledException) when (_sessionToken.IsCancellationRequested)
        {
            NetworkMessage = "Отправка прервана из-за остановки сессии";
        }
        catch (Exception ex)
            when (ex
                    is IOException
                        or UnauthorizedAccessException
                        or SocketException
                        or TimeoutException
            )
        {
            NetworkMessage = $"Не удалось отправить файл: {ex.Message}";
        }
    }

    private void OnFileOffered(FileTransfer transfer)
    {
        OnTransferChanged(transfer);

        var service = _peerService;
        Dispatcher.UIThread.Post(() =>
        {
            if (
                service is null
                || !ReferenceEquals(service, _peerService)
                || service.Transfers.GetTransfer(transfer.Key).State
                    != TransferState.WaitingForAnswer
            )
            {
                return;
            }

            foreach (var existing in IncomingFiles)
            {
                if (existing.Key == transfer.Key)
                {
                    return;
                }
            }

            string senderName =
                service
                    .GetPeers()
                    .FirstOrDefault(peer => peer.DeviceId == transfer.PeerDeviceId)
                    ?.Name
                ?? "Неизвестное устройство";
            var offer = new IncomingFileOffer(
                transfer.Key,
                transfer.FileName,
                transfer.FileSize,
                senderName
            );
            IncomingFiles.Add(offer);
            SelectedIncomingFile ??= offer;
        });
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
            && !_sessionToken.IsCancellationRequested
            && Recipient is not null
            && !string.IsNullOrWhiteSpace(SelectedFilePath);
    }
}
