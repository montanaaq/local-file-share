using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Models;
using Protocol;

namespace Client.ViewModels;

public partial class MainViewModel : ViewModelBase
{
    private readonly SettingsStorage _settingsStorage;

    private PeerService? _peerService;
    private CancellationToken _sessionToken;

    public Func<Task<string?>>? PickReceiveDirectoryAsync { get; set; }
    public Func<Task<string?>>? PickFileAsync { get; set; }

    [ObservableProperty]
    [NotifyCanExecuteChangedFor(nameof(SendFileCommand))]
    public partial string? SelectedFilePath { get; set; }

    [ObservableProperty]
    public partial ClientSettings ClientSettings { get; set; } = new();

    [ObservableProperty]
    public partial string SettingsMessage { get; set; } = string.Empty;

    [ObservableProperty]
    public partial string NetworkMessage { get; set; } = "Обмен не запущен";

    public MainViewModel()
        : this("default") { }

    public MainViewModel(string profile)
    {
        _settingsStorage = new SettingsStorage(profile);

        try
        {
            ClientSettings = _settingsStorage.Load();
        }
        catch (Exception exception)
            when (exception is IOException or UnauthorizedAccessException or JsonException)
        {
            SettingsMessage = $"Не удалось загрузить настройки: {exception.Message}";
        }
    }

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(IsHome))]
    [NotifyPropertyChangedFor(nameof(IsSettings))]
    [NotifyPropertyChangedFor(nameof(IsTransfers))]
    public partial int SelectedPage { get; set; }

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(HasSelectedDevice))]
    [NotifyCanExecuteChangedFor(nameof(SendFileCommand))]
    public partial Peer? SelectedDevice { get; set; }

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(HasIncomingFile))]
    [NotifyCanExecuteChangedFor(nameof(AnswerIncomingFileCommand))]
    public partial IncomingFileOffer? SelectedIncomingFile { get; set; }

    public ObservableCollection<Peer> Devices { get; } = [];

    public ObservableCollection<IncomingFileOffer> IncomingFiles { get; } = [];
    public ObservableCollection<TransferItem> Transfers { get; } = [];

    public bool IsHome => SelectedPage == 0;

    public bool IsTransfers => SelectedPage == 1;

    public bool IsSettings => SelectedPage == 2;

    public bool HasDevices => Devices.Count > 0;

    public bool HasSelectedDevice => SelectedDevice is not null;

    public bool HasIncomingFile => SelectedIncomingFile is not null;

    [RelayCommand]
    private void ChangeRecipient() => SelectedDevice = null;

    public async Task StopAsync()
    {
        StartSearchCommand.Cancel();

        if (StartSearchCommand.ExecutionTask is { } sessionTask)
        {
            await sessionTask;
        }
    }

    [RelayCommand(IncludeCancelCommand = true)]
    private async Task StartSearchAsync(CancellationToken ct)
    {
        if (_peerService is not null)
        {
            return;
        }

        if (
            string.IsNullOrWhiteSpace(ClientSettings.DeviceName)
            || Encoding.UTF8.GetByteCount(ClientSettings.DeviceName) > 255
        )
        {
            NetworkMessage = "Укажите непустое имя размером до 255 байт в UTF-8";
            return;
        }

        if (ClientSettings.LocalPort is < 1 or > 65535)
        {
            NetworkMessage = "Порт должен быть от 1 до 65535";
            return;
        }

        try
        {
            await using var peerService = new PeerService(
                ClientSettings.DeviceName,
                ClientSettings.LocalPort
            );

            _peerService = peerService;
            _sessionToken = ct;

            Devices.Clear();
            Transfers.Clear();
            IncomingFiles.Clear();

            OnPropertyChanged(nameof(HasDevices));
            SelectedDevice = null;
            SelectedFilePath = null;
            SelectedIncomingFile = null;

            peerService.PeerChanged += OnPeerChanged;
            peerService.Transfers.FileOffered += OnFileOffered;
            peerService.Transfers.TransferChanged += OnIncomingTransferChanged;
            peerService.Transfers.TransferChanged += OnTransferChanged;

            try
            {
                using var pauseRefresh = DispatcherTimer.Run(
                    RefreshPausedTransfers,
                    TimeSpan.FromSeconds(1),
                    DispatcherPriority.Background
                );

                NetworkMessage = $"Обмен запущен на порту {peerService.LocalPort}";
                await peerService.RunAsync(ct);
            }
            finally
            {
                peerService.PeerChanged -= OnPeerChanged;
                peerService.Transfers.FileOffered -= OnFileOffered;
                peerService.Transfers.TransferChanged -= OnIncomingTransferChanged;
                peerService.Transfers.TransferChanged -= OnTransferChanged;
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            NetworkMessage = "Обмен остановлен";
        }
        catch (SocketException socketEx)
        {
            NetworkMessage = $"Ошибка сети: {socketEx}";
        }
        finally
        {
            _peerService = null;
            SelectedDevice = null;
            IncomingFiles.Clear();
            SelectedIncomingFile = null;
            AnswerIncomingFileCommand.NotifyCanExecuteChanged();
        }
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
        catch (OperationCanceledException) when (token.IsCancellationRequested)
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
    private void SaveSettings()
    {
        if (
            string.IsNullOrWhiteSpace(ClientSettings.DeviceName)
            || Encoding.UTF8.GetByteCount(ClientSettings.DeviceName) > 255
        )
        {
            SettingsMessage =
                "Имя устройства не должно быть пустым и должно занимать не более 255 байт в UTF-8.";
            return;
        }

        if (ClientSettings.LocalPort is < 1 or > 65535)
        {
            SettingsMessage = "Порт должен быть от 1 до 65535.";
            return;
        }

        try
        {
            _settingsStorage.Save(ClientSettings);
            SettingsMessage = "Настройки сохранены.";
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            SettingsMessage = $"Не удалось сохранить настройки: {exception.Message}";
        }
    }

    [RelayCommand]
    private void ShowHome() => SelectedPage = 0;

    [RelayCommand]
    private void ShowTransfers() => SelectedPage = 1;

    [RelayCommand]
    private void ShowSettings() => SelectedPage = 2;

    [RelayCommand]
    private async Task ChooseFileAsync()
    {
        var service = _peerService;

        if (service is null || SelectedDevice is null)
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
                SelectedFilePath = path;
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            NetworkMessage = $"Не удалось открыть файл: {ex.Message}";
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

            NetworkMessage =
                $"Предлагаем файл «{fileInfo.Name}» устройству «{SelectedDevice!.Name}».";

            var transfer = await _peerService!.OfferFileAsync(
                SelectedDevice.DeviceId,
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

    private void OnPeerChanged(Peer peer)
    {
        Dispatcher.UIThread.Post(() =>
        {
            for (int i = 0; i < Devices.Count; i++)
            {
                if (Devices[i].DeviceId == peer.DeviceId)
                {
                    bool wasSelected = SelectedDevice?.DeviceId == peer.DeviceId;
                    Devices[i] = peer;

                    if (wasSelected)
                    {
                        SelectedDevice = peer;
                    }
                    return;
                }
            }

            Devices.Add(peer);
            OnPropertyChanged(nameof(HasDevices));
        });
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
            && !_sessionToken.IsCancellationRequested
            && SelectedDevice is not null
            && !string.IsNullOrWhiteSpace(SelectedFilePath);
    }
}
