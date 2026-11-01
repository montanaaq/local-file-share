using System;
using System.Collections.ObjectModel;
using System.IO;
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
    public partial Peer? SelectedDevice { get; set; }

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(HasIncomingFile))]
    [NotifyCanExecuteChangedFor(nameof(AnswerIncomingFileCommand))]
    public partial FileTransfer? SelectedIncomingFile { get; set; }

    public ObservableCollection<Peer> Devices { get; } = [];

    public ObservableCollection<FileTransfer> IncomingFiles { get; } = [];

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
            IncomingFiles.Clear();

            OnPropertyChanged(nameof(HasDevices));
            SelectedDevice = null;
            SelectedIncomingFile = null;

            peerService.PeerChanged += OnPeerChanged;
            peerService.Transfers.FileOffered += OnFileOffered;
            peerService.Transfers.TransferChanged += OnIncomingTransferChanged;

            try
            {
                NetworkMessage = $"Обмен запущен на порту {peerService.LocalPort}";
                await peerService.RunAsync(ct);
            }
            finally
            {
                peerService.PeerChanged -= OnPeerChanged;
                peerService.Transfers.FileOffered -= OnFileOffered;
                peerService.Transfers.TransferChanged -= OnIncomingTransferChanged;
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
        catch (Exception exception) when (
            exception is IOException or UnauthorizedAccessException or SocketException or TimeoutException
        )
        {
            NetworkMessage = $"Не удалось передать решение по файлу: {exception.Message}";
        }
    }

    private void OnIncomingTransferChanged(FileTransfer transfer)
    {
        if (transfer.Direction != TransferDirection.Incoming || transfer.State == TransferState.WaitingForAnswer)
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
        var service = _peerService;
        Dispatcher.UIThread.Post(() =>
        {
            if (service is null || !ReferenceEquals(service, _peerService)
                || service.Transfers.GetTransfer(transfer.Key).State != TransferState.WaitingForAnswer)
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

            IncomingFiles.Add(transfer);
            SelectedIncomingFile ??= transfer;
        });
    }
}
