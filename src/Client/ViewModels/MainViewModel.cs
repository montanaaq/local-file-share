using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Protocol;

namespace Client.ViewModels;

public partial class MainViewModel : ObservableObject
{
    private PeerService? _peerService;
    private readonly Guid _deviceId = Guid.NewGuid();
    private readonly HashSet<IPEndPoint> _manualEndpoints = [];
    private uint _nextSeq;

    public SettingsViewModel Settings { get; }
    public TextViewModel TextExchange { get; } = new();
    public TransfersViewModel FileTransfers { get; } = new();

    [ObservableProperty]
    public partial string NetworkMessage { get; set; } = "Обмен не запущен";

    [ObservableProperty]
    public partial string ManualAddress { get; set; } = string.Empty;

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(HasManualAddressError))]
    public partial string ManualAddressError { get; set; } = string.Empty;

    partial void OnManualAddressChanged(string value) => ManualAddressError = string.Empty;

    [ObservableProperty]
    public partial bool IsDetailsVisible { get; set; }

    [ObservableProperty]
    public partial TransportStatistics Statistics { get; set; } = new(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    [ObservableProperty]
    public partial int PauseCount { get; set; }

    [ObservableProperty]
    public partial int ResumeCount { get; set; }

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(IsHome))]
    [NotifyPropertyChangedFor(nameof(IsSettings))]
    [NotifyPropertyChangedFor(nameof(IsTransfers))]
    public partial int SelectedPage { get; set; }

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(HasSelectedDevice))]
    public partial PeerItem? SelectedDevice { get; set; }

    partial void OnSelectedDeviceChanged(PeerItem? value)
    {
        TextExchange.Recipient = value;
        FileTransfers.Recipient = value;
        OnPropertyChanged(nameof(RecipientMessage));
        OnPropertyChanged(nameof(HasRecipientMessage));
    }

    public ObservableCollection<PeerItem> Devices { get; } = [];
    public bool HasManualAddressError => ManualAddressError.Length > 0;
    public bool HasRecipientMessage => RecipientMessage.Length > 0;
    public bool IsNetworkActive => _peerService is { IsExchangeEnabled: true };
    public bool IsConnectionInterrupted => IsNetworkActive && Settings.LossRate == 100;
    public string SessionStatus =>
        _peerService is null ? "Сессия остановлена"
        : !IsNetworkActive ? "Обмен выключен"
        : IsConnectionInterrupted ? "Имитация обрыва"
        : "Обмен включён";
    public string RecipientMessage =>
        SelectedDevice is null ? string.Empty
        : !IsNetworkActive ? "Включите обмен, чтобы отправить файл или текст."
        : IsConnectionInterrupted
            ? "Выключите имитацию обрыва в настройках, чтобы продолжить отправку."
        : !SelectedDevice.IsAvailable
            ? "Нет связи с получателем. Отправка станет доступна после его обнаружения."
        : string.Empty;
    public bool IsHome => SelectedPage == 0;
    public bool IsTransfers => SelectedPage == 1;
    public bool IsSettings => SelectedPage == 2;
    public bool HasDevices => Devices.Count > 0;
    public bool HasSelectedDevice => SelectedDevice is not null;

    public MainViewModel()
        : this("default") { }

    public MainViewModel(string profile)
    {
        Settings = new SettingsViewModel(profile);
        Settings.PropertyChanged += OnNetworkSettingsChanged;
        FileTransfers.PropertyChanged += OnTransferMessageChanged;
    }

    private void OnTransferMessageChanged(object? sender, PropertyChangedEventArgs e)
    {
        if (e.PropertyName == nameof(TransfersViewModel.NetworkMessage))
        {
            NetworkMessage = FileTransfers.NetworkMessage;
        }
    }

    private void OnNetworkSettingsChanged(object? sender, PropertyChangedEventArgs e)
    {
        if (e.PropertyName == nameof(SettingsViewModel.LossRate))
        {
            if (_peerService is { } service && !StartSearchCommand.IsCancellationRequested)
            {
                service.LossRate = Settings.LossRate;
            }
            RefreshNetworkState();
        }
        else if (e.PropertyName == nameof(SettingsViewModel.IsExchangeEnabled))
        {
            if (_peerService is { } service && !StartSearchCommand.IsCancellationRequested)
            {
                service.SetExchangeEnabled(Settings.IsExchangeEnabled);
            }
            RefreshNetworkState();
            NetworkMessage = Settings.IsExchangeEnabled
                ? "Обмен включён."
                : "Обмен выключен. Незавершённые передачи приостановлены.";
        }
    }

    private void RefreshNetworkState()
    {
        OnPropertyChanged(nameof(IsNetworkActive));
        OnPropertyChanged(nameof(IsConnectionInterrupted));
        OnPropertyChanged(nameof(SessionStatus));
        OnPropertyChanged(nameof(RecipientMessage));
        OnPropertyChanged(nameof(HasRecipientMessage));
        FileTransfers.SendFileCommand.NotifyCanExecuteChanged();
        TextExchange.SendTextCommand.NotifyCanExecuteChanged();
        FileTransfers.AcceptIncomingFileCommand.NotifyCanExecuteChanged();
        FileTransfers.RejectIncomingFileCommand.NotifyCanExecuteChanged();
    }

    [RelayCommand]
    private void ChangeRecipient() => SelectedDevice = null;

    [RelayCommand]
    private void AddManualAddress()
    {
        if (
            !IPEndPoint.TryParse(ManualAddress.Trim(), out var endpoint)
            || endpoint.AddressFamily != AddressFamily.InterNetwork
            || endpoint.Port == 0
        )
        {
            ManualAddressError = "Укажите IPv4-адрес и порт, например 192.168.1.10:5000.";
            return;
        }

        _manualEndpoints.Add(endpoint);
        if (_peerService is { } service && !StartSearchCommand.IsCancellationRequested)
        {
            service.AddManualEndpoint(endpoint);
            NetworkMessage = $"Адрес {endpoint} добавлен для поиска устройства.";
        }
        else
        {
            NetworkMessage = $"Адрес {endpoint} сохранён до запуска сессии.";
        }
        ManualAddress = string.Empty;
    }

    [RelayCommand]
    private void ToggleDetails() => IsDetailsVisible = !IsDetailsVisible;

    private bool RefreshStatistics()
    {
        var service = _peerService;
        if (service is null)
        {
            return false;
        }

        Statistics = service.GetStatistics();
        var transfers = service.Transfers.GetTransfers();
        PauseCount = transfers.Sum(transfer => transfer.PauseCount);
        ResumeCount = transfers.Sum(transfer => transfer.ResumeCount);
        return true;
    }

    public async Task StopAsync()
    {
        StartSearchCommand.Cancel();

        if (StartSearchCommand.ExecutionTask is { } sessionTask)
        {
            await sessionTask;
        }
    }

    /// <summary>
    /// Открывает сетевую сессию с сохранёнными deviceId и seq, подключает модели UI и обновление статистики.
    /// При остановке освобождает подписки и сохраняет следующий seq для повторного запуска.
    /// </summary>
    /// <param name="ct">Токен остановки сетевой сессии и её фоновых операций.</param>
    [RelayCommand(IncludeCancelCommand = true)]
    private async Task StartSearchAsync(CancellationToken ct)
    {
        if (_peerService is not null)
        {
            return;
        }

        if (!Settings.Validate())
        {
            NetworkMessage = "Исправьте имя или порт в настройках.";
            SelectedPage = 2;
            return;
        }

        try
        {
            await using var peerService = new PeerService(
                Settings.ClientSettings.DeviceName,
                Settings.ClientSettings.LocalPort,
                deviceId: _deviceId,
                initialSeq: _nextSeq
            );

            _peerService = peerService;
            peerService.LossRate = Settings.LossRate;
            peerService.SetExchangeEnabled(Settings.IsExchangeEnabled);
            RefreshNetworkState();

            SelectedDevice = null;

            foreach (
                var endpoint in _manualEndpoints.Concat(Devices.Select(device => device.Endpoint))
            )
            {
                peerService.AddManualEndpoint(endpoint);
            }

            Action<Peer> peerChanged = peer => OnPeerChanged(peerService, peer);
            peerService.PeerChanged += peerChanged;

            try
            {
                TextExchange.SetSession(peerService, ct);
                FileTransfers.SetSession(peerService, ct);
                RefreshStatistics();
                using var statisticsRefresh = DispatcherTimer.Run(
                    RefreshStatistics,
                    TimeSpan.FromSeconds(1),
                    DispatcherPriority.Background
                );
                NetworkMessage = peerService.IsExchangeEnabled
                    ? $"Обмен запущен на порту {peerService.LocalPort}."
                    : $"Сессия запущена на порту {peerService.LocalPort}. Обмен выключен.";
                await peerService.RunAsync(ct);
            }
            finally
            {
                RefreshStatistics();
                peerService.PeerChanged -= peerChanged;
                TextExchange.SetSession(null, ct);
                FileTransfers.SetSession(null, ct);
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
            if (_peerService is not null)
            {
                _nextSeq = _peerService.NextSequence;
            }
            _peerService = null;
            foreach (var device in Devices)
            {
                device.Snapshot = device.Snapshot with { Availability = PeerAvailability.Offline };
            }
            RefreshNetworkState();
            SelectedDevice = null;
        }
    }

    private void OnPeerChanged(PeerService service, Peer peer)
    {
        Dispatcher.UIThread.Post(() =>
        {
            if (!ReferenceEquals(service, _peerService))
            {
                return;
            }
            for (int i = 0; i < Devices.Count; i++)
            {
                if (Devices[i].DeviceId == peer.DeviceId)
                {
                    Devices[i].Snapshot = peer;
                    if (SelectedDevice?.DeviceId == peer.DeviceId)
                    {
                        TextExchange.SendTextCommand.NotifyCanExecuteChanged();
                        FileTransfers.SendFileCommand.NotifyCanExecuteChanged();
                        OnPropertyChanged(nameof(RecipientMessage));
                        OnPropertyChanged(nameof(HasRecipientMessage));
                    }
                    return;
                }
            }

            Devices.Add(new PeerItem(peer));
            OnPropertyChanged(nameof(HasDevices));
        });
    }

    [RelayCommand]
    private void ShowHome() => SelectedPage = 0;

    [RelayCommand]
    private void ShowTransfers() => SelectedPage = 1;

    [RelayCommand]
    private void ShowSettings() => SelectedPage = 2;
}
