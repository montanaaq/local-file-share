using System;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Protocol;

namespace Client.ViewModels;

public partial class MainViewModel : ViewModelBase
{
    private PeerService? _peerService;

    public SettingsViewModel Settings { get; }
    public TextViewModel TextExchange { get; } = new();
    public TransfersViewModel FileTransfers { get; } = new();

    [ObservableProperty]
    public partial string NetworkMessage { get; set; } = "Обмен не запущен";

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
    }

    public ObservableCollection<PeerItem> Devices { get; } = [];
    public bool IsNetworkActive => _peerService is { IsExchangeEnabled: true };
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
            if (_peerService is { } service)
            {
                service.LossRate = Settings.LossRate;
            }
        }
        else if (e.PropertyName == nameof(SettingsViewModel.IsExchangeEnabled))
        {
            _peerService?.SetExchangeEnabled(Settings.IsExchangeEnabled);
            OnPropertyChanged(nameof(IsNetworkActive));
            FileTransfers.SendFileCommand.NotifyCanExecuteChanged();
            TextExchange.SendTextCommand.NotifyCanExecuteChanged();
            FileTransfers.AnswerIncomingFileCommand.NotifyCanExecuteChanged();
            NetworkMessage = Settings.IsExchangeEnabled
                ? "Обмен включён."
                : "Обмен выключен. Незавершённые передачи приостановлены.";
        }
    }

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

        try
        {
            Settings.ClientSettings.Validate();
        }
        catch (ArgumentException exception)
        {
            NetworkMessage = exception.Message;
            return;
        }

        try
        {
            await using var peerService = new PeerService(
                Settings.ClientSettings.DeviceName,
                Settings.ClientSettings.LocalPort
            );

            _peerService = peerService;
            peerService.LossRate = Settings.LossRate;
            peerService.SetExchangeEnabled(Settings.IsExchangeEnabled);
            OnPropertyChanged(nameof(IsNetworkActive));

            Devices.Clear();

            OnPropertyChanged(nameof(HasDevices));
            SelectedDevice = null;

            peerService.PeerChanged += OnPeerChanged;

            try
            {
                TextExchange.SetSession(peerService, ct);
                FileTransfers.SetSession(peerService, ct);
                NetworkMessage = peerService.IsExchangeEnabled
                    ? $"Обмен запущен на порту {peerService.LocalPort}."
                    : $"Сессия запущена на порту {peerService.LocalPort}. Обмен выключен.";
                await peerService.RunAsync(ct);
            }
            finally
            {
                peerService.PeerChanged -= OnPeerChanged;
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
            _peerService = null;
            OnPropertyChanged(nameof(IsNetworkActive));
            SelectedDevice = null;
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
                    Devices[i].Snapshot = peer;
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
