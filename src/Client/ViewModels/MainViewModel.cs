using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Models;
using Protocol;

namespace Client.ViewModels;

public partial class MainViewModel : ViewModelBase
{
    private readonly SettingsStorage _settingsStorage;

    private PeerService? _peerService;
    private CancellationTokenSource? _sessionCancellation;

    [ObservableProperty]
    public partial ClientSettings ClientSettings { get; set; } = new();

    [ObservableProperty]
    public partial string SettingsMessage { get; set; } = string.Empty;

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
            SettingsMessage = $"Could not load settings: {exception.Message}";
        }
    }

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(IsHome))]
    public partial bool IsSettings { get; set; }

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(HasSelectedDevice))]
    public partial Peer? SelectedDevice { get; set; }

    public bool IsHome => !IsSettings;

    public bool HasSelectedDevice => SelectedDevice is not null;

    public ObservableCollection<Peer> Devices { get; } = [];

    [RelayCommand]
    private void StartSearch()
    {
        if (_peerService is not null)
        {
            return;
        }

        _sessionCancellation = new CancellationTokenSource();
        _peerService = new PeerService(ClientSettings.DeviceName);
    }

    [RelayCommand]
    private void SaveSettings()
    {
        if (
            string.IsNullOrWhiteSpace(ClientSettings.DeviceName)
            || Encoding.UTF8.GetByteCount(ClientSettings.DeviceName) > 255
        )
        {
            SettingsMessage = "Device name must be nonempty and at most 255 UTF-8 bytes.";
            return;
        }

        if (ClientSettings.LocalPort is < 1 or > 65535)
        {
            SettingsMessage = "Port must be between 1 and 65535.";
            return;
        }

        try
        {
            _settingsStorage.Save(ClientSettings);
            SettingsMessage = "Settings saved.";
        }
        catch (Exception exception)
            when (exception is IOException or UnauthorizedAccessException)
        {
            SettingsMessage = $"Could not save settings: {exception.Message}";
        }
    }

    [RelayCommand]
    private void ShowHome() => IsSettings = false;

    [RelayCommand]
    private void ShowSettings() => IsSettings = true;
}
