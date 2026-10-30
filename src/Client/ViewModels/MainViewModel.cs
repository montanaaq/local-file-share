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

    public bool IsHome => SelectedPage == 0;

    public bool IsTransfers => SelectedPage == 1;

    public bool IsSettings => SelectedPage == 2;

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
            SettingsMessage = "Имя устройства не должно быть пустым и должно занимать не более 255 байт в UTF-8.";
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
        catch (Exception exception)
            when (exception is IOException or UnauthorizedAccessException)
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
}
