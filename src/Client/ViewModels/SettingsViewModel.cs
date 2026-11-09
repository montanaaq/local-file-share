using System;
using System.IO;
using System.Text.Json;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Models;

namespace Client.ViewModels;

public partial class SettingsViewModel : ViewModelBase
{
    private readonly SettingsStorage _settingsStorage;

    public ClientSettings ClientSettings { get; }

    [ObservableProperty]
    public partial string SettingsMessage { get; set; } = string.Empty;

    public int LossRate
    {
        get => IsConnectionBroken ? 100 : ClientSettings.LossRate;
        set
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(value, 100);
            if (IsConnectionBroken)
            {
                return;
            }

            ClientSettings.LossRate = value;
            OnPropertyChanged();
        }
    }

    [ObservableProperty]
    public partial bool IsConnectionBroken { get; set; }

    partial void OnIsConnectionBrokenChanged(bool value) => OnPropertyChanged(nameof(LossRate));

    public bool IsExchangeEnabled
    {
        get => ClientSettings.IsExchangeEnabled;
        set
        {
            ClientSettings.IsExchangeEnabled = value;
            OnPropertyChanged();
        }
    }

    public SettingsViewModel(string profile)
    {
        _settingsStorage = new SettingsStorage(profile);
        try
        {
            ClientSettings = _settingsStorage.Load();
        }
        catch (Exception exception)
            when (exception
                    is IOException
                        or UnauthorizedAccessException
                        or JsonException
                        or ArgumentException
            )
        {
            ClientSettings = new ClientSettings();
            SettingsMessage = $"Не удалось загрузить настройки: {exception.Message}";
        }
    }

    [RelayCommand]
    private void SaveSettings()
    {
        try
        {
            _settingsStorage.Save(ClientSettings);
            SettingsMessage = "Настройки сохранены.";
        }
        catch (Exception exception)
            when (exception is IOException or UnauthorizedAccessException or ArgumentException)
        {
            SettingsMessage = $"Не удалось сохранить настройки: {exception.Message}";
        }
    }
}
