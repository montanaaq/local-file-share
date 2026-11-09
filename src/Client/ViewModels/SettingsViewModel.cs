using System;
using System.IO;
using System.Text.Json;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Models;

namespace Client.ViewModels;

public partial class SettingsViewModel : ObservableObject
{
    private readonly SettingsStorage _settingsStorage;

    public ClientSettings ClientSettings { get; }

    [ObservableProperty]
    public partial string SettingsMessage { get; set; } = string.Empty;

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(HasDeviceNameError))]
    public partial string DeviceNameError { get; set; } = string.Empty;

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(HasLocalPortError))]
    public partial string LocalPortError { get; set; } = string.Empty;

    public bool HasDeviceNameError => DeviceNameError.Length > 0;
    public bool HasLocalPortError => LocalPortError.Length > 0;

    public string DeviceName
    {
        get => ClientSettings.DeviceName;
        set
        {
            ClientSettings.DeviceName = value;
            OnPropertyChanged();
            Validate();
        }
    }

    public int LocalPort
    {
        get => ClientSettings.LocalPort;
        set
        {
            ClientSettings.LocalPort = value;
            OnPropertyChanged();
            Validate();
        }
    }

    /// <summary>
    /// Проверяет настройки по правилам ClientSettings и показывает ошибку рядом с соответствующим полем.
    /// </summary>
    /// <returns>true, если настройки допустимы для сохранения и запуска сессии.</returns>
    public bool Validate()
    {
        DeviceNameError = string.Empty;
        LocalPortError = string.Empty;
        try
        {
            ClientSettings.Validate();
            return true;
        }
        catch (ArgumentException exception)
        {
            if (exception.ParamName == nameof(ClientSettings.LocalPort))
            {
                LocalPortError = "Порт должен быть от 1 до 65535.";
            }
            else if (exception.ParamName == nameof(ClientSettings.LossRate))
            {
                SettingsMessage = "Потери пакетов должны быть от 0 до 100%.";
            }
            else
            {
                DeviceNameError = "Введите имя размером от 1 до 255 байт в UTF-8.";
            }
            return false;
        }
    }

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
        if (!Validate())
        {
            SettingsMessage = "Исправьте выделенные поля перед сохранением.";
            return;
        }

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
