using System;
using System.IO;
using System.Text.Json;
using Client;

namespace Models;

public sealed class SettingsStorage
{
    private readonly string _filePath;

    public SettingsStorage(string profile)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(profile);

        foreach (char character in profile)
        {
            if (!char.IsAsciiLetterOrDigit(character) && character != '-' && character != '_')
            {
                throw new ArgumentException(
                    "Имя профиля может содержать только латинские буквы, цифры, дефис и подчёркивание.",
                    nameof(profile)
                );
            }
        }

        string directory = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "LocalFileShare"
        );
        _filePath = Path.Combine(directory, $"{profile}.json");
    }

    public ClientSettings Load()
    {
        if (!File.Exists(_filePath))
        {
            return new ClientSettings();
        }

        string json = File.ReadAllText(_filePath);

        var settings =
            JsonSerializer.Deserialize<ClientSettings>(json)
            ?? throw new InvalidDataException("Файл настроек пуст.");
        settings.Validate();

        return settings;
    }

    public void Save(ClientSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings.Validate();

        string directory =
            Path.GetDirectoryName(_filePath)
            ?? throw new InvalidOperationException("Папка настроек не найдена.");

        Directory.CreateDirectory(directory);

        string json = JsonSerializer.Serialize(
            settings,
            new JsonSerializerOptions { WriteIndented = true }
        );

        File.WriteAllText(_filePath, json);
    }
}
