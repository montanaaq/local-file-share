using System.IO;
using System.Threading.Tasks;
using Avalonia.Controls;
using Avalonia.Platform.Storage;
using Client.ViewModels;

namespace Client.Views;

public partial class MainWindow : Window
{
    private bool _closeInProgress;
    private bool _canClose;

    public MainWindow()
    {
        InitializeComponent();

        Closing += OnClosing;
    }

    public async Task<string?> PickReceiveDirectoryAsync()
    {
        using var downloads = await StorageProvider.TryGetWellKnownFolderAsync(
            WellKnownFolder.Downloads
        );
        string? downloadsPath = downloads?.TryGetLocalPath();
        if (downloadsPath is not null)
        {
            return downloadsPath;
        }

        if (!StorageProvider.CanPickFolder)
        {
            throw new System.IO.IOException("Folder selection is unavailable on this platform.");
        }

        var folders = await StorageProvider.OpenFolderPickerAsync(
            new FolderPickerOpenOptions
            {
                Title = "Выберите папку для входящего файла",
                AllowMultiple = false,
            }
        );
        if (folders.Count == 0)
        {
            return null;
        }

        using var folder = folders[0];
        return folder.TryGetLocalPath()
            ?? throw new System.IO.IOException("A local folder is required.");
    }

    public async Task<string?> PickFileAsync()
    {
        if (!StorageProvider.CanOpen)
        {
            throw new IOException("Отсутствует доступ к открытию файлов");
        }

        var files = await StorageProvider.OpenFilePickerAsync(
            new FilePickerOpenOptions
            {
                Title = "Выберите файл для отправки",
                AllowMultiple = false,
            }
        );

        if (files.Count == 0)
        {
            return null;
        }

        using var file = files[0];
        return file.TryGetLocalPath()
            ?? throw new IOException("Необходимо выбрать файл для отправки");
    }

    private async void OnClosing(object? sender, WindowClosingEventArgs e)
    {
        if (_canClose)
        {
            return;
        }

        e.Cancel = true;

        if (_closeInProgress)
        {
            return;
        }
        _closeInProgress = true;
        IsEnabled = false;

        if (DataContext is MainViewModel viewModel)
        {
            await viewModel.StopAsync();
        }

        _canClose = true;
        Close();
    }
}
