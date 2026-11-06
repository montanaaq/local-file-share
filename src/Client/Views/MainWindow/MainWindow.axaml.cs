using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Avalonia.Input;
using Avalonia.Input.Platform;
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

    private void OnFileDragOver(object? sender, DragEventArgs e)
    {
        bool allowed = DataContext is MainViewModel vm
            && vm.SelectedDevice is not null
            && vm.StartSearchCommand.IsRunning
            && !vm.StartSearchCommand.IsCancellationRequested
            && e.DataTransfer.Contains(DataFormat.File);
        e.DragEffects = allowed ? DragDropEffects.Copy : DragDropEffects.None;
        if (sender is Control target)
        {
            target.Classes.Set("dragover", allowed);
        }
        e.Handled = true;
    }

    private void OnFileDragLeave(object? sender, DragEventArgs e)
    {
        if (sender is Control target)
        {
            target.Classes.Remove("dragover");
        }
    }

    private void OnFileDrop(object? sender, DragEventArgs e)
    {
        OnFileDragLeave(sender, e);
        e.Handled = true;
        e.DragEffects = DragDropEffects.None;
        if (DataContext is not MainViewModel vm)
        {
            return;
        }

        if (vm.SelectedDevice is null || !vm.StartSearchCommand.IsRunning
            || vm.StartSearchCommand.IsCancellationRequested)
        {
            vm.NetworkMessage = "Сначала запустите обмен и выберите получателя.";
            return;
        }

        var items = e.DataTransfer.TryGetFiles()?.Take(2).ToArray();
        if (items is null || items.Length == 0)
        {
            vm.NetworkMessage = "Перетащите один локальный файл.";
            return;
        }
        if (items.Length > 1)
        {
            vm.NetworkMessage = "Пока можно выбрать только один файл за раз.";
            return;
        }
        if (items[0] is not IStorageFile file || file.TryGetLocalPath() is not { } path)
        {
            vm.NetworkMessage = "Нужен локальный файл. Папки не поддерживаются.";
            return;
        }

        if (vm.SelectFile(path))
        {
            e.DragEffects = DragDropEffects.Copy;
        }
    }

    public Task<string?> ReadClipboardTextAsync() =>
        (Clipboard ?? throw new IOException("Clipboard is unavailable.")).TryGetTextAsync();

    public Task WriteClipboardTextAsync(string text) =>
        (Clipboard ?? throw new IOException("Clipboard is unavailable.")).SetTextAsync(text);

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
