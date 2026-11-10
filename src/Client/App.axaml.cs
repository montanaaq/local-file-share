using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Markup.Xaml;
using Client.ViewModels;
using Client.Views;

namespace Client;

public partial class App : Application
{
    public override void Initialize()
    {
        AvaloniaXamlLoader.Load(this);
    }

    public override void OnFrameworkInitializationCompleted()
    {
        if (ApplicationLifetime is IClassicDesktopStyleApplicationLifetime desktop)
        {
            string profile = desktop.Args is { Length: > 0 } ? desktop.Args[0] : "default";

            var window = new MainWindow();
            window.DataContext = new MainViewModel(profile)
            {
                FileTransfers = { PickFileAsync = window.PickFileAsync },
                IncomingFiles = { PickReceiveDirectoryAsync = window.PickReceiveDirectoryAsync },
                TextExchange =
                {
                    ReadClipboardTextAsync = window.ReadClipboardTextAsync,
                    WriteClipboardTextAsync = window.WriteClipboardTextAsync,
                },
            };

            desktop.MainWindow = window;
        }

        base.OnFrameworkInitializationCompleted();
    }
}
