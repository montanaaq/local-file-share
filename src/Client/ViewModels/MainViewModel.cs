using CommunityToolkit.Mvvm.ComponentModel;

namespace Client.ViewModels;

public partial class MainViewModel : ViewModelBase
{
    [ObservableProperty]
    public partial string Greeting { get; set; } = "Local File Share";
}
