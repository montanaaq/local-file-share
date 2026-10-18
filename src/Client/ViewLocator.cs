using System;
using System.Diagnostics.CodeAnalysis;
using Avalonia.Controls;
using Avalonia.Controls.Templates;
using ViewModels;

namespace Client;

/// <summary>
/// Finds a view for the supplied view model.
/// </summary>
[RequiresUnreferencedCode(
    "ViewLocator uses reflection; required types may be removed by trimming.",
    Url = "https://docs.avaloniaui.net/docs/concepts/view-locator"
)]
public class ViewLocator : IDataTemplate
{
    public Control? Build(object? param)
    {
        if (param is null)
        {
            return null;
        }

        string name = param
            .GetType()
            .FullName!.Replace("ViewModel", "View", StringComparison.Ordinal);
        var type = Type.GetType(name);

        if (type != null)
        {
            return (Control)Activator.CreateInstance(type)!;
        }

        return new TextBlock { Text = "View not found: " + name };
    }

    public bool Match(object? data)
    {
        return data is ViewModelBase;
    }
}
