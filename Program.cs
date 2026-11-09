using System;

namespace LocalFileShare;

sealed class Program
{
    [STAThread]
    public static void Main(string[] args) => Client.Program.Main(args);
}
