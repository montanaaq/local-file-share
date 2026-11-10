using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Protocol.Peers;
using Protocol.Transfers.Models;

namespace Client.ViewModels;

public partial class IncomingFilesViewModel : ObservableObject
{
    private PeerService? _peerService;
    private CancellationToken _sessionToken;

    public Func<Task<string?>>? PickReceiveDirectoryAsync { get; set; }

    [ObservableProperty]
    public partial string NetworkMessage { get; set; } = string.Empty;

    [ObservableProperty]
    [NotifyCanExecuteChangedFor(
        nameof(AcceptIncomingFileCommand),
        nameof(RejectIncomingFileCommand)
    )]
    public partial bool IsAnsweringFile { get; set; }

    public ObservableCollection<IncomingFileOffer> Offers { get; } = [];

    public bool HasIncomingFile => Offers.Count > 0;

    /// <summary>
    /// Меняет сессию входящих предложений и подписки на их появление и завершение.
    /// </summary>
    /// <param name="service">Текущая сессия; null отключает получение предложений.</param>
    /// <param name="token">Токен времени жизни сессии для решений пользователя.</param>
    public void SetSession(PeerService? service, CancellationToken token = default)
    {
        if (_peerService is not null)
        {
            _peerService.Transfers.FileOffered -= OnFileOffered;
            _peerService.Transfers.TransferChanged -= OnIncomingTransferChanged;
        }

        _peerService = service;
        _sessionToken = token;
        Offers.Clear();
        NotifyIncomingFilesChanged();

        if (service is not null)
        {
            service.Transfers.FileOffered += OnFileOffered;
            service.Transfers.TransferChanged += OnIncomingTransferChanged;
        }
    }

    private void NotifyIncomingFilesChanged()
    {
        OnPropertyChanged(nameof(HasIncomingFile));
        AcceptIncomingFileCommand.NotifyCanExecuteChanged();
        RejectIncomingFileCommand.NotifyCanExecuteChanged();
    }

    private bool CanAnswerIncomingFile(IncomingFileOffer? offer) =>
        _peerService is not null
        && _peerService.IsExchangeEnabled
        && _peerService.LossRate < 100
        && !_sessionToken.IsCancellationRequested
        && !IsAnsweringFile
        && offer is not null
        && Offers.Contains(offer);

    [RelayCommand(CanExecute = nameof(CanAnswerIncomingFile))]
    private Task AcceptIncomingFileAsync(IncomingFileOffer? offer) =>
        AnswerIncomingFileAsync(offer, true);

    [RelayCommand(CanExecute = nameof(CanAnswerIncomingFile))]
    private Task RejectIncomingFileAsync(IncomingFileOffer? offer) =>
        AnswerIncomingFileAsync(offer, false);

    /// <summary>
    /// Передаёт решение по конкретному входящему файлу; при принятии запрашивает папку у окна.
    /// </summary>
    /// <param name="transfer">Предложение из карточки входящего файла.</param>
    /// <param name="accepted">Принимает файл при true, отклоняет при false.</param>
    private async Task AnswerIncomingFileAsync(IncomingFileOffer? transfer, bool accepted)
    {
        var service = _peerService;
        var token = _sessionToken;

        if (
            service is null
            || transfer is null
            || token.IsCancellationRequested
            || !Offers.Contains(transfer)
        )
        {
            return;
        }

        IsAnsweringFile = true;

        try
        {
            string directory = string.Empty;

            if (accepted)
            {
                if (PickReceiveDirectoryAsync is null)
                {
                    NetworkMessage = "Выбор папки недоступен.";
                    return;
                }

                string? selectedDirectory = await PickReceiveDirectoryAsync();

                if (selectedDirectory is null)
                {
                    return;
                }

                directory = selectedDirectory;
            }

            token.ThrowIfCancellationRequested();

            if (!ReferenceEquals(service, _peerService))
            {
                return;
            }

            if (!service.IsExchangeEnabled)
            {
                NetworkMessage = "Включите обмен, прежде чем принять или отклонить файл.";
                return;
            }

            if (service.Transfers.GetTransfer(transfer.Key).State != TransferState.WaitingForAnswer)
            {
                Offers.Remove(transfer);
                NotifyIncomingFilesChanged();
                NetworkMessage =
                    "Срок предложения файла истёк или его состояние изменилось. Попросите отправителя повторить отправку.";
                return;
            }

            await service.AnswerFileOfferAsync(transfer.Key, accepted, directory, token);

            Offers.Remove(transfer);
            NotifyIncomingFilesChanged();

            NetworkMessage = accepted
                ? $"Файл «{transfer.FileName}» принят. Папка: {directory}"
                : $"Файл «{transfer.FileName}» отклонён.";
        }
        catch (OperationCanceledException exception)
            when (token.IsCancellationRequested
                || exception.CancellationToken.IsCancellationRequested
            )
        {
            NetworkMessage = "Сессия остановлена, решение по файлу прервано.";
        }
        catch (InvalidOperationException)
            when (token.IsCancellationRequested
                || !ReferenceEquals(service, _peerService)
                || service.Transfers.GetTransfer(transfer.Key).State
                    != TransferState.WaitingForAnswer
            )
        {
            Offers.Remove(transfer);
            NotifyIncomingFilesChanged();
            NetworkMessage =
                "Предложение файла больше не доступно. Попросите отправителя повторить отправку.";
        }
        catch (Exception exception)
            when (exception
                    is IOException
                        or UnauthorizedAccessException
                        or SocketException
                        or TimeoutException
            )
        {
            NetworkMessage = $"Не удалось передать решение по файлу: {exception.ToUserMessage()}";
        }
        finally
        {
            IsAnsweringFile = false;
        }
    }

    private void OnIncomingTransferChanged(FileTransfer transfer)
    {
        if (
            transfer.Direction != TransferDirection.Incoming
            || transfer.State == TransferState.WaitingForAnswer
        )
        {
            return;
        }

        var service = _peerService;
        Dispatcher.UIThread.Post(() =>
        {
            if (!ReferenceEquals(service, _peerService))
            {
                return;
            }

            for (int index = 0; index < Offers.Count; index++)
            {
                if (Offers[index].Key == transfer.Key)
                {
                    Offers.RemoveAt(index);
                    NotifyIncomingFilesChanged();
                    break;
                }
            }
        });
    }

    private void OnFileOffered(FileTransfer transfer)
    {
        var service = _peerService;
        Dispatcher.UIThread.Post(() =>
        {
            if (
                service is null
                || !ReferenceEquals(service, _peerService)
                || service.Transfers.GetTransfer(transfer.Key).State
                    != TransferState.WaitingForAnswer
            )
            {
                return;
            }

            foreach (var existing in Offers)
            {
                if (existing.Key == transfer.Key)
                {
                    return;
                }
            }

            string senderName =
                service
                    .GetPeers()
                    .FirstOrDefault(peer => peer.DeviceId == transfer.PeerDeviceId)
                    ?.Name
                ?? "Неизвестное устройство";

            var offer = new IncomingFileOffer(
                transfer.Key,
                transfer.FileName,
                transfer.FileSize,
                senderName
            );
            Offers.Add(offer);
            NotifyIncomingFilesChanged();
        });
    }
}
