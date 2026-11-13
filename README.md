# Структура проекта

```text
LocalFileShare.slnx
LocalFileShare.csproj      
Program.cs              
src/
  Protocol/                 Классовая библиотека протокола и P2P-обмена
    ProtocolConstants.cs    Общие ограничения и интервалы обмена
    Extensions/             Общая отправка, учёт задач и классификация ошибок
    Messages/               Типы сообщений, пакет, payload и смещения полей
    Transport/              UDP, ACK, повторы, потери и статистика
    Peers/                  PeerService, обнаружение и доступность устройств
    Exchange/               Управление обменом, текст и доставка уведомлений
    Transfers/              Файловый сценарий и управление передачами
      Files/                Чтение, запись, проверка SHA-256 и безопасные имена
      Models/               Снимки, ключи, состояния и внутренняя запись передачи
  Client/                   Avalonia-приложение
    Views/                  Окна и разметка
    ViewModels/             Модели сессии, настроек, текста, отправки и входящих предложений
    Models/                 Модели клиента
    Settings/               Загрузка и сохранение настроек профиля
    Extensions/             Вывод размера файлов и понятных сообщений об ошибках
tests/
  Protocol.Tests/           Тесты Protocol на xUnit
```

## Работа сервисов

```mermaid
flowchart TD
    subgraph client[Client · Avalonia]
        app[App] -->|Создаёт окно| view[MainWindow]
        app -->|Назначает DataContext| viewModel[MainViewModel]
        viewModel -->|Привязки состояния и команд| view
        viewModel --> sending[TransfersViewModel · отправка и список передач]
        viewModel --> offers[IncomingFilesViewModel · входящие предложения]
        viewModel --> text[TextViewModel · отправка и получение текста]
        ui[Dispatcher.UIThread.Post] -->|Обновление состояния| viewModel
        ui --> sending
        ui --> offers
        ui --> text
    end

    subgraph transport[UdpTransport]
        socket[Socket] --> validation[Проверка пакета и payload]
        validation -->|ACK| pending[Сопоставление deviceId, endpoint, seq]
        validation -->|Обычное сообщение| queue[Очередь входящих]
        reliable[SendReliableAsync] --> send[SendAsync · отправка без ожидания ACK]
        send -->|UDP-пакет| socket
        reliable -->|После отправки| wait[Ожидание ACK]
        pending -->|Подтверждение| wait
        wait -->|Тайм-аут: повтор с тем же seq, кроме HEARTBEAT| send
    end

    subgraph peers[PeerService]
        peerApi[Команды клиента и управление сессией]
        route[Единый потребитель очереди] -->|DISCOVER / PEER_INFO| devices[Имя, адрес и доступность устройств]
        route -->|HEARTBEAT| heartbeat[Обновление доступности и ACK]
        monitor[Таймер поиска и HEARTBEAT] --> devices
        peerInfo[Отправка DISCOVER и PEER_INFO]
    end

    subgraph exchange[ExchangeService]
        exchangeApi[Отправка файла, текста и решение по файлу]
        sendText[Отправка CLIPBOARD]
        dispatch[Маршрутизация обмена] -->|CLIPBOARD| dedup[Дедупликация текста и ACK]
        toggle[Включение и отключение] --> token[Токен обмена]
        maintenance[Обслуживание каждые 100 мс]
    end

    subgraph files[FileExchange]
        fileApi[Отправка файла и решение пользователя]
        incoming[Разбор файловых сообщений и решения пользователя]
        tasks[FileBackgroundTaskHandler · задачи и повторные ответы]
        fileSend[Отправка файловых сообщений через транспорт]
        fileMaintenance[Проверка тайм-аутов, задач и возобновления]
    end

    subgraph outgoing[OutgoingFileExchange]
        scenario[FILE_OFFER → FILE_ANSWER → CHUNK → FILE_DONE → FILE_RESULT]
        chunks[Один чанк → ACK → следующий]
        resume[В паузе: FILE_OFFER каждые 2 с]
        scenario --> chunks
        resume -->|FILE_ANSWER.nextIndex| chunks
    end

    subgraph transfers[FileTransferService]
        state[Состояние по TransferKey: peerDeviceId + fileId]
        decision[Решение пользователя] --> state
        state --> disk[Запись .part → SHA-256 → сохранение]
        timeouts[Проверка тайм-аутов] -->|Пауза истекла| cleanup[Ошибка и удаление .part]
    end

    viewModel -->|Запуск, остановка и включение обмена| peerApi
    sending -->|Отправить файл или отменить паузу| peerApi
    offers -->|Принять или отклонить| peerApi
    text -->|Отправить текст| peerApi
    peerApi --> exchangeApi
    peerApi --> toggle
    peerApi -->|Запуск обслуживания| maintenance
    exchangeApi --> fileApi
    exchangeApi --> sendText
    sendText --> reliable
    monitor -->|Поиск устройств| peerInfo
    monitor -->|HEARTBEAT, одна попытка| reliable
    route -->|Ответ на DISCOVER| peerInfo
    peerInfo --> send
    heartbeat -->|ACK| send
    queue --> route
    route -->|Файлы и текст от известного устройства| dispatch
    dispatch -->|Файловые сообщения| incoming
    dedup -->|ACK, в том числе для дубликата| send
    incoming --> state
    incoming -->|ACK| send
    incoming -->|Фоновые ответы| tasks
    fileApi -->|Отправка файла| scenario
    fileApi -->|Решение по входящему файлу| decision
    fileApi -->|FILE_ANSWER| fileSend
    scenario -->|FILE_OFFER и FILE_DONE| fileSend
    chunks -->|CHUNK| fileSend
    tasks -->|FILE_ANSWER и FILE_RESULT| fileSend
    fileSend --> reliable
    resume -->|Фоновое возобновление| tasks
    maintenance --> fileMaintenance
    fileMaintenance -->|Наблюдение ошибок задач| tasks
    token -.->|Отмена сетевых операций и пауза| scenario
    token -.->|Отмена сетевых операций| fileSend
    token -.->|Отмена отправки текста| sendText
    fileMaintenance --> timeouts
    fileMaintenance --> resume
    scenario --> state
    chunks -->|Чтение, запись и прогресс| state
    dedup -->|Новый текст| notifications
    state -->|Снимки и события| notifications
    devices -->|Изменения устройств| notifications
    notifications[NotificationDispatcher · фоновая очередь] --> subscribers[Подписчики событий]
    subscribers -->|В клиенте: перенос в поток UI| ui

    classDef service fill:#edf6ff,stroke:#7396b8,color:#172b40;
    class token,queue,state,notifications service;
```
