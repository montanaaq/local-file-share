# Структура проекта

```text
LocalFileShare.slnx
src/
  Protocol/                 Классовая библиотека протокола и P2P-обмена
    ProtocolConstants.cs    Общие ограничения и интервалы обмена
    Extensions/             Общая отправка, учёт завершения задач и сетевые ошибки
    Messages/               Типы сообщений, пакет, payload и смещения полей
    Transport/              UDP, ACK, повторы, потери и статистика
    Peers/                  PeerService, обнаружение и доступность устройств
    Exchange/               Управление обменом, текст и доставка уведомлений
    Transfers/              Файловый сценарий и управление передачами
      Files/                Чтение, запись, проверка SHA-256 и безопасные имена
      Models/               Снимки, ключи, состояния и внутренняя запись передачи
  Client/                   Avalonia-приложение
    Views/                  Окна и разметка
    ViewModels/             Состояние и команды UI
    Models/                 Модели клиента
    Assets/                 Ресурсы клиента
tests/
  Protocol.Tests/           Тесты Protocol на xUnit
    Messages/               Сетевой формат и некорректные пакеты
    Transport/              Доставка, ACK, повторы и дедупликация
    Peers/                  Обнаружение и обмен нескольких PeerService
    Exchange/               Уведомления подписчикам
    Transfers/              Файлы, пауза, возобновление и отмена
      Files/                Безопасные имена файлов
```


## Работа сервисов

```mermaid
flowchart TD
    subgraph client[Client · Avalonia]
        app[App] -->|Создаёт окно| view[MainWindow]
        app -->|Назначает DataContext| viewModel[MainViewModel]
        viewModel -->|Привязка Greeting| view
    end

    subgraph transport[UdpTransport]
        socket[Socket] --> validation[Проверка пакета и payload]
        validation -->|ACK| pending[Сопоставление deviceId, endpoint, seq]
        validation -->|Обычное сообщение| queue[Очередь входящих]
        send[Отправка пакета] --> wait[Ожидание ACK]
        pending -->|Подтверждение| wait
        wait -->|Тайм-аут: повтор с тем же seq| send
    end

    subgraph peers[PeerService]
        route[Единый потребитель очереди] -->|DISCOVER / PEER_INFO| devices[Имя, адрес и доступность устройств]
        route -->|HEARTBEAT| heartbeat[Обновление доступности и ACK]
        monitor[Таймер поиска и HEARTBEAT] --> devices
    end

    subgraph exchange[ExchangeService]
        dispatch[Маршрутизация обмена] -->|CLIPBOARD| dedup[Дедупликация текста и ACK]
        toggle[Включение и отключение] --> token[Токен обмена]
        maintenance[Обслуживание каждые 100 мс]
    end

    subgraph files[FileExchange]
        scenario[FILE_OFFER → FILE_ANSWER → CHUNK → FILE_DONE → FILE_RESULT]
        chunks[Один чанк → ACK → следующий]
        resume[В паузе: FILE_OFFER каждые 2 с]
        scenario --> chunks
        resume -->|FILE_ANSWER.nextIndex| chunks
    end

    subgraph transfers[FileTransferService]
        state[Состояние по TransferKey: deviceId + fileId]
        decision[Решение пользователя] --> state
        state --> disk[Запись .part → SHA-256 → сохранение]
        timeouts[Проверка тайм-аутов] -->|Пауза истекла| cleanup[Ошибка и удаление .part]
    end

    viewModel -.->|Обмен пока не подключён| route
    queue --> route
    route -->|Файлы и текст от известного устройства| dispatch
    dispatch -->|Файловые сообщения| scenario
    token -.->|Отмена сетевых операций и пауза| scenario
    maintenance --> timeouts
    maintenance --> resume
    scenario --> state
    chunks -->|Чтение, запись и прогресс| state
    dedup -->|Новый текст| notifications
    state -->|Снимки и события| notifications
    devices -->|Изменения устройств| notifications
    notifications[NotificationDispatcher] --> subscribers[Подписчики]

    classDef service fill:#edf6ff,stroke:#7396b8,color:#172b40;
    class token,queue,state,notifications service;
```
