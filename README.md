# Структура проекта

```text
LocalFileShare.slnx
src/
  Protocol/                 Общие типы протокола и временная консольная точка входа
    Packet/                 Заголовок, сериализация, разбор и CRC32
    Payloads/               Форматы всех сообщений P2P
  Client/                   Avalonia-приложение
    Views/                  Окна и разметка
    ViewModels/             Состояние и команды UI
    Models/                 Модели клиента
    Assets/                 Ресурсы клиента
tests/
  Protocol.Tests/           Тесты Protocol на xUnit
```

Client и Protocol.Tests ссылаются на Protocol. Библиотека не зависит от клиента.
