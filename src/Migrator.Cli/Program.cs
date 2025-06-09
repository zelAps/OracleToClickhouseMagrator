using Spectre.Console;
using Spectre.Console.Cli;
using Migrator.Cli.Commands;

// Создаём CLI-приложение с двумя ветками команд: create и migrate
var app = new CommandApp();

app.Configure(cfg =>
{
    cfg.SetApplicationName("oracle2ch");
    // Внутри регистрируются подкоманды «create» и «migrate»
    cfg.AddBranch("create", b =>
    {
        b.AddCommand<CreateAllCommand>("all");
        b.AddCommand<CreateTablesCommand>("tables");
    });

    // Внутри регистрируются подкоманды «create» и «migrate»
    cfg.AddBranch("migrate", b =>
    {
        b.AddCommand<MigrateAllCommand>("all");
        b.AddCommand<MigrateTablesCommand>("tables");
    });

    // PropagateExceptions позволяет увидеть исходное исключение при ошибке
    cfg.PropagateExceptions();
});

return await app.RunAsync(args);
