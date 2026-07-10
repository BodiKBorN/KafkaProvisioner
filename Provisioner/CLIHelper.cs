using System.Diagnostics;

// ReSharper disable InconsistentNaming

namespace Tech.Deployment.KafkaSetup;

public static class CLIHelper
{
    public static string RunShell(string cmd)
    {
        var psi = new ProcessStartInfo
        {
            FileName = "/bin/sh",
            Arguments = $"-c \"{cmd}\"",
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        using var process = Process.Start(psi);

        Console.WriteLine("\n✅ Reassignments initiated for all eligible topics.");

        process!.WaitForExit();

        var stdout = process.StandardOutput.ReadToEnd();
        var stderr = process.StandardError.ReadToEnd();

        if (!string.IsNullOrEmpty(stderr))
            Console.WriteLine($"Error: [stderr]: {stderr.Trim()}");

        return stdout;
    }

    /// <summary>
    /// Chose command exists in PATH, trying both "command" and "command.sh" accordingly to installed package
    /// </summary>
    public static string? ChoseKafkaCommand(string command)
    {
        string[] candidates = [command, $"{command}.sh"];
        return candidates.FirstOrDefault(IsCommandAvailable);
    }

    /// <summary>
    /// Checks if a command exists in PATH
    /// </summary>
    private static bool IsCommandAvailable(string command)
    {
        var pathEnv = Environment.GetEnvironmentVariable("PATH");
        if (string.IsNullOrEmpty(pathEnv)) return false;

        var paths = pathEnv.Split(Path.PathSeparator);

        // Windows handles extensions differently
        var extensions = Environment.OSVersion.Platform == PlatformID.Win32NT
            ? Environment.GetEnvironmentVariable("PATHEXT")?.Split(';') ?? [".exe", ".bat", ".cmd"]
            : [""];

        return paths.Any(dir => extensions.Select(ext => Path.Combine(dir, command + ext)).Any(File.Exists));
    }
}