using Newtonsoft.Json;
using System;
using System.IO;

namespace RestoreSnapshot
{
    class Program
    {
        private static DirectoryRaid.Header _Header = null;
        private static DirectoryRaid.RaidDB _DB = null;

        static void Main(string[] args)
        {
            var argc = args.Length;
            if (argc < 2)
            {
                Console.WriteLine("restore <HeaderFile> <mode>");
                Console.WriteLine("  Modes:");
                Console.WriteLine("    Parity [AutoRefreshContent:0|1]");
                Console.WriteLine("    Storage <StorageNumber>");
                Console.WriteLine("    File [S<StorageNumber>:]<FileID|FilePath>");
                return;
            }

            var hdrFileName = args[0];
            var mode = args[1];

            var hdrData = File.ReadAllText(hdrFileName);
            _Header = JsonConvert.DeserializeObject<DirectoryRaid.Header>(hdrData);

            var fi = new FileInfo(hdrFileName);
            var cwd = fi.Directory.FullName;

            _DB = new DirectoryRaid.RaidDB();
            _DB.RaidHeader = _Header;
            _DB.FileName = Path.Combine(cwd, "blocks.db");
            _DB.Load();

            if (mode.Equals("parity", StringComparison.OrdinalIgnoreCase))
            {
                if (_Header.Status.Equals("Commited", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine("Snapshot was not commited.");
                    return;
                }

                var builder = new Builder(_DB);
                if (argc > 2)
                {
                    //builder.EnableAutoRefreshContent = (!args[2].Equals("0", StringComparison.Ordinal));
                }

                builder.Build(_Header.NumberOfPartitions + 1);

                if (!_Header.Status.Equals("Commited", StringComparison.OrdinalIgnoreCase))
                {
                    hdrData = JsonConvert.SerializeObject(_Header, Formatting.Indented);
                    File.WriteAllText(hdrFileName, hdrData);
                }
            }
            else if (mode.Equals("storage", StringComparison.OrdinalIgnoreCase))
            {
                if (argc < 3)
                {
                    Console.WriteLine("StorageNumber is required.");
                    return;
                }

                if (!CheckRaidReady())
                {
                    return;
                }

                var storageNum = uint.Parse(args[2]);
                var builder = new Builder(_DB);
                builder.IsRestorationMode = true;
                builder.Build(storageNum);
            }
            else if (mode.Equals("id", StringComparison.OrdinalIgnoreCase))
            {
                if (argc < 3)
                {
                    Console.WriteLine("Target ID is required.");
                    return;
                }

                if (!CheckRaidReady())
                {
                    return;
                }

                var builder = new Builder(_DB);
                builder.IsRestorationMode = true;
                builder.TargetID = args[2];
                builder.Build(0);
            }

#if DEBUG
            //Console.ReadKey();
#endif
        }

        private static bool CheckRaidReady()
        {
            if (!_Header.Status.Equals("Backup", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine("Snapshot is incompleted.");
                return false;
            }
            else
            {
                return true;
            }
        }

    }
}
