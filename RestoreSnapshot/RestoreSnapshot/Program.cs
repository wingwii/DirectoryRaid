using Newtonsoft.Json;
using System;
using System.IO;

namespace RestoreSnapshot
{
    class Program
    {
        private static DirectoryRaid.Header _Header = null;
        private static RaidDB _DB = null;

        static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("restore <mode> <HeaderFile>");
                Console.WriteLine("  Modes:");
                Console.WriteLine("    Build");
                Console.WriteLine("    Restore-<StorageNumber>");
                return;
            }

            var mode = args[0];
            var hdrFileName = args[1];

            var hdrData = File.ReadAllText(hdrFileName);
            _Header = JsonConvert.DeserializeObject<DirectoryRaid.Header>(hdrData);

            var fi = new FileInfo(hdrFileName);
            var cwd = fi.Directory.FullName;

            _DB = new RaidDB();
            _DB.RaidHeader = _Header;
            _DB.FileName = Path.Combine(cwd, "blocks.db");
            _DB.Load();

            if (mode.Equals("build", StringComparison.OrdinalIgnoreCase))
            {
                if (_Header.Status.Equals("Commited", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine("Snapshot was not commited.");
                    return;
                }

                var builder = new Builder(_DB);
                builder.Build(_Header.NumberOfPartitions + 1);

                if (!_Header.Status.Equals("Commited", StringComparison.OrdinalIgnoreCase))
                {
                    hdrData = JsonConvert.SerializeObject(_Header, Formatting.Indented);
                    File.WriteAllText(hdrFileName, hdrData);
                }
            }
            else if (mode.IndexOf("restore-", 0, StringComparison.OrdinalIgnoreCase) == 0)
            {
                if (!_Header.Status.Equals("Backup", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine("Snapshot is incompleted.");
                    return;
                }

                var s = mode.Substring(8);
                var storageNum = uint.Parse(s);
                var builder = new Builder(_DB);
                builder.IsRestorationMode = true;
                builder.Build(storageNum);
            }

#if DEBUG
            //Console.ReadKey();
#endif
        }

    }
}
